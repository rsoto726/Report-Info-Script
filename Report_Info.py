# ============================
# IMPORTS & INITIALIZATION
# ============================

import requests
import csv
import os
import re
import json
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import pytz
from datetime import datetime
from zoneinfo import ZoneInfo

# Load environment variables from a .env file
load_dotenv()

# Constants for Azure AD authentication
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
IMPERSONATED_USERNAME = os.getenv("IMPERSONATED_USERNAME")
SCOPE = ['https://analysis.windows.net/powerbi/api/.default']
AUTHORITY_URL = f"https://login.microsoftonline.com/{TENANT_ID}"
TOKEN_URL = f"{AUTHORITY_URL}/oauth2/v2.0/token"

# Cache to avoid redundant datasource name lookups
datasource_name_cache = {}

# ============================
# AUTHENTICATION
# ============================

def get_access_token():
    """Get an access token using client credentials flow."""
    payload = {
        'client_id': CLIENT_ID,
        'scope': ' '.join(SCOPE),
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    response = requests.post(TOKEN_URL, data=payload)
    response.raise_for_status()
    return response.json()['access_token']

# ============================
# GENERIC API HELPERS
# ============================

def get_json(url, token):
    """Make a GET request with retry on rate limits."""
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 5))
        time.sleep(retry_after)
        return get_json(url, token)
    response.raise_for_status()
    return response.json()

def execute_dax_query(token, group_id, dataset_id, dax_query):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/executeQueries"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "queries": [
            {
                "query": dax_query
            }
        ],
        "serializerSettings": {
        "includeNulls": True
        },
        "impersonatedUserName": IMPERSONATED_USERNAME
    }
    while True:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited on DAX query. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response.json()

def get_all_pages(url, token):
    """Handle paginated responses from Power BI API."""
    headers = {'Authorization': f'Bearer {token}'}
    items = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return items

def utc_to_central(utc_str):
    if not utc_str or utc_str == "N/A":
        return None  # use None instead of "N/A" if you want datetime return type
    try:
        # Full UTC timestamp with optional milliseconds
        try:
            utc_time = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            utc_time = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")

        utc_time = utc_time.replace(tzinfo=ZoneInfo("UTC"))
        central_time = utc_time.astimezone(ZoneInfo("America/Chicago"))
        return central_time  # return as datetime object (timezone-aware)

    except ValueError:
        # Maybe it's just a plain time string "HH:MM"
        try:
            t = datetime.strptime(utc_str, "%H:%M").time()
            # attach today's date + CST/CDT
            today = datetime.now(ZoneInfo("America/Chicago")).date()
            return datetime.combine(today, t, tzinfo=ZoneInfo("America/Chicago"))
        except ValueError:
            return None  # if format completely unknown
# ============================
# DATASOURCE HELPERS
# ============================

def get_datasource_name(gateway_id, datasource_id, token):
    """Get human-readable name for a datasource using its gateway + ID."""
    key = f"{gateway_id}|{datasource_id}"
    if key in datasource_name_cache:
        return datasource_name_cache[key]
    url = f"https://api.powerbi.com/v1.0/myorg/gateways/{gateway_id}/datasources/{datasource_id}"
    try:
        name = get_json(url, token).get("datasourceName", "Unnamed Dataflow")
    except Exception as e:
        name = f"Error: {e}"
    datasource_name_cache[key] = name
    return name

def get_dataflow_refresh_history(token, group_id, dataflow_id, top=10):
    """
    Get the refresh history (transactions) for a specific dataflow.
    Returns most recent entries up to `top`.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflow_id}/transactions?$top={top}"
    try:
        return get_json(url, token).get("value", [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"⚠️ Access denied for dataflow {dataflow_id} in workspace {group_id}")
            return []
        raise

def get_datasources(token, group_id, dataset_id):
    """Fetch datasources for a dataset in a workspace."""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/datasources"
    try:
        return get_json(url, token).get('value', [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            return [{"error": "Access Denied"}]
        raise

# ============================
# METADATA FETCHERS
# ============================

def get_dataset_metadata(token, workspace_id, dataset_id):
    """Get dataset configuration including refreshability and owner."""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}"
    return get_json(url, token)

def get_refresh_metadata(token, workspace_id, dataset_id):
    """Get dataset's scheduled refresh settings."""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
    return get_json(url, token)

def get_last_refresh(token, workspace_id, dataset_id):
    """Get timestamp of most recent dataset refresh."""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
    data = get_json(url, token).get('value', [])
    return data[0].get('endTime', 'N/A') if data else 'N/A'

# ============================
# DATAFLOW METADATA (ADMIN)
# ============================

def get_all_admin_dataflows(token):
    """Return all dataflows in the tenant (admin scope)."""
    return get_all_pages("https://api.powerbi.com/v1.0/myorg/admin/dataflows", token)

def get_dataflow_datasources_from_admin_api(token, dataflow_id):
    """Get datasources associated with a specific dataflow."""
    url = f"https://api.powerbi.com/v1.0/myorg/admin/dataflows/{dataflow_id}/datasources"
    try:
        return get_json(url, token).get("value", [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            return []
        raise

def build_dataflow_lookup(admin_dataflows, token):
    """Create mapping of (dataflow_id, datasource_id) -> dataflow_name."""
    lookup = defaultdict(list)
    for df in admin_dataflows:
        df_id = df.get("objectId")
        df_name = df.get("name", "Unnamed Dataflow")
        if not df_id:
            continue
        for ds in get_dataflow_datasources_from_admin_api(token, df_id):
            ds_id = ds.get("datasourceId")
            if ds_id:
                lookup[(df_id.lower(), ds_id.lower())].append(df_name)
    return dict(lookup)

def get_upstream_dataflows(token, group_id):
    """Get mapping of datasets to dataflows for a workspace."""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/upstreamDataflows"
    try:
        return get_json(url, token).get("value", [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            return []
        raise

def format_refresh_schedule(refresh_meta):
    """Extract schedule day(s) and time(s) from metadata."""
    return refresh_meta.get("days", []), refresh_meta.get("times", [])

def find_dataflow_name(dataflow_lookup, datasource_id, dataflow_ids_hint):
    """Try to resolve dataflow name from lookup."""
    if not datasource_id or not dataflow_ids_hint:
        return ""
    matches = []
    for dfid in dataflow_ids_hint:
        key = (dfid.lower(), datasource_id.lower())
        matches.extend(dataflow_lookup.get(key, []))
    return "; ".join(sorted(set(matches))) if matches else ""

# ============================
# DATASOURCE FORMATTING
# ============================

def format_datasources(datasources, token, dataflow_lookup, dataflow_ids_hint=None):
    """Parse and standardize datasource entries."""
    if not datasources:
        print("⚠️ No datasources provided")
        return []

    formatted = []

    for ds in datasources:
        if isinstance(ds, str) or "error" in ds:
            formatted.append({
                "datasource_type": "Error" if "error" in ds else "Unknown",
                "name": "Access Denied" if "error" in ds else f"Unexpected data: {ds}",
                "gateway_id": "N/A",
                "datasource_id": "N/A",
                "dataflow_name": ""
            })
            continue

        # Extract key fields
        kind = ds.get("datasourceType", "Unknown")
        conn = ds.get("connectionDetails", {})
        datasource_id = ds.get("datasourceId", "N/A")
        gateway_id = ds.get("gatewayId", "N/A")

        # Connection-specific metadata
        server = conn.get("server", "")
        database = conn.get("database", "")
        connection_mode = conn.get("connectionMode", "")
        ext_kind = conn.get("extensionDataSourceKind", "")
        ext_path = conn.get("extensionDataSourcePath", "")
        path = conn.get("path", "")
        surl = conn.get("url", "")
        conn_str = conn.get("connectionString", "")
        ds_kind = conn.get("kind", "").lower()

        # Try to find dataflow name if applicable
        dataflow_name = ""
        if kind == "Extension" and path.lower().startswith("powerplatformdataflows"):
            dataflow_name = find_dataflow_name(dataflow_lookup, datasource_id, dataflow_ids_hint)

        # Build user-friendly name
        name = determine_datasource_name(kind, server, database, connection_mode, path, ext_kind, ext_path,
                                         surl, conn_str, dataflow_name, gateway_id, datasource_id, token)

        # Handle one or more dataflows
        if dataflow_name:
            for flow in [f.strip() for f in dataflow_name.split(";") if f.strip()]:
                formatted.append({
                    "datasource_type": kind,
                    "name": f"Power Platform Dataflow | {flow}",
                    "gateway_id": gateway_id,
                    "datasource_id": datasource_id,
                    "dataflow_name": flow
                })
        else:
            formatted.append({
                "datasource_type": kind,
                "name": name,
                "gateway_id": gateway_id,
                "datasource_id": datasource_id,
                "dataflow_name": ""
            })

    return formatted

def determine_datasource_name(kind, server, database, connection_mode, path, ext_kind, ext_path, 
                              surl, conn_str, dataflow_name, gateway_id, datasource_id, token):
    """Return human-readable datasource name depending on type."""
    if kind == "AnalysisServices":
        return f"AS: {server}/{database} ({connection_mode})"
    elif kind == "File":
        return f"Excel File | Path: {path}"
    elif kind in ["Web", "OData"]:
        return surl
    elif kind == "SharePointList":
        return f"SharePoint | {surl or 'Unknown'}"
    elif kind == "Sql":
        if ".database.windows.net" in server:
            return f"Azure SQL | {server}/{database}"
        elif ".datawarehouse.fabric.microsoft.com" in server:
            return f"Fabric Data Warehouse | {database}"
        else:
            return f"SQL Server | {server}/{database}"
    elif kind == "ODBC":
        match = re.search(r"dsn=([^;]+)", conn_str, re.IGNORECASE)
        dsn = match.group(1) if match else "Unknown"
        return f"ODBC | DSN: {dsn}"
    elif kind == "Extension":
        if ext_kind.lower() == "datamarts":
            return f"Datamart | {ext_path}"
        elif path == "UsageMetricsDataConnector":
            return "Power BI Usage Metrics Connector (Internal)"
        elif dataflow_name:
            return f"Power Platform Dataflow | {dataflow_name}"
        else:
            return f"Extension | {ext_kind or 'Unknown'} | Path: {path or ext_path}"
    elif datasource_id != "N/A" and gateway_id != "N/A":
        return get_datasource_name(gateway_id, datasource_id, token)
    return "Unknown source"

# ============================
# EXPORT RESULTS
# ============================

def save_datasource_results(results, filename='powerbi_report_sources.csv'):
    fieldnames = [
        'Workspace ID', 'Workspace Name', 'Report ID', 'Report Name', 'Report Description',
        'Dataset ID', 'Datasource Type', 'Datasource Name', 'Gateway ID', 'Datasource ID',
        'Last Edited By', 'Last Refresh Time', 'Is Refreshable', 'Refresh Day', 'Refresh Time',
        'Table'
    ]
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ Datasource details CSV written to {filename}")

def save_dataflow_refreshes(results, filename="dataflow_refreshes.csv"):
    fieldnames = [
        "Workspace ID", "Workspace Name", "Dataflow ID", "Dataflow Name",
        "Refresh ID", "Refresh Type", "Status", "Start Time", "End Time"
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ Dataflow refresh history saved to {filename}")



# ============================
# MAIN EXECUTION
# ============================

def main():
    token = get_access_token()
    admin_dataflows = get_all_admin_dataflows(token)
    dataflow_lookup = build_dataflow_lookup(admin_dataflows, token)
    workspaces = get_all_pages("https://api.powerbi.com/v1.0/myorg/groups", token)
    results = []
    report_summaries = []
    all_relationships = []  # <--- global list for all relationships
    all_refreshes = [] 

    for ws in workspaces:
        ws_id, ws_name = ws['id'], ws['name']

        # Map each dataset to its upstream dataflows
        upstream_dataflows = get_upstream_dataflows(token, ws_id)
        dataset_to_dataflows = defaultdict(list)
        for link in upstream_dataflows:
            dataset_to_dataflows[link.get("datasetObjectId")].append(link.get("dataflowObjectId"))

        # Loop through reports in the workspace
        reports = get_all_pages(f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/reports", token)
        for report in reports:
            report_id = report['id']
            report_name = report['name']
            dataset_id = report.get('datasetId')
            if not dataset_id:
                continue
            tables_query = """
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.TABLES(),
                "ObjectType", "Table",
                "Name", [Name],
                "ExtraInfo", [StorageMode]
            )
            """

            relationships_query = """
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.RELATIONSHIPS(),
                "ObjectType", "Relationship",
                "ID", [ID],
                "Name", [Name],
                "FromTable", [FromTable],
                "FromColumn", [FromColumn],
                "FromCardinality", [FromCardinality],
                "ToTable", [ToTable],
                "ToColumn", [ToColumn],
                "ToCardinality", [ToCardinality],
                "IsActive", [IsActive]
            )
            """

            try:
                tables_result = execute_dax_query(token, ws_id, dataset_id, tables_query)
                relationships_result = execute_dax_query(token, ws_id, dataset_id, relationships_query)
                tables = tables_result.get('results', [])[0].get('tables', [])[0].get('rows', [])
                relationships = relationships_result.get('results', [])[0].get('tables', [])[0].get('rows', [])

                # Build tables summary string
                if tables:
                    table_list = []
                    for row in tables:
                        table_name = row.get("[Name]") or row.get("Table Name") or "UnknownTable"
                        table_name = table_name.replace(";", ":")  # clean up
                        storage_mode = row.get("[ExtraInfo]") or "UnknownStorage"
                        table_list.append(f"{table_name} ({storage_mode})")
                    table_list = sorted(table_list, key=lambda x: x.lower())
                    tables_str = "; ".join(table_list)
                else:
                    tables_str = "No tables found"

                # Build relationships summary string (old style)
                if relationships:
                    relationship_list = []
                    for row in relationships:
                        rel_name = row.get("[Name]") or "UnknownRel"
                        from_table = row.get("[FromTable]") or ""
                        from_card = row.get("[FromCardinality]") or ""
                        to_table = row.get("[ToTable]") or ""
                        to_card = row.get("[ToCardinality]") or ""
                        active_flag = row.get("[IsActive]")
                        active_str = "Active" if active_flag else "Inactive"
                        relationship_list.append(f"{rel_name} ({from_table} {from_card} -> {to_table} {to_card}, {active_str})")
                    relationship_list = sorted(relationship_list, key=lambda x: x.lower())
                    relationships_str = "; ".join(relationship_list)
                else:
                    relationships_str = "No relationships found"

                # Save summary info in report_summaries list
                report_summaries.append({
                    "report_id": report_id,
                    "report_name": report_name,
                    "tables": tables_str,
                    "relationships": relationships_str
                })

                # === Collect relationships globally ===
                for row in relationships:
                    all_relationships.append({
                        "Workspace ID": ws_id,
                        "Workspace Name": ws_name,
                        "Report ID": report_id,
                        "Report Name": report_name,
                        "From Table": row.get("[FromTable]", ""),
                        "From Column": row.get("[FromColumn]", ""),
                        "From Cardinality": row.get("[FromCardinality]", ""),
                        "To Table": row.get("[ToTable]", ""),
                        "To Column": row.get("[ToColumn]", ""),
                        "To Cardinality": row.get("[ToCardinality]", ""),
                        "Is Active": "Active" if row.get("[IsActive]", False) else "Inactive"
                    })

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    tables_str = "Unauthorized to run DAX"
                else:
                    tables_str = f"DAX query failed: {e}"

            # Fetch metadata concurrently
            dataset_meta, refresh_meta, last_refresh = {}, {}, "N/A"
            with ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(get_dataset_metadata, token, ws_id, dataset_id): 'meta',
                    executor.submit(get_refresh_metadata, token, ws_id, dataset_id): 'refresh',
                    executor.submit(get_last_refresh, token, ws_id, dataset_id): 'last'
                }
                for future in as_completed(futures):
                    key = futures[future]
                    try:
                        result = future.result()
                        if key == 'meta': dataset_meta = result
                        if key == 'refresh': refresh_meta = result
                        if key == 'last': last_refresh = result
                    except Exception as e:
                        print(f"⚠️ Error fetching {key} for {report_name}: {e}")

            # Process datasources and schedule
            dataflow_ids = dataset_to_dataflows.get(dataset_id, [])
            datasources = get_datasources(token, ws_id, dataset_id)
            datasource_entries = format_datasources(datasources, token, dataflow_lookup, dataflow_ids)
            refresh_days, refresh_times = format_refresh_schedule(refresh_meta)

            for ds_entry in datasource_entries:
                for day in refresh_days or ["N/A"]:
                    for time in refresh_times or ["N/A"]:
                        results.append({
                            "Workspace ID": ws_id,
                            "Workspace Name": ws_name,
                            "Report ID": report_id,
                            "Report Name": report_name,
                            "Report Description": report.get('description', 'N/A'),
                            "Dataset ID": dataset_id,
                            "Datasource Type": ds_entry["datasource_type"],
                            "Datasource Name": ds_entry["name"],
                            "Gateway ID": ds_entry["gateway_id"],
                            "Datasource ID": ds_entry["datasource_id"],
                            "Last Edited By": dataset_meta.get("configuredBy", "N/A"),
                            "Last Refresh Time": utc_to_central(last_refresh),
                            "Is Refreshable": dataset_meta.get("isRefreshable", "N/A"),
                            "Refresh Day": day,
                            "Refresh Time": utc_to_central(time),
                            "Table": tables_str
                        })

    # === NEW: Loop through all tenant dataflows via admin API ===
    for df in admin_dataflows:
        df_id = df.get("objectId")
        df_name = df.get("name", "Unnamed Dataflow")
        ws_id = df.get("workspaceId")
        ws_name = df.get("workspaceName", "Unknown Workspace")

        refreshes = get_dataflow_refresh_history(token, ws_id, df_id, top=10)
        for r in refreshes:
            all_refreshes.append({
                "Workspace ID": ws_id,
                "Workspace Name": ws_name,
                "Dataflow ID": df_id,
                "Dataflow Name": df_name,
                "Refresh ID": r.get("id", "N/A"),
                "Refresh Type": r.get("refreshType", "N/A"),
                "Status": r.get("status", "N/A"),
                "Start Time": utc_to_central(r.get("startTime", "N/A")),
                "End Time": utc_to_central(r.get("endTime", "N/A"))
            })

    # Save results after all workspaces & reports processed
    save_datasource_results(results)
    save_dataflow_refreshes(all_refreshes)
    # === Save all relationships to one CSV ===
    if all_relationships:
        with open("all_relationships.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "Workspace ID", "Workspace Name", "Report ID", "Report Name",
                "From Table", "From Column", "From Cardinality",
                "To Table", "To Column", "To Cardinality", "Is Active"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_relationships)
        print("✅ All relationships saved to all_relationships.csv")
    else:
        print("⚠️ No relationships found to save.")

# ============================
# ENTRY POINT
# ============================

if __name__ == '__main__':
    main()