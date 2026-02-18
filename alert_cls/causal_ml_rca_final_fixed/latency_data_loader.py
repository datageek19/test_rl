"""
Latency Data Loader

Loads latency time-series data for services identified in the focused subgraph.
The subgraph (from alert preprocessing) determines WHICH services we need data for.
Data can be pulled from a DB or loaded from local CSV/JSON files.

Flow:
  focused_subgraph → extract service names → pull latency metrics → DataFrame
"""

import json
import pandas as pd
import networkx as nx
from pathlib import Path
from typing import Dict, List, Optional, Union


def get_services_from_subgraph(focused_subgraph: nx.DiGraph) -> List[str]:
    """
    Extract the list of service names from the focused subgraph.
    These are the services we need latency data for.

    Args:
        focused_subgraph: Subgraph from preprocessing (firing Latency alerts mapped to service graph)

    Returns:
        Sorted list of service names
    """
    services = sorted(focused_subgraph.nodes())
    print(f" Services from subgraph ({len(services)}): {services}")
    return services


def load_latency_from_db(
    services: List[str],
    db_config: Dict,
) -> pd.DataFrame:
    """
    Pull latency metrics from a database for the given services.

    This is a placeholder — fill in the actual query logic for your
    data source (e.g., Mimir/Prometheus PromQL, InfluxDB, SQL DB, etc.).

    Args:
        services: List of service names to pull latency data for
        db_config: Dict with connection details

    Returns:
        DataFrame with Time index and one column per service (latency in ms)

    TODO:
        Replace the placeholder below with actual DB query logic.
        Example for Mimir/Prometheus PromQL:
            query = f'avg(latency_seconds{{service="{service}"}}) by (service)'
            response = requests.get(url, params={'query': query, 'start': ..., 'end': ..., 'step': ...})
            # parse response.json()['data']['result'] into DataFrame
    """
    raise NotImplementedError(
        "load_latency_from_db() is a placeholder. "
        f"Services requested: {services}"
    )


def load_latency_from_files(
    services: List[str],
    data_path: str,
) -> pd.DataFrame:
    """
    Load latency data from local CSV or JSON files, filtered to the
    services identified in the focused subgraph.

    Supports:
      - A folder of CSVs (each with a time column + service columns)
      - A single JSON file with {service: [{time: ..., value: ...}, ...]}
      - A single CSV file with a time column + service columns

    Args:
        services: List of service names to load data for (from subgraph)
        data_path: Path to a folder of CSVs, a single CSV, or a single JSON

    Returns:
        DataFrame with Time index and one column per matched service
    """
    path = Path(data_path)

    if path.is_dir():
        return _load_from_csv_folder(services, path)
    elif path.suffix.lower() == '.json':
        return _load_from_json(services, path)
    elif path.suffix.lower() == '.csv':
        return _load_from_single_csv(services, path)
    else:
        raise ValueError(f"Unsupported data source: {data_path}")


def _load_from_csv_folder(services: List[str], folder: Path) -> pd.DataFrame:
    """Load from a folder of CSVs, filtering columns to requested services."""
    csv_files = list(folder.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in {folder}")

    dataframes = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)

        time_col = next((col for col in df.columns if 'time' in col.lower()), None)
        if time_col is None:
            continue

        df[time_col] = pd.to_datetime(df[time_col])
        service_cols = [c for c in df.columns if c != time_col]

        for service_col in service_cols:
            # Normalize column name (strip ' - inbound', whitespace)
            clean_name = service_col.replace(' - inbound', '').strip()

            # Only keep columns that match a requested service
            if clean_name not in services:
                continue

            df[service_col] = df[service_col].astype(str).str.replace(' ms', '').astype(float, errors='ignore')
            service_df = pd.DataFrame({
                'Time': df[time_col],
                clean_name: df[service_col]
            }).set_index('Time')
            dataframes.append(service_df)

    if not dataframes:
        raise ValueError(
            f"No latency data found for requested services.\n"
            f"  Requested: {services}\n"
            f"  Searched: {folder}"
        )

    result = dataframes[0]
    for df in dataframes[1:]:
        result = result.join(df, how='outer')

    result = result.sort_index().ffill().bfill().interpolate(method='time')
    return result


def _load_from_json(services: List[str], json_path: Path) -> pd.DataFrame:
    """
    Load from a JSON file.

    Expected format (one of):
      1. { "service_name": [{"time": "...", "value": 1.23}, ...], ... }
      2. [ {"time": "...", "service_a": 1.23, "service_b": 4.56}, ... ]
    """
    with open(json_path, 'r') as f:
        data = json.load(f)

    if isinstance(data, dict):
        # Format 1: {service: [{time, value}, ...]}
        dataframes = []
        for service in services:
            if service not in data:
                continue
            records = data[service]
            df = pd.DataFrame(records)
            time_col = next((c for c in df.columns if 'time' in c.lower()), df.columns[0])
            df[time_col] = pd.to_datetime(df[time_col])
            value_col = next((c for c in df.columns if c != time_col), None)
            if value_col:
                service_df = pd.DataFrame({
                    'Time': df[time_col],
                    service: pd.to_numeric(df[value_col], errors='coerce')
                }).set_index('Time')
                dataframes.append(service_df)

        if not dataframes:
            raise ValueError(f"No matching services in {json_path}. Requested: {services}")

        result = dataframes[0]
        for df in dataframes[1:]:
            result = result.join(df, how='outer')

    elif isinstance(data, list):
        # Format 2: [{time, service_a, service_b, ...}, ...]
        df = pd.DataFrame(data)
        time_col = next((c for c in df.columns if 'time' in c.lower()), df.columns[0])
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.set_index(time_col)
        df.index.name = 'Time'

        matched_cols = [c for c in df.columns if c in services]
        if not matched_cols:
            raise ValueError(f"No matching services in {json_path}. Requested: {services}")
        result = df[matched_cols].apply(pd.to_numeric, errors='coerce')

    else:
        raise ValueError(f"Unsupported JSON structure in {json_path}")

    result = result.sort_index().ffill().bfill().interpolate(method='time')
    return result


def _load_from_single_csv(services: List[str], csv_path: Path) -> pd.DataFrame:
    """Load from a single CSV file, filtering columns to requested services."""
    df = pd.read_csv(csv_path)
    time_col = next((c for c in df.columns if 'time' in c.lower()), None)
    if time_col is None:
        raise ValueError(f"No time column found in {csv_path}")

    df[time_col] = pd.to_datetime(df[time_col])
    df = df.set_index(time_col)
    df.index.name = 'Time'

    # Normalize column names
    df.columns = [c.replace(' - inbound', '').strip() for c in df.columns]

    matched_cols = [c for c in df.columns if c in services]
    if not matched_cols:
        raise ValueError(
            f"No matching services in {csv_path}.\n"
            f"  Requested: {services}\n"
            f"  Available: {list(df.columns)}"
        )

    result = df[matched_cols].apply(pd.to_numeric, errors='coerce')
    result = result.sort_index().ffill().bfill().interpolate(method='time')
    return result


def load_latency_data(
    services: List[str],
    data_source: str,
    db_config: Optional[Dict] = None,
) -> pd.DataFrame:
    """
    Main entry point: load latency data for the given services.

    The services list comes from the focused subgraph (alert preprocessing).
    Data is pulled from a DB or loaded from local files.

    Args:
        services: Service names from focused subgraph
        data_source: Path to CSV folder, single CSV, single JSON, or 'db'
        db_config: Required if data_source == 'db'

    Returns:
        DataFrame with Time index and one column per matched service
    """
    print(f"\n Loading latency data for {len(services)} services...")

    if data_source == 'db':
        if db_config is None:
            raise ValueError("db_config required when data_source='db'")
        result = load_latency_from_db(services, db_config)
    else:
        result = load_latency_from_files(services, data_source)

    loaded_services = list(result.columns)
    missing = [s for s in services if s not in loaded_services]

    print(f"  Loaded: {result.shape[0]} samples, {len(loaded_services)} services: {loaded_services}")
    if missing:
        print(f"  Missing (no data found): {missing}")

    return result
