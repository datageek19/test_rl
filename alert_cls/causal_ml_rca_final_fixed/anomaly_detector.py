"""
detect anamolies in latency data and identify most affected services:
    Splits latency data into normal vs anomalous periods using statistical methods.
    Identifies the most affected services by latency increase.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple


def detect_anomalies(
    latency_data: pd.DataFrame,
    method: str = 'zscore',
    threshold: float = 2.5,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Detect anomalous periods using statistical methods.

    Args:
        latency_data: DataFrame with service latency columns
        method: 'zscore', 'iqr', or 'percentile'
        threshold: Threshold for anomaly detection

    Returns:
        (normal_data, anomalous_data, anomaly_info)
    """
    print(f"\n Detecting anomalies using {method} (threshold={threshold})...")

    anomaly_scores = pd.DataFrame(index=latency_data.index)

    for service in latency_data.columns:
        values = latency_data[service]

        if method == 'zscore':
            mean, std = values.mean(), values.std()
            if std > 0:
                anomaly_scores[service] = np.abs((values - mean) / std)
            else:
                anomaly_scores[service] = 0

        elif method == 'iqr':
            q1, q3 = values.quantile(0.25), values.quantile(0.75)
            iqr = q3 - q1
            lower, upper = q1 - threshold * iqr, q3 + threshold * iqr
            anomaly_scores[service] = ((values < lower) | (values > upper)).astype(float) * 10

        elif method == 'percentile':
            percentile_threshold = values.quantile(1 - threshold / 100)
            anomaly_scores[service] = (values > percentile_threshold).astype(float) * 10

    if method == 'zscore':
        is_anomalous = (anomaly_scores > threshold).any(axis=1)
    else:
        is_anomalous = (anomaly_scores > 0).any(axis=1)

    normal_data = latency_data[~is_anomalous]
    anomalous_data = latency_data[is_anomalous]

    anomaly_info = {
        'method': method,
        'threshold': threshold,
        'total_samples': len(latency_data),
        'normal_samples': len(normal_data),
        'anomalous_samples': len(anomalous_data),
        'affected_services': {}
    }

    for service in latency_data.columns:
        normal_mean = normal_data[service].mean()
        anomalous_mean = anomalous_data[service].mean() if len(anomalous_data) > 0 else normal_mean
        increase_pct = ((anomalous_mean - normal_mean) / normal_mean) * 100 if normal_mean > 0 else 0

        anomaly_info['affected_services'][service] = {
            'normal_mean': normal_mean,
            'anomalous_mean': anomalous_mean,
            'increase_pct': increase_pct
        }

    print(f"  Normal: {len(normal_data)} samples ({len(normal_data)/len(latency_data)*100:.1f}%)")
    print(f"  Anomalous: {len(anomalous_data)} samples ({len(anomalous_data)/len(latency_data)*100:.1f}%)")

    if len(anomalous_data) == 0:
        print("   No anomalies detected - consider lowering threshold")
    elif len(anomalous_data) < 10:
        print("   Very few anomalies - results may be unreliable")

    return normal_data, anomalous_data, anomaly_info


def identify_most_affected_services(
    anomaly_info: Dict,
    top_n: int = 3,
) -> List[Tuple[str, float]]:
    """
    Identify services most affected by anomalies.

    Args:
        anomaly_info: Dict from detect_anomalies()
        top_n: Number of top services to return

    Returns:
        List of (service_name, increase_pct) tuples
    """
    affected = anomaly_info['affected_services']
    sorted_services = sorted(affected.items(), key=lambda x: x[1]['increase_pct'], reverse=True)

    print(f"TOP {top_n} MOST AFFECTED SERVICES")
    print(f"{'-' * 70}")
    for i, (service, info) in enumerate(sorted_services[:top_n], 1):
        print(f"{i}. {service:30s} +{info['increase_pct']:7.1f}% ({info['normal_mean']:.2f} -> {info['anomalous_mean']:.2f} ms)")
    print()

    return [(s, info['increase_pct']) for s, info in sorted_services[:top_n]]
