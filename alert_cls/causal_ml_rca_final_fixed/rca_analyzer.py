"""
RCA Analyzer

Two root cause analysis scenarios from the DoWhy microservice RCA tutorial:
  Scenario 1: Anomaly Attribution (attribute_anomalies)
  Scenario 2: Distribution Change  (distribution_change)
"""

import numpy as np
import pandas as pd
from typing import Dict
import dowhy.gcm as gcm


def perform_anomaly_attribution(
    causal_model: gcm.StructuralCausalModel,
    anomalous_data: pd.DataFrame,
    target_node: str,
) -> Dict:
    """
    Scenario 1: Anomaly Attribution

    Per tutorial (Microservice RCA - Scenario 1):
      "To answer why Website was slower for this customer, we attribute
       the outlier latency at Website to upstream services."

    Uses: gcm.attribute_anomalies(causal_model, target_node, anomaly_samples)

    Args:
        causal_model: Fitted StructuralCausalModel
        anomalous_data: DataFrame of anomalous samples
        target_node: Service to explain

    Returns:
        Dict with attributions, sorted causes, raw attributions
    """
    print(f"\n{'=' * 70}")
    print(f"SCENARIO 1: ANOMALY ATTRIBUTION")
    print(f"Target: {target_node} | Anomalous samples: {len(anomalous_data)}")
    print(f"{'=' * 70}")

    gcm.config.disable_progress_bars()

    attributions = gcm.attribute_anomalies(
        causal_model,
        target_node=target_node,
        anomaly_samples=anomalous_data,
    )

    # attributions is dict of {node: array of scores per sample}
    median_attributions = {k: np.median(v) for k, v in attributions.items()}
    sorted_causes = sorted(median_attributions.items(), key=lambda x: abs(x[1]), reverse=True)
    total = sum(abs(v) for v in median_attributions.values())

    print(f"\n{'Rank':<6} {'Service':<30} {'Score':<15} {'Percent'}")
    print(f"{'-' * 65}")
    for i, (node, score) in enumerate(sorted_causes, 1):
        pct = (abs(score) / total * 100) if total > 0 else 0
        print(f"{i:<6} {node:<30} {score:>8.4f}       {pct:>5.1f}%")
    print(f"{'=' * 70}")

    return {
        'target': target_node,
        'attributions': median_attributions,
        'sorted_causes': sorted_causes,
        'raw_attributions': attributions,
    }


def perform_distribution_change(
    causal_model: gcm.StructuralCausalModel,
    normal_data: pd.DataFrame,
    anomalous_data: pd.DataFrame,
    target_node: str,
) -> Dict:
    """
    Scenario 2: Distribution Change

    Per tutorial (Microservice RCA - Scenario 2 & Online Shop):
      "we attribute the change in the average latency of Website
       to services upstream in the causal graph."

    Uses: gcm.distribution_change(model, normal, anomalous, target_node,
                                   difference_estimation_func)

    Args:
        causal_model: Fitted StructuralCausalModel
        normal_data: Normal period data
        anomalous_data: Anomalous period data
        target_node: Service to explain

    Returns:
        Dict with attributions and sorted causes
    """
    print(f"\n{'=' * 70}")
    print(f"SCENARIO 2: DISTRIBUTION CHANGE")
    print(f"Target: {target_node}")
    print(f"Normal: {len(normal_data)} samples | Anomalous: {len(anomalous_data)} samples")
    print(f"{'=' * 70}")

    mean_before = normal_data[target_node].mean()
    mean_after = anomalous_data[target_node].mean()
    print(f"Mean latency change: {mean_before:.4f} -> {mean_after:.4f} (delta: {mean_after - mean_before:+.4f})")

    gcm.config.disable_progress_bars()

    attributions = gcm.distribution_change(
        causal_model,
        normal_data,
        anomalous_data,
        target_node=target_node,
        difference_estimation_func=lambda x, y: np.mean(y) - np.mean(x),
    )

    sorted_causes = sorted(attributions.items(), key=lambda x: abs(x[1]), reverse=True)
    total = sum(abs(v) for v in attributions.values())

    print(f"\n{'Rank':<6} {'Service':<30} {'Delta Mean':<15} {'Percent'}")
    print(f"{'-' * 65}")
    for i, (node, delta) in enumerate(sorted_causes, 1):
        pct = (abs(delta) / total * 100) if total > 0 else 0
        print(f"{i:<6} {node:<30} {delta:>8.4f}       {pct:>5.1f}%")
    print(f"{'=' * 70}")

    return {
        'target': target_node,
        'attributions': attributions,
        'sorted_causes': sorted_causes,
    }
