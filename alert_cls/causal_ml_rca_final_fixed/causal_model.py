"""
causal model - 
    Build, fit, and evaluate DoWhy GCM StructuralCausalModel for RCA scenarios.
    StructuralCausalModel is required for attribute_anomalies() and distribution_change().
"""

import pandas as pd
import networkx as nx
from scipy.stats import halfnorm
import dowhy.gcm as gcm


def build_causal_model(
    causal_graph: nx.DiGraph,
    normal_data: pd.DataFrame,
    use_auto: bool = True,
) -> gcm.StructuralCausalModel:
    """
    Build Structural Causal Model.

    Args:
        causal_graph: DAG from graph_builder
        normal_data: Normal (non-anomalous) latency data
        use_auto: If True, use gcm.auto.assign_causal_mechanisms.
                  If False, use manual assignment (halfnorm roots, linear non-roots).

    Returns:
        StructuralCausalModel with mechanisms assigned
    """
    model = gcm.StructuralCausalModel(causal_graph)

    if use_auto:
        print("\n Building causal model with auto-assigned mechanisms...")
        gcm.auto.assign_causal_mechanisms(model, normal_data, quality=gcm.auto.AssignmentQuality.GOOD)
    else:
        for node in causal_graph.nodes():
            if causal_graph.in_degree(node) == 0:
                model.set_causal_mechanism(node, gcm.ScipyDistribution(halfnorm))
            else:
                model.set_causal_mechanism(node, gcm.AdditiveNoiseModel(gcm.ml.create_linear_regressor()))
    return model


def fit_model(
    causal_model: gcm.StructuralCausalModel,
    normal_data: pd.DataFrame,
) -> None:
    """Fit causal model on normal data."""
    gcm.fit(causal_model, normal_data)
    print(" Fitted causal model on normal data")


def evaluate_model(
    causal_model: gcm.StructuralCausalModel,
    normal_data: pd.DataFrame,
) -> None:
    """
    Evaluate causal model quality using gcm.evaluate_causal_model().

    Per tutorial: gcm.evaluate_causal_model(causal_model, normal_data)
    """
    print("\n" + "=" * 70)
    print("MODEL EVALUATION")
    print("=" * 70)

    summary = gcm.evaluate_causal_model(
        causal_model,
        normal_data,
        compare_mechanism_baselines=False,
        evaluate_invertibility_assumptions=False,
    )
    print(summary)
    print("=" * 70)
