"""
Utility functions for formatting baseline data in standardized structure.

This module provides functions to convert baseline statistics into the
standardized baseline structure required by the API.
"""

from typing import Optional, Dict, Any
from app.models.profile import ProfileBaselineStats, Profile


def build_standardized_baseline(
    baseline_stat: Optional[ProfileBaselineStats],
    profile: Optional[Profile],
    fallback_mean: Optional[float] = None,
    fallback_std: Optional[float] = None,
    fallback_min: Optional[float] = None,
    fallback_max: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Build standardized baseline structure for a sensor/metric.
    
    Args:
        baseline_stat: ProfileBaselineStats object with baseline statistics
        profile: Profile object containing material_id
        fallback_mean: Fallback mean value if baseline_stat is None
        fallback_std: Fallback std value if baseline_stat is None
        fallback_min: Fallback min value if baseline_stat is None
        fallback_max: Fallback max value if baseline_stat is None
    
    Returns:
        Dictionary with standardized baseline structure:
        {
            "sensor_name": str,
            "baseline_mean": float,
            "baseline_min": float,
            "baseline_max": float,
            "baseline_material": str,
            "baseline_confidence": float
        }
    """
    # Get sensor name from metric_name
    sensor_name = baseline_stat.metric_name if baseline_stat else None
    
    # Get baseline_mean
    baseline_mean = None
    if baseline_stat and baseline_stat.baseline_mean is not None:
        baseline_mean = float(baseline_stat.baseline_mean)
    elif fallback_mean is not None:
        baseline_mean = float(fallback_mean)
    
    # Get baseline_min and baseline_max
    # Priority: p05/p95 > fallback min/max > mean ± std > mean ± 5%
    baseline_min = None
    baseline_max = None
    
    if baseline_stat:
        # First try: Use p05 and p95 percentiles (most accurate)
        if baseline_stat.p05 is not None:
            baseline_min = float(baseline_stat.p05)
        if baseline_stat.p95 is not None:
            baseline_max = float(baseline_stat.p95)
    
    # Second try: Use fallback min/max if provided
    if baseline_min is None and fallback_min is not None:
        baseline_min = float(fallback_min)
    if baseline_max is None and fallback_max is not None:
        baseline_max = float(fallback_max)
    
    # Third try: Derive from mean ± std
    if baseline_min is None or baseline_max is None:
        std = None
        if baseline_stat and baseline_stat.baseline_std is not None:
            std = float(baseline_stat.baseline_std)
        elif fallback_std is not None:
            std = float(fallback_std)
        
        if baseline_mean is not None and std is not None:
            if baseline_min is None:
                baseline_min = baseline_mean - std
            if baseline_max is None:
                baseline_max = baseline_mean + std
    
    # Fourth try: Derive from mean ± 5% (fallback if no std available)
    if baseline_min is None or baseline_max is None:
        if baseline_mean is not None:
            percent_deviation = 0.05  # 5%
            if baseline_min is None:
                baseline_min = baseline_mean * (1 - percent_deviation)
            if baseline_max is None:
                baseline_max = baseline_mean * (1 + percent_deviation)
    
    # Get baseline_material from profile
    baseline_material = None
    if profile and profile.material_id:
        baseline_material = str(profile.material_id)
    
    # Get baseline_confidence (default 1.0 if not available)
    # For now, we use a simple heuristic based on sample_count
    baseline_confidence = 1.0  # Default
    if baseline_stat and baseline_stat.sample_count is not None:
        sample_count = float(baseline_stat.sample_count)
        # Confidence increases with sample count, capped at 1.0
        # 100+ samples = 1.0, 50-99 = 0.9, 25-49 = 0.8, 10-24 = 0.7, <10 = 0.6
        if sample_count >= 100:
            baseline_confidence = 1.0
        elif sample_count >= 50:
            baseline_confidence = 0.9
        elif sample_count >= 25:
            baseline_confidence = 0.8
        elif sample_count >= 10:
            baseline_confidence = 0.7
        else:
            baseline_confidence = 0.6
    
    # Build and return standardized structure
    return {
        "sensor_name": sensor_name,
        "baseline_mean": round(baseline_mean, 3) if baseline_mean is not None else None,
        "baseline_min": round(baseline_min, 3) if baseline_min is not None else None,
        "baseline_max": round(baseline_max, 3) if baseline_max is not None else None,
        "baseline_material": baseline_material,
        "baseline_confidence": round(baseline_confidence, 2),
    }


def build_standardized_baseline_from_dict(
    metric_name: str,
    baseline_data: Dict[str, Any],
    material_id: Optional[str] = None,
    confidence: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Build standardized baseline structure from a dictionary.
    
    This is a convenience function for cases where baseline data comes from
    rolling baselines or other sources that aren't ProfileBaselineStats.
    
    Args:
        metric_name: Name of the sensor/metric
        baseline_data: Dictionary with keys: mean, std, min, max, etc.
        material_id: Material ID for the baseline
        confidence: Confidence value (defaults to 1.0)
    
    Returns:
        Dictionary with standardized baseline structure
    """
    baseline_mean = baseline_data.get("mean")
    baseline_std = baseline_data.get("std")
    
    # Get min/max from various possible keys
    baseline_min = (
        baseline_data.get("min") or
        baseline_data.get("min_normal") or
        baseline_data.get("p05")
    )
    baseline_max = (
        baseline_data.get("max") or
        baseline_data.get("max_normal") or
        baseline_data.get("p95")
    )
    
    # Derive from mean ± std if not available
    if baseline_min is None and baseline_mean is not None and baseline_std is not None:
        baseline_min = baseline_mean - baseline_std
    if baseline_max is None and baseline_mean is not None and baseline_std is not None:
        baseline_max = baseline_mean + baseline_std
    
    # Derive from mean ± 5% as last resort
    if baseline_min is None and baseline_mean is not None:
        baseline_min = baseline_mean * 0.95
    if baseline_max is None and baseline_mean is not None:
        baseline_max = baseline_mean * 1.05
    
    return {
        "sensor_name": metric_name,
        "baseline_mean": round(baseline_mean, 3) if baseline_mean is not None else None,
        "baseline_min": round(baseline_min, 3) if baseline_min is not None else None,
        "baseline_max": round(baseline_max, 3) if baseline_max is not None else None,
        "baseline_material": material_id,
        "baseline_confidence": round(confidence, 2) if confidence is not None else 1.0,
    }
