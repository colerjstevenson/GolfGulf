from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point


@dataclass
class ExposureConfig:
    distance_unit_m: float = 1000.0  # convert meters to km by default


def _normalize_ctuid(ctuid: str) -> str:
    s = str(ctuid)
    if '.' in s:
        left, right = s.split('.', 1)
        right = right.rstrip('0') or '0'
        return f"{left}.{right}"
    return s


def load_golf_points(csv_path: str) -> Optional[gpd.GeoDataFrame]:
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Warning: Could not load golf courses CSV from {csv_path}: {e}")
        return None
    if not {'latitude', 'longitude'} <= set(df.columns):
        # Support alt column names
        if {'lat', 'lon'} <= set(df.columns):
            df = df.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
        elif {'lat', 'lng'} <= set(df.columns):
            df = df.rename(columns={'lat': 'latitude', 'lng': 'longitude'})
        else:
            print(f"Warning: Golf CSV missing lat/lon columns. Found: {list(df.columns)}")
            return None

    df = df.dropna(subset=['latitude', 'longitude']).copy()
    if df.empty:
        print("Warning: No valid lat/lon rows in golf CSV after dropping NaNs.")
        return None
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    g = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
        crs='EPSG:4326'
    ).to_crs('EPSG:3347')
    return g


def compute_exposure_features(
    tracts_3347: gpd.GeoDataFrame,
    golf_points_3347: gpd.GeoDataFrame,
    cfg: ExposureConfig = ExposureConfig(),
    max_distance_km: float = 1000.0,
) -> pd.DataFrame:
    if golf_points_3347 is None or golf_points_3347.empty:
        # Return NaNs to signal missing exposure data
        out = pd.DataFrame({'CTUID': tracts_3347['CTUID'].astype(str),
                            'dist_to_gc_km': np.nan,
                            'golf_count': 0})
        return out

    # Precompute a unary_union for nearest distance
    gc_union = golf_points_3347.geometry.unary_union
    
    # Vectorized distance computation using representative points
    rep_points = tracts_3347.geometry.representative_point()
    distances_m = rep_points.distance(gc_union)
    distances_km = distances_m / cfg.distance_unit_m
    # Cap at max_distance
    distances_km = distances_km.clip(upper=max_distance_km)
    
    # Vectorized intersection count using spatial join
    joined = gpd.sjoin(tracts_3347[['CTUID', 'geometry']], 
                       golf_points_3347[['geometry']], 
                       how='left', predicate='intersects')
    counts = joined.groupby('CTUID').size().reindex(tracts_3347['CTUID'], fill_value=0)
    
    return pd.DataFrame({
        'CTUID': tracts_3347['CTUID'].astype(str),
        'dist_to_gc_km': distances_km.values,
        'golf_count': counts.values,
    })


def try_fit_spatial_lag(
    y: np.ndarray,
    X: np.ndarray,
    tracts_3347: gpd.GeoDataFrame,
    w=None,  # Allow passing precomputed weights
):
    try:
        from libpysal.weights import Queen
        from spreg import ML_Lag
    except Exception:
        return None, None

    import warnings
    
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=FutureWarning, module='libpysal')
        warnings.filterwarnings('ignore', category=RuntimeWarning)
        warnings.filterwarnings('ignore', message='.*not fully connected.*')
        warnings.filterwarnings('ignore', message='.*correlation coefficient.*')
        warnings.filterwarnings('ignore', message='.*constant.*')
        
        if w is None:
            w = Queen.from_dataframe(tracts_3347, use_index=True)
            w.transform = 'r'

        model = ML_Lag(
            y,
            X,
            w=w,
            name_y='metric',
            name_x=['dist_to_gc_km', 'golf_count'],
            name_w='Queen',
            name_ds='city_tracts',
        )
    # model.predy is the fitted values (N x 1)
    y_hat = getattr(model, 'predy', None)
    if y_hat is None:
        # Fallback: use X @ beta if available
        try:
            beta = np.asarray(model.betas).reshape(-1, 1)
            # beta order: [const, dist, count, rho?]. We'll try to align to X with const.
            if beta.shape[0] >= 3:
                const = beta[0]
                b_dist = beta[1]
                b_cnt = beta[2]
                y_hat = const + X[:, [0]] * b_dist + X[:, [1]] * b_cnt
        except Exception:
            y_hat = None
    return model, y_hat


def fit_metric_spatial_lag_values(
    tracts_3347: gpd.GeoDataFrame,
    exposure_df: pd.DataFrame,
    values_by_ctuid: Dict[str, float],
) -> Tuple[Optional[pd.Series], Dict[str, str]]:
    # Align and clean
    df = pd.DataFrame({'CTUID': tracts_3347['CTUID'].astype(str)})
    df['CTUID'] = df['CTUID'].apply(_normalize_ctuid)
    # Normalize CTUIDs in exposure as well so merge keys align
    exp_norm = exposure_df.copy()
    exp_norm['CTUID'] = exp_norm['CTUID'].astype(str).apply(_normalize_ctuid)
    df = df.merge(exp_norm, on='CTUID', how='left')
    df['y'] = df['CTUID'].map(values_by_ctuid)

    # Track skip reasons BEFORE filtering
    skip_reasons = {}
    for idx, row in df.iterrows():
        ctuid = row['CTUID']
        y_val = row['y']
        dist_val = row['dist_to_gc_km']
        
        # Check metric value first
        has_valid_y = False
        if pd.notna(y_val):
            try:
                y_num = float(y_val)
                if np.isfinite(y_num):
                    has_valid_y = True
            except (ValueError, TypeError):
                pass
        
        if not has_valid_y:
            skip_reasons[ctuid] = 'Missing or invalid metric value'
            continue
            
        # Check distance
        has_valid_dist = False
        if pd.notna(dist_val):
            try:
                dist_num = float(dist_val)
                if np.isfinite(dist_num):
                    has_valid_dist = True
            except (ValueError, TypeError):
                pass
        
        if not has_valid_dist:
            skip_reasons[ctuid] = 'Missing golf exposure (no courses in range)'

    # Keep numeric and finite
    df = df[pd.to_numeric(df['y'], errors='coerce').notna()].copy()
    df = df[np.isfinite(df['dist_to_gc_km'])].copy()

    if df.empty:
        return None, skip_reasons

    # Build y, X
    y = df['y'].astype(float).values.reshape(-1, 1)
    X = df[['dist_to_gc_km', 'golf_count']].astype(float).values

    # Subset tracts to those rows with normalized CTUIDs and preserve order
    tr_norm = tracts_3347.copy()
    tr_norm['CTUID'] = tr_norm['CTUID'].astype(str).apply(_normalize_ctuid)
    sub = tr_norm.merge(df[['CTUID']], on='CTUID', how='inner')
    # Reorder to match df ordering so y/X align with W rows
    sub = sub.set_index('CTUID').loc[df['CTUID']].reset_index()
    
    # Build weights once and cache for reuse across metrics
    w_cache = getattr(fit_metric_spatial_lag_values, '_w_cache', {})
    cache_key = f"{len(sub)}_{','.join(sorted(sub['CTUID'].head(5).tolist()))}"
    if cache_key not in w_cache:
        try:
            from libpysal.weights import Queen
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')
                w = Queen.from_dataframe(sub, use_index=True)
                w.transform = 'r'
                w_cache[cache_key] = w
                fit_metric_spatial_lag_values._w_cache = w_cache
        except Exception:
            w = None
    else:
        w = w_cache[cache_key]
    
    model, y_hat = try_fit_spatial_lag(y, X, sub, w=w)

    if y_hat is None:
        # Fallback: OLS using numpy lstsq on [1, X]
        try:
            X_ = np.column_stack([np.ones((X.shape[0], 1)), X])
            coef, *_ = np.linalg.lstsq(X_, y, rcond=None)
            y_hat = X_ @ coef
        except Exception:
            return None, skip_reasons

    # Normalize for visualization: z-score
    y_hat = y_hat.reshape(-1)
    mu = float(np.mean(y_hat))
    sd = float(np.std(y_hat)) or 1.0
    z = (y_hat - mu) / sd

    out = pd.Series(z, index=df['CTUID'].values)
    return out, skip_reasons


def summarize(values: Iterable[float]) -> Dict[str, float]:
    arr = pd.Series(list(values), dtype=float)
    arr = arr.replace([np.inf, -np.inf], np.nan).dropna()
    if arr.empty:
        return {"count": 0}
    return {
        "count": int(arr.count()),
        "min": float(arr.min()),
        "max": float(arr.max()),
        "mean": float(arr.mean()),
        "p10": float(arr.quantile(0.10)),
        "p25": float(arr.quantile(0.25)),
        "p50": float(arr.quantile(0.50)),
        "p75": float(arr.quantile(0.75)),
        "p90": float(arr.quantile(0.90)),
    }
