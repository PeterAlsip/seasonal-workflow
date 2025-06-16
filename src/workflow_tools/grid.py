import os

import numpy as np
import xarray
import xesmf


def center_to_outer(center, left=None, right=None):
    """
    Given an array of center coordinates, find the edge coordinates,
    including extrapolation for far left and right edge.
    """
    edges = 0.5 * (center.values[0:-1] + center.values[1:])
    if left is None:
        left = edges[0] - (edges[1] - edges[0])
    if right is None:
        right = edges[-1] + (edges[-1] - edges[-2])
    outer = np.hstack([left, edges, right])
    return outer


def reuse_regrid(*args, **kwargs):
    filename = kwargs.pop('filename', None)
    reuse_weights = kwargs.pop('reuse_weights', False)

    if reuse_weights:
        if os.path.isfile(filename):
            return xesmf.Regridder(
                *args, reuse_weights=True, filename=filename, **kwargs
            )
        else:
            regrid = xesmf.Regridder(*args, **kwargs)
            regrid.to_netcdf(filename)
            return regrid
    else:
        regrid = xesmf.Regridder(*args, **kwargs)
        return regrid


def round_coords(
    ds: xarray.Dataset, to: float, lat: str = 'latitude', lon: str = 'longitude'
) -> xarray.Dataset:
    ds[lat] = np.round(ds[lat] * to) / to
    ds[lon] = np.round(ds[lon] * to) / to
    return ds
