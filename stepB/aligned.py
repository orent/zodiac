"""Aligned TESS / VIRGO series from the compact data branch, on the common
emission-time base (see timebase.py, TIMEBASE.md).

The grid is VIRGO's own minute stamps (emission time). TESS 20-s samples
are overlap-binned onto exactly 60-s boxes centred on them.
"""

import os
import numpy as np
from scipy.ndimage import median_filter

import timebase as tb

NULL_MARGIN_DAYS = 400


def highpass(x, size=180):
    y = np.full_like(x, np.nan)
    i = np.flatnonzero(np.isfinite(x))
    y[i] = x[i] - median_filter(x[i], size=size, mode='nearest')
    return y


def load(datadir, sector, cam, ref='red', stamp=tb.VIRGO_STAMP):
    sdir = os.path.join(datadir, f's{int(sector):04d}')
    eph = tb.Ephem(os.path.join(sdir, 'ephem.npz'))
    c = np.load(os.path.join(sdir, f'cam{cam}.npz'))
    t_tess = tb.tess_emission(c['t_sc'], eph)

    if ref == 'red':
        v = np.load(os.path.join(datadir, 'virgo', 'spm_red_1min.npz'))
        tv = v['days_since_1996_01_23_utc']
        xv = v['red_ppm'].astype(float) * 1e-6
        conv = lambda t: tb.virgo_red_emission(t, eph, stamp)
        # Pre-select a generous window in native units, then convert.
        t0 = (tb.Time(2457000 + c['t_sc'][0], format='jd', scale='tdb') - tb.RED_EPOCH).jd
        t1 = (tb.Time(2457000 + c['t_sc'][-1], format='jd', scale='tdb') - tb.RED_EPOCH).jd
        sel = np.flatnonzero((tv > t0 - 1) & (tv < t1 + 1))
    elif ref == 'tsi':
        v = np.load(os.path.join(datadir, 'virgo', 'tsi_pmo6_1min.npz'))
        tv = v['tai_seconds_since_1958']
        raw = v['tsi_wm2_wrr'].astype(float)
        xv = raw / np.nanmedian(raw) - 1
        conv = lambda t: tb.virgo_tsi_emission(t, eph, stamp)
        t0 = (tb.Time(2457000 + c['t_sc'][0], format='jd', scale='tdb') - tb.TAI_EPOCH).sec
        t1 = (tb.Time(2457000 + c['t_sc'][-1], format='jd', scale='tdb') - tb.TAI_EPOCH).sec
        sel = np.flatnonzero((tv > t0 - 86400) & (tv < t1 + 86400))
    else:
        raise ValueError(ref)

    # The ephemeris only covers the sector, so emission-time conversion is
    # done for the sector window; null-shift VIRGO segments are used by
    # index offset (whole days of 1440 minutes) and keep their own data.
    t_grid = conv(tv[sel])
    y = bin_tess = tb.bin_to_minutes(t_grid, t_tess, c['frac'].astype(float))
    lev = tb.bin_to_minutes(t_grid, t_tess, c['level'].astype(float))
    off = NULL_MARGIN_DAYS * 1440
    lo, hi = max(sel[0] - off, 0), sel[-1] + 1 + off
    x_all = highpass(xv[lo:hi])
    return dict(t=t_grid, x=x_all[sel[0] - lo:sel[0] - lo + len(sel)], x_all=x_all,
                off=sel[0] - lo, y=highpass(y), level=lev, n=len(sel))
