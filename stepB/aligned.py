"""Aligned TESS / VIRGO series from the compact data branch, on the common
emission-time base (see timebase.py, TIMEBASE.md).

20-s products (sNNNN): the grid is VIRGO's own minute stamps; TESS 20-s
samples are overlap-binned onto exactly 60-s boxes centred on them.
2-min products (sNNNN_2min): the grid is the TESS 120-s cadence; VIRGO
1-min samples are overlap-binned onto the 120-s TESS boxes.
Either way both series end up with the same box response.
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


def load(datadir, sector, cam, ref='red', stamp=tb.VIRGO_STAMP, product=''):
    sdir = os.path.join(datadir, f's{int(sector):04d}{product}')
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

    cadence = float(c['cadence']) if 'cadence' in c.files else 20.0
    if cadence > 60:
        return _load_tess_grid(c, t_tess, tv, xv, sel, conv, cadence)

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
                off=sel[0] - lo, y=highpass(y), level=lev, n=len(sel), cadence=60.0,
                per_day=1440)


def _load_tess_grid(c, t_tess, tv, xv, sel, conv, cadence):
    """2-min products: everything on the TESS cadence grid."""
    hp = int(round(3 * 3600 / cadence))
    per_day = int(round(86400 / cadence))
    margin = NULL_MARGIN_DAYS * per_day
    n = len(t_tess)
    # Regular grid extended by the null margin. TESS emission times stay
    # within ~1 s of it over a sector, which only matters for the real
    # (unshifted) segment, so that one is binned on the exact times.
    t0 = t_tess[0]
    t_g = t0 + (np.arange(-margin, n + margin)) * cadence
    span = (t_g[0] - 86400, t_g[-1] + 86400)
    # VIRGO in native units around the whole extended range.
    tvs = conv(tv)                          # light time clamps outside the ephemeris
    keep = (tvs > span[0]) & (tvs < span[1])
    x_all = highpass(tb.bin_to_minutes(t_g, tvs[keep], xv[keep], sample=60.0, width=cadence), hp)
    x = highpass(tb.bin_to_minutes(t_tess, tvs[keep], xv[keep], sample=60.0, width=cadence), hp)
    y = highpass(c['frac'].astype(float), hp)
    lev = c['level'].astype(float)
    return dict(t=t_tess, x=x, x_all=x_all, off=margin, y=y, level=lev, n=n,
                cadence=cadence, per_day=per_day)
