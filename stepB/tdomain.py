"""Independent time-domain estimator, as a cross-check of the frequency-
domain fit (joint.py): regress the TESS background on lag-binned copies of
VIRGO, with a zero-delay Earthshine regressor, non-negative coefficients.

  y(t) = sum_b a_b * mean_{k in bin b} x(t - k*cadence) + E * eps(t) * x(t) + noise

y is the floor-normalised TESS fluctuation, x the VIRGO fractional
variation (both high-passed as in aligned.py), eps the level excess over
the sector floor. Rows are weighted by the inverse noise variance of their
6-h block (from a first unweighted pass). Errors: block bootstrap over
6-h blocks. Bins are coarser than in joint.py because 2-min data cannot
resolve below one cadence.

usage: tdomain.py datadir SECTORS [cameras] [dt]
"""

import sys
import numpy as np
from scipy.optimize import nnls

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint
from aligned import load

EDGES = [0, 240, 480, 960, 2040]
BLOCK_S = 6 * 3600
NBOOT = 50


def design(a, dt):
    cad = a['cadence']
    x, y, lev = a['x'], a['y'], a['level']
    n = len(x)
    fl = np.nanpercentile(lev, 5)
    ynorm = y * lev / fl
    eps = (lev - fl) / fl
    shift = int(round(dt / cad))
    cols = []
    for lo, hi in zip(EDGES[:-1], EDGES[1:]):
        ks = [k for k in range(int(np.ceil(lo / cad)), int(np.ceil(hi / cad)))] or [int(lo // cad)]
        acc = np.zeros(n)
        bad = np.zeros(n, bool)
        for k in ks:
            k2 = k + shift
            xs = np.full(n, np.nan)
            if k2 >= 0:
                xs[k2:] = x[:n - k2] if k2 else x
            else:
                xs[:k2] = x[-k2:]
            bad |= ~np.isfinite(xs)
            acc += np.nan_to_num(xs)
        acc /= len(ks)
        acc[bad] = np.nan
        cols.append(acc)
    cols.append(eps * x)
    D = np.array(cols).T
    ok = np.isfinite(D).all(1) & np.isfinite(ynorm)
    block = (np.arange(n) * cad // BLOCK_S).astype(int)
    return D[ok], ynorm[ok], block[ok]


def main(datadir, spec, cameras='1,2', dt='0'):
    dt = float(dt)
    rng = np.random.default_rng(9)
    print(f'time-domain NNLS, {spec}, dt = {dt:+.0f} s; bins {EDGES} s (per unit floor)')
    for cam in (int(c) for c in cameras.split(',')):
        Ds, ys, bl = [], [], []
        b0 = 0
        for sec, prod in joint.parse_sectors(spec):
            try:
                a = load(datadir, sec, cam, 'red', product=prod)
            except FileNotFoundError:
                continue
            D, y, block = design(a, dt)
            # First pass per chunk for block noise weights.
            coef, *_ = np.linalg.lstsq(D, y, rcond=None)
            r = y - D @ coef
            for b in np.unique(block):
                m = block == b
                s = np.std(r[m])
                if s > 0 and m.sum() > 30:
                    Ds.append(D[m] / s), ys.append(y[m] / s), bl.append(np.full(m.sum(), b0 + b))
            b0 += block.max() + 1
        D, y, bl = np.vstack(Ds), np.concatenate(ys), np.concatenate(bl)
        sol = nnls(D, y)[0]
        nb = bl.max() + 1
        bs = []
        for _ in range(NBOOT):
            w = np.sqrt(np.bincount(rng.integers(0, nb, nb), minlength=nb)[bl])
            bs.append(nnls(D * w[:, None], y * w)[0])
        bs = np.array(bs)
        lo, hi = np.percentile(bs, [16, 84], axis=0)
        print(f'  cam {cam}: ' + '  '.join(f'{v:.3f} [{l:.2f},{h:.2f}]' for v, l, h in
                                          zip(sol[:-1], lo[:-1], hi[:-1])) +
              f'   E {sol[-1]:.2f} [{lo[-1]:.2f},{hi[-1]:.2f}]   total {sol[:-1].sum():.2f} '
              f'[{np.percentile(bs[:, :-1].sum(1), 16):.2f},{np.percentile(bs[:, :-1].sum(1), 84):.2f}]'
              f'   ({len(y)} rows)')


if __name__ == '__main__':
    main(*sys.argv[1:])
