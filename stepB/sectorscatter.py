"""Sector-to-sector scatter of the echo amplitude, and group comparisons
with honest errors.

Group means (north/south, year to year) have differed by 20-35% while the
block-bootstrap errors suggested ~5-10%. This measures the excess scatter
of the per-sector amplitude directly. One product per sector, so that
products sharing the same sky are not counted twice: 2-min for S1-26,
20-s from S27 on. For each (sector, camera) chunk: A at alpha 1.4 with E
fixed at the camera's pooled value (northsouth.chunk_fit) and the model
brightness B (tilted Kelsall/Hong), giving q = Z/B = A * floor / 1.15 / B.
Then per camera:
  - chi^2 of q about the weighted mean, using the bootstrap errors;
  - the extra per-sector scatter s_x (fraction of the mean) that makes
    chi^2/dof = 1;
  - group means with errors from sqrt(sigma_boot^2 + s_x^2).

usage: VIRGO_REF=red23 [SCATTER_PRODUCT=2min] sectorscatter.py datadir [cameras]
"""

import os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['GEOM_TILT'] = '1'
import geometry
import joint
import northsouth as ns

from sectors import STANDARD as _STD, ECLIPTIC as _ECL, SOUTH, NORTH
STANDARD = sorted(_STD)
ECLIPTIC = sorted(_ECL)
GROUPS = [
    ('year 1 south S1-13', lambda s: 1 <= s <= 13),
    ('year 2 north S17-23', lambda s: 17 <= s <= 23),
    ('year 3 south S27-39', lambda s: 27 <= s <= 39),
    ('year 4 north S47-55', lambda s: s in (47, 48, 49, 50, 51, 54, 55)),
    ('year 5 north S56-60', lambda s: 56 <= s <= 60),
    ('year 5 south S61-63', lambda s: 61 <= s <= 63),
    ('year 5-7 south S64-69,87-90', lambda s: s in range(64, 70) or s in range(87, 91)),
    ('year 6 north S73,80-86', lambda s: s == 73 or s in range(80, 87)),
    ('all south', lambda s: s in SOUTH),
    ('all north', lambda s: s in NORTH),
]


def product(s):
    # SCATTER_PRODUCT=2min: the 2-min product for every sector (it exists
    # for all of S1-63), to test whether a result depends on the product.
    if os.environ.get('SCATTER_PRODUCT') == '2min':
        return '_2min'
    return '_2min' if s <= 26 else ''


def wmean(q, s):
    w = 1 / s ** 2
    return np.sum(w * q) / np.sum(w), 1 / np.sqrt(np.sum(w))


def extra_scatter(q, s):
    """s_x with chi^2/dof = 1 about the weighted mean (0 if already <= 1)."""
    def chi(sx):
        st = np.sqrt(s ** 2 + sx ** 2)
        m, _ = wmean(q, st)
        return np.sum(((q - m) / st) ** 2) / (len(q) - 1)
    if chi(0) <= 1:
        return 0.0
    lo, hi = 0.0, 10 * np.std(q)
    for _ in range(60):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if chi(mid) > 1 else (lo, mid)
    return (lo + hi) / 2


def main(datadir, cameras='1,2,3,4'):
    joint.DTS = np.array([0.0])
    rng = np.random.default_rng(57)
    for cam in (int(c) for c in cameras.split(',')):
        chunks = {}
        for s in STANDARD + ECLIPTIC:
            try:
                chunks[s] = joint.Chunk(datadir, s, product(s), cam, 'red')
            except FileNotFoundError:
                continue
        std = [chunks[s] for s in STANDARD if s in chunks]
        tp = joint.Terms([(c.f, c.cadence, c.X, c.Y, c.eps, c.u) for c in std], None, alphas=[ns.ALPHA])
        E = tp.solve()[1][0, 0, 1]
        rows = {}
        for s, ch in chunks.items():
            A, sA = ns.chunk_fit(ch, E, rng)
            B = geometry.brightness(ch.u, ns.ALPHA)
            rows[s] = (A * ch.floor / 1.15 / B, sA * ch.floor / 1.15 / B)
        ss = [s for s in STANDARD if s in rows]
        q = np.array([rows[s][0] for s in ss]); sq = np.array([rows[s][1] for s in ss])
        m0, _ = wmean(q, sq)
        q, sq = q / m0, sq / m0
        chi0 = np.sum(((q - wmean(q, sq)[0]) / sq) ** 2) / (len(q) - 1)
        sx = extra_scatter(q, sq)
        st = np.sqrt(sq ** 2 + sx ** 2)
        m, sm = wmean(q, st)
        print(f'camera {cam}: {len(ss)} standard-pointing sectors, E fixed {E:+.2f}')
        print(f'  per-sector q = Z/B (normalised to the weighted mean): median bootstrap sigma {np.median(sq):.2f}; '
              f'chi2/dof {chi0:.2f}; extra per-sector scatter {sx:.2f}')
        print(f'  archive mean with honest errors: {m:.3f} ± {sm:.3f} (bootstrap-only ± {wmean(q, sq)[1]:.3f})')
        for name, sel in GROUPS:
            k = [i for i, s in enumerate(ss) if sel(s)]
            if not k:
                continue
            gm, gs = wmean(q[k], st[k])
            print(f'  {name:22s} n {len(k):2d}  {gm:.3f} ± {gs:.3f}   ({(gm - m) / gs:+.1f} sigma from the archive mean)')
        g = dict(GROUPS)
        n_s = [i for i, s in enumerate(ss) if g['all south'](s)]
        n_n = [i for i, s in enumerate(ss) if g['all north'](s)]
        (a, sa), (b, sb) = wmean(q[n_n], st[n_n]), wmean(q[n_s], st[n_s])
        print(f'  north - south: {a - b:+.3f} ± {np.hypot(sa, sb):.3f} ({(a - b) / np.hypot(sa, sb):+.1f} sigma)')
        ec = [s for s in ECLIPTIC if s in rows]
        if ec:
            qe = np.array([rows[s][0] for s in ec]) / m0
            se = np.sqrt((np.array([rows[s][1] for s in ec]) / m0) ** 2 + sx ** 2)
            em, es = wmean(qe, se)
            print(f'  ecliptic S42-46 (new geometry) n {len(ec)}  {em:.3f} ± {es:.3f}   '
                  f'({(em - m) / np.hypot(es, sm):+.1f} sigma from the standard-pointing mean)')
        print('  per sector: ' + ' '.join(f'{s}:{rows[s][0] / m0:.2f}' for s in ss))
        print()


if __name__ == '__main__':
    main(*sys.argv[1:])
