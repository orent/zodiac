"""Fit the strength of the cloud's tilt from the echo.

The model brightness B already contains the Kelsall tilt (i 2.03 deg,
node 77.7 deg). Its effect per row, T = B_tilted / B_flat - 1, depends on
the sightline (largest at high ecliptic latitude), the hemisphere and the
season. Fitting
    r = c0 + s sin(lam) + c cos(lam) + k T
gives k: 0 if the Kelsall tilt is right, -1 if there were no tilt, +1 if
the tilt effect were twice Kelsall's. Also with the node free: T split
into its sin/cos parts by fitting T_node(Omega) over a grid of nodes.
Works on the camera table (epochs.py) and the cell table (cellseason.py).

usage: tiltscale.py datadir epochs|cells table.csv ...
"""

import csv, os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['GEOM_TILT'] = '1'
import geometry
import joint
import leadtrail as lt

ECC, PERI_LON = 0.0167, 102.9
sys.path.insert(0, __file__.rsplit('/', 1)[0])
from sectors import STANDARD as STD


def rows_of(datadir, kind, paths):
    rows = [{k: float(v) for k, v in r.items()} for p in paths for r in csv.DictReader(open(p))]
    rows = [r for r in rows if int(r['sector']) in STD and r['sq'] > 0 and np.isfinite(r['q'])]
    cache = {}
    for r in rows:
        s, c = int(r['sector']), int(r['camera'])
        lam = np.radians(r['earth_lon_deg'])
        rE = (1 - ECC ** 2) / (1 + ECC * np.cos(lam - np.radians(PERI_LON)))
        if kind == 'epochs':
            u = joint.sightline(datadir, s, c)
        else:
            key = (s, c, int(r['gx']), int(r['gy']))
            if s not in cache:
                C = np.load(os.path.join(datadir, f's{s:04d}_2min', 'cells.npz'))
                cache[s] = (C['cells'], list(C['cells_columns']))
            cells, cols = cache[s]
            row = [x for x in cells if int(x[0]) == c and int(x[1]) == key[2] and int(x[2]) == key[3]][0]
            d = dict(zip(cols, row))
            sun = np.degrees(lam) - 180
            u = np.append(lt.to_sun_frame(np.array([d['ux'], d['uy'], d['uz']]), sun), lam)
        r['u'] = u
        r['rE'] = rE
        if kind == 'epochs':
            # The epochs table's B had Earth at 1 AU: bring q to the eccentric model.
            r['q'] *= geometry.brightness(u, 1.4) / geometry.brightness(u, 1.4, r_earth=rE)
            r['sq'] *= geometry.brightness(u, 1.4) / geometry.brightness(u, 1.4, r_earth=rE)
    groups = {}
    for r in rows:
        g = (int(r['camera']),) if kind == 'epochs' else (int(r['camera']), int(r['gx']), int(r['gy']))
        groups.setdefault(g, []).append(r)
    for g, rr in groups.items():
        w = np.array([1 / r['sq'] ** 2 for r in rr])
        m = np.sum(w * np.array([r['q'] for r in rr])) / w.sum()
        for r in rr:
            r['r'], r['sr'] = r['q'] / m, r['sq'] / m
    return rows


def tilt_effect(u, rE, node_deg):
    old = geometry.KELSALL_NODE
    geometry.KELSALL_NODE = np.radians(node_deg)
    geometry.TILT = True
    b1 = geometry.brightness(u, 1.4, r_earth=rE)
    geometry.KELSALL_NODE = old
    geometry.TILT = False
    b0 = geometry.brightness(u[:3], 1.4, r_earth=rE)
    geometry.TILT = True
    return b1 / b0 - 1


def fit(rows, T, label):
    y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
    lam = np.radians([r['earth_lon_deg'] for r in rows])
    cams = sorted({int(r['camera']) for r in rows})
    # Per-camera seasonal terms (the cameras differ), one common tilt scale k.
    cols = []
    for c in cams:
        m = np.array([int(r['camera']) == c for r in rows], float)
        cols += [m, m * np.sin(lam), m * np.cos(lam)]
    D = np.stack(cols + [T], 1)
    w = 1 / sy ** 2
    M = D.T @ (w[:, None] * D)
    b = np.linalg.solve(M, D.T @ (w * y))
    chi2 = np.sum(w * (y - D @ b) ** 2)
    dof = len(y) - D.shape[1]
    se = np.sqrt(np.diag(np.linalg.inv(M)) * max(chi2 / dof, 1))
    print(f'  {label}: k = {b[-1]:+.2f} ± {se[-1]:.2f}  (chi2 {chi2:.1f}, dof {dof})')
    for i, c in enumerate(cams):
        print(f'      cam {c}: s {b[3 * i + 1]:+.3f}±{se[3 * i + 1]:.3f}  c {b[3 * i + 2]:+.3f}±{se[3 * i + 2]:.3f}')
    return chi2


def main(datadir, kind, *paths):
    rows = rows_of(datadir, kind, paths)
    print(f'{kind}: {len(rows)} rows')
    T = np.array([tilt_effect(r['u'], r['rE'], 77.7) for r in rows])
    print(f'  model tilt effect T (Kelsall node 77.7): rms {T.std():.3f}, max |T| {abs(T).max():.3f}')
    fit(rows, np.zeros(len(rows)) + 1e-12 * np.arange(len(rows)), 'no tilt term (reference)')
    fit(rows, T, 'Kelsall node 77.7 deg')
    print('  node scan (k fitted at each node):')
    for node in range(0, 360, 30):
        Tn = np.array([tilt_effect(r['u'], r['rE'], node) for r in rows])
        y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
        lam = np.radians([r['earth_lon_deg'] for r in rows])
        cams = sorted({int(r['camera']) for r in rows})
        cols = []
        for c in cams:
            m = np.array([int(r['camera']) == c for r in rows], float)
            cols += [m, m * np.sin(lam), m * np.cos(lam)]
        D = np.stack(cols + [Tn], 1)
        w = 1 / sy ** 2
        M = D.T @ (w[:, None] * D)
        b = np.linalg.solve(M, D.T @ (w * y))
        chi2 = np.sum(w * (y - D @ b) ** 2)
        se = np.sqrt(np.linalg.inv(M)[-1, -1] * max(chi2 / (len(y) - D.shape[1]), 1))
        print(f'    node {node:3d}: k {b[-1]:+.2f} ± {se:.2f}   chi2 {chi2:.1f}')


if __name__ == '__main__':
    main(*sys.argv[1:])
