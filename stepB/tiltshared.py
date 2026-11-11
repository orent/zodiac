"""Can one tilted plane explain why the cameras' seasonal terms differ?

Models of r (epochs.py table, Earth's true distance in B):
  A  per-camera offsets + per-camera seasonal (s_k, c_k)          [no tilt]
  B  per-camera offsets + one common seasonal (s, c)              [no tilt]
  C  model B + k T(node), node scanned                            [one plane]
  D  model A + k T(node)                                          [both]
If C fits about as well as A (and D adds little), the camera dependence
of the seasonal term is the geometry of a single extra tilt; if C stays
well above A, the cameras differ for another (likely instrumental) reason.

usage: tiltshared.py datadir epochs_cam1.csv ...
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import tiltscale as ts


def chi2(D, y, sy):
    w = 1 / sy ** 2
    M = D.T @ (w[:, None] * D)
    b = np.linalg.solve(M, D.T @ (w * y))
    return np.sum(w * (y - D @ b) ** 2), b, np.sqrt(np.diag(np.linalg.inv(M)))


def main(datadir, *tables):
    rows = ts.rows_of(datadir, 'epochs', tables)
    y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
    lam = np.radians([r['earth_lon_deg'] for r in rows])
    cams = np.array([int(r['camera']) for r in rows])
    offs = [(cams == c).astype(float) for c in (1, 2, 3, 4)]
    per = [m * f(lam) for m in offs for f in (np.sin, np.cos)]
    common = [np.sin(lam), np.cos(lam)]
    cA, _, _ = chi2(np.stack(offs + per, 1), y, sy)
    cB, _, _ = chi2(np.stack(offs + common, 1), y, sy)
    print(f'{len(y)} rows')
    print(f'  A per-camera season, no tilt     chi2 {cA:7.1f}  (params {4 + 8})')
    print(f'  B common season, no tilt         chi2 {cB:7.1f}  (params {4 + 2})')
    best = None
    for node in range(0, 360, 15):
        T = np.array([ts.tilt_effect(r['u'], r['rE'], node) for r in rows])
        cC, bC, sC = chi2(np.stack(offs + common + [T], 1), y, sy)
        cD, bD, sD = chi2(np.stack(offs + per + [T], 1), y, sy)
        if best is None or cC < best[1]:
            best = (node, cC, bC[-1], sC[-1], cD, bD[-1], sD[-1])
        print(f'  node {node:3d}:  C (common season + tilt) chi2 {cC:7.1f}, k {bC[-1]:+.2f}±{sC[-1]:.2f}   '
              f'D (per-camera + tilt) chi2 {cD:7.1f}, k {bD[-1]:+.2f}±{sD[-1]:.2f}')
    node, cC, kC, sC, cD, kD, sD = best
    print(f'\nbest single plane: node {node} deg, k {kC:+.2f}±{sC:.2f}, chi2 {cC:.1f} (params 8 incl. node)')
    print(f'  vs A (per-camera season, 12 params): delta chi2 {cC - cA:+.1f}; '
          f'vs B (common season, 6): {cC - cB:+.1f}; D at that node: {cD:.1f}')
    print(f'  reduced chi2 of A: {cA / (len(y) - 12):.2f}; scale delta chi2 by it for significance')


if __name__ == '__main__':
    main(*sys.argv[1:])
