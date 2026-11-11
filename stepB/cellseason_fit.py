"""Fit the seasonal term per sky-cell position (cellseason.py table).

Per (camera, gx, gy): r = q / weighted mean = c0 + s sin(lam) + c cos(lam);
errors scaled by sqrt(chi2/dof). Then: chi^2 of s across positions (is
the seasonal term the same everywhere in the field?), and weighted
regressions of s on the cell's position (field angles xi, eta; distance
from the camera centre).

usage: cellseason_fit.py cells_cam1.csv ...
"""

import csv, sys
import numpy as np


def wls(D, y, sy):
    w = 1 / sy ** 2
    M = D.T @ (w[:, None] * D)
    b = np.linalg.solve(M, D.T @ (w * y))
    chi = np.sum(w * (y - D @ b) ** 2) / max(len(y) - D.shape[1], 1)
    return b, np.sqrt(np.diag(np.linalg.inv(M)) * max(chi, 1)), chi


def main(*paths):
    rows = [{k: float(v) for k, v in r.items()} for p in paths for r in csv.DictReader(open(p))]
    rows = [r for r in rows if r['sq'] > 0 and np.isfinite(r['q'])]
    pos = sorted({(int(r['camera']), int(r['gx']), int(r['gy'])) for r in rows})
    res = []
    print('per position: seasonal amplitude s (sin lam) and c (cos lam), relative to the position mean')
    for p in pos:
        rr = [r for r in rows if (int(r['camera']), int(r['gx']), int(r['gy'])) == p]
        if len(rr) < 10:
            continue
        q = np.array([r['q'] for r in rr]); sq = np.array([r['sq'] for r in rr])
        w = 1 / sq ** 2
        m = np.sum(w * q) / w.sum()
        y, sy = q / m, sq / m
        lam = np.radians([r['earth_lon_deg'] for r in rr])
        D = np.stack([np.ones(len(y)), np.sin(lam), np.cos(lam)], 1)
        b, se, chi = wls(D, y, sy)
        xi = np.mean([r['xi'] for r in rr]); eta = np.mean([r['eta'] for r in rr])
        res.append((p, b[1], se[1], b[2], se[2], xi, eta, len(rr), chi))
    for cam in sorted({p[0] for p, *_ in res}):
        print(f'  camera {cam} (gy rows top to bottom, gx left to right): s ± sigma')
        for gy in (3, 2, 1, 0):
            cells = []
            for gx in range(4):
                m = [x for x in res if x[0] == (cam, gx, gy)]
                cells.append(f'{m[0][1]:+.2f}±{m[0][2]:.2f}'.rjust(13) if m else ' ' * 13)
            print('    ' + ''.join(cells))
    s = np.array([x[1] for x in res]); ss = np.array([x[2] for x in res])
    w = 1 / ss ** 2
    ms = np.sum(w * s) / w.sum()
    chi = np.sum(w * (s - ms) ** 2)
    print(f'\nall positions: mean s {ms:+.3f} ± {1 / np.sqrt(w.sum()):.3f}; chi2 of s about the mean {chi:.1f} '
          f'for {len(s) - 1} dof')
    for cam in sorted({x[0][0] for x in res}):
        k = [i for i, x in enumerate(res) if x[0][0] == cam]
        mc = np.sum(w[k] * s[k]) / w[k].sum()
        print(f'  camera {cam}: mean s {mc:+.3f} ± {1 / np.sqrt(w[k].sum()):.3f}; chi2 {np.sum(w[k] * (s[k] - mc) ** 2):.1f} '
              f'for {len(k) - 1} dof')
    xi = np.array([x[5] for x in res]); eta = np.array([x[6] for x in res])
    rad = np.hypot(xi, eta)
    for name, col in (('xi (deg)', xi), ('eta (deg)', eta), ('distance from camera centre (deg)', rad)):
        D = np.stack([np.ones(len(s)), (col - col.mean()) / col.std()], 1)
        b, se, _ = wls(D, s, ss)
        print(f'  s vs {name:34s} slope per 1 sd {b[1]:+.3f} ± {se[1]:.3f} ({b[1] / se[1]:+.1f} sigma)')


if __name__ == '__main__':
    main(*sys.argv[1:])
