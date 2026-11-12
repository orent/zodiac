"""Shared season + extra tilt, with stray-light and time-trend controls.

Fits r (epochs.py table, Earth's true distance in B) with per-camera
offsets, a common sin/cos(lam) season, k T(node) (tiltscale.tilt_effect)
and optional controls: the window-level excess eps_mean and eps_p90 (stray
light; standardised per camera) and a linear trend in sector number. Errors
scaled by sqrt(chi2/dof). Reports k, the season, the control coefficients
and the node scan for the full model.

usage: tiltcontrol.py datadir epochs_cam1.csv ...
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import tiltscale as ts


def fit(D, y, sy):
    w = 1 / sy ** 2
    M = D.T @ (w[:, None] * D)
    b = np.linalg.solve(M, D.T @ (w * y))
    chi2 = np.sum(w * (y - D @ b) ** 2)
    se = np.sqrt(np.diag(np.linalg.inv(M)) * max(chi2 / (len(y) - D.shape[1]), 1))
    return b, se, chi2


def main(datadir, *tables):
    rows = ts.rows_of(datadir, 'epochs', tables)
    y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
    lam = np.radians([r['earth_lon_deg'] for r in rows])
    cams = np.array([int(r['camera']) for r in rows])
    sec = np.array([r['sector'] for r in rows])
    offs = [(cams == c).astype(float) for c in (1, 2, 3, 4)]

    def std_per_cam(x):
        out = np.zeros(len(x))
        for c in (1, 2, 3, 4):
            m = cams == c
            out[m] = (x[m] - x[m].mean()) / (x[m].std() or 1)
        return out

    eps = std_per_cam(np.array([r['eps_mean'] for r in rows]))
    eps90 = std_per_cam(np.array([r['eps_p90'] for r in rows]))
    trend = (sec - sec.mean()) / sec.std()
    controls = {'none': [], 'stray light': [eps, eps90], 'trend': [trend],
                'stray light + trend': [eps, eps90, trend]}
    print(f'{len(y)} rows')
    for name, ctrl in controls.items():
        best = None
        for node in range(0, 180, 15):
            T = np.array([ts.tilt_effect(r['u'], r['rE'], node) for r in rows])
            b, se, c2 = fit(np.stack(offs + [np.sin(lam), np.cos(lam)] + ctrl + [T], 1), y, sy)
            if best is None or c2 < best[2]:
                best = (node, b, c2, se)
        b0, se0, c0 = fit(np.stack(offs + [np.sin(lam), np.cos(lam)] + ctrl, 1), y, sy)
        node, b, c2, se = best
        red = c0 / (len(y) - len(offs) - 2 - len(ctrl))
        ctrl_s = ' '.join(f'{v:+.3f}±{e:.3f}' for v, e in zip(b[6:-1], se[6:-1]))
        print(f'  controls: {name:20s} season-only s {b0[4]:+.3f}±{se0[4]:.3f} c {b0[5]:+.3f}±{se0[5]:.3f} '
              f'(chi2 {c0:.1f})')
        print(f'      + tilt: best node {node:3d}  k {b[-1]:+.2f}±{se[-1]:.2f}  s {b[4]:+.3f}±{se[4]:.3f} '
              f'c {b[5]:+.3f}±{se[5]:.3f}  chi2 {c2:.1f} (delta {c2 - c0:+.1f}, scaled {(c2 - c0) / red:+.1f})'
              + (f'  controls {ctrl_s}' if ctrl else ''))


if __name__ == '__main__':
    main(*sys.argv[1:])
