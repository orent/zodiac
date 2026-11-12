"""Hemisphere x season test on the epochs.py table.

A symmetry plane of the local dust that differs from the Kelsall plane
already in the model (i 2.03 deg, node 77.7 deg) would modulate northern
and southern sightlines in opposite senses over the year, with the phase
of the plane's node: r ~ c0 + h * (a sin lam + b cos lam) with h = +1 north,
-1 south, lam = Earth's heliocentric longitude. A hemisphere-independent
seasonal term (s sin lam + c cos lam) and a plain north offset are fitted
alongside. Also: north - south at matched season (90-deg bins of lam).
Errors are scaled by sqrt(chi2/dof).

usage: [ECCENTRIC=1] epochs_tilt.py datadir cam1.csv [cam2.csv ...]
  ECCENTRIC=1: model brightness with Earth's true heliocentric distance
"""

import csv, os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['GEOM_TILT'] = '1'
import geometry
import joint

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from sectors import STANDARD as STD


ECC, PERI_LON = 0.0167, 102.9          # Earth's orbit: eccentricity, longitude of perihelion [deg]


def load(datadir, paths):
    rows = [{k: float(v) for k, v in r.items()} for p in paths for r in csv.DictReader(open(p))]
    rows = [r for r in rows if int(r['sector']) in STD and r['sq'] > 0]
    if os.environ.get('ECCENTRIC') == '1':
        # The table's B has Earth at 1 AU; rescale q to the model with Earth's
        # true distance r_E = a(1 - e^2) / (1 + e cos(lam - perihelion)).
        for r in rows:
            u = joint.sightline(datadir, int(r['sector']), int(r['camera']))
            lam = np.radians(r['earth_lon_deg'])
            rE = (1 - ECC ** 2) / (1 + ECC * np.cos(lam - np.radians(PERI_LON)))
            f = geometry.brightness(u, 1.4) / geometry.brightness(u, 1.4, r_earth=rE)
            r['q'] *= f
            r['sq'] *= f
    for c in {r['camera'] for r in rows}:
        rc = [r for r in rows if r['camera'] == c]
        w = np.array([1 / r['sq'] ** 2 for r in rc])
        m = np.sum(w * np.array([r['q'] for r in rc])) / w.sum()
        for r in rc:
            r['r'], r['sr'] = r['q'] / m, r['sq'] / m
    return rows


def wls(y, sy, cols):
    D = np.stack([np.ones(len(y))] + cols, 1)
    w = 1 / sy ** 2
    M = D.T @ (w[:, None] * D)
    b = np.linalg.solve(M, D.T @ (w * y))
    chi = np.sum(w * (y - D @ b) ** 2) / (len(y) - D.shape[1])
    C = np.linalg.inv(M) * max(chi, 1)
    return b, C, chi


def main(datadir, *paths):
    rows = load(datadir, paths)
    y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
    lam = np.radians([r['earth_lon_deg'] for r in rows])
    h = np.array([1.0 if r['ecl_lat_deg'] > 0 else -1.0 for r in rows])
    names = ['north offset (h)', 'h sin lam', 'h cos lam', 'sin lam', 'cos lam']
    b, C, chi = wls(y, sy, [h, h * np.sin(lam), h * np.cos(lam), np.sin(lam), np.cos(lam)])
    print(f'joint fit, {len(y)} (sector, camera) rows, chi2/dof {chi:.2f} (errors scaled):')
    for n, bb, s in zip(names, b[1:], np.sqrt(np.diag(C))[1:]):
        print(f'   {n:18s} {bb:+.3f} ± {s:.3f}  ({bb / s:+.1f} sigma)')
    a, bc = b[2], b[3]
    amp = np.hypot(a, bc)
    node = np.degrees(np.arctan2(-bc, a)) % 360        # h * amp * sin(lam - node)
    print(f'   hemisphere-antisymmetric seasonal term: amplitude {amp:.3f}, phase: h * sin(lam - {node:.0f} deg)')
    # Model expectation of the same term from the Kelsall tilt (already in B): its size
    # for comparison, from B(tilted)/B(flat) at the camera sightlines.
    ex = []
    for r in rows:
        u = joint.sightline(datadir, int(r['sector']), int(r['camera']))
        geometry.TILT = True
        b1 = geometry.brightness(u, 1.4)
        geometry.TILT = False
        b0 = geometry.brightness(u[:3], 1.4)
        geometry.TILT = True
        ex.append(b1 / b0 - 1)
    ex = np.array(ex)
    be, _, _ = wls(ex, np.ones(len(ex)), [h * np.sin(lam), h * np.cos(lam)])
    print(f'   for scale, the modelled Kelsall tilt itself: h-antisymmetric amplitude {np.hypot(be[1], be[2]):.3f}, '
          f'phase sin(lam - {np.degrees(np.arctan2(-be[2], be[1])) % 360:.0f} deg)')
    print('\nnorth - south at matched season (bins of Earth longitude; errors scaled by the bin chi2):')
    for lo in range(0, 360, 90):
        m = (np.degrees(lam) % 360 >= lo) & (np.degrees(lam) % 360 < lo + 90)
        out = []
        for hs in (1, -1):
            k = m & (h == hs)
            if k.sum() < 3:
                out.append(None); continue
            w = 1 / sy[k] ** 2
            mu = np.sum(w * y[k]) / w.sum()
            chi = np.sum(w * (y[k] - mu) ** 2) / (k.sum() - 1)
            out.append((mu, np.sqrt(max(chi, 1) / w.sum()), k.sum()))
        if None in out:
            print(f'   lam {lo:3d}-{lo + 90:3d}: ' + ', '.join('north' if hs == 1 else 'south'
                  for hs, o in zip((1, -1), out) if o) + ' only')
            continue
        (a1, s1, n1), (a2, s2, n2) = out
        print(f'   lam {lo:3d}-{lo + 90:3d}: north {a1:.3f}±{s1:.3f} (n {n1})  south {a2:.3f}±{s2:.3f} (n {n2})  '
              f'N-S {a1 - a2:+.3f}±{np.hypot(s1, s2):.3f} ({(a1 - a2) / np.hypot(s1, s2):+.1f} sigma)')


if __name__ == '__main__':
    main(*sys.argv[1:])
