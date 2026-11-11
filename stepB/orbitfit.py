"""Seasonal term: calendar (Earth longitude) or TESS orbit orientation?

Fits the (sector, camera) table of epochs.py (normalised Z/B, Earth's true
distance in the model) with r = c0 + h + {sin, cos} of either Earth
longitude lam or the apogee-minus-anti-Sun angle phi (orbitgeom.csv),
or both, and compares chi^2. The orbit precesses slowly, so phi drifts
against lam by ~10 deg/yr; over five years this separates the two.

usage: orbitfit.py datadir orbitgeom.csv epochs_cam1.csv ...
"""

import csv, os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['ECCENTRIC'] = '1'
import epochs_tilt as et


def main(datadir, orbit, *tables):
    rows = et.load(datadir, tables)
    og = {int(r['sector']): r for r in csv.DictReader(open(orbit))}
    rows = [r for r in rows if int(r['sector']) in og]
    y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
    lam = np.radians([r['earth_lon_deg'] for r in rows])
    phi = np.radians([float(og[int(r['sector'])]['apogee_minus_antisun_deg']) for r in rows])
    h = np.array([1.0 if r['ecl_lat_deg'] > 0 else -1.0 for r in rows])
    print(f'correlation of sin(lam) with sin(phi): {np.corrcoef(np.sin(lam), np.sin(phi))[0, 1]:+.2f}, '
          f'cos: {np.corrcoef(np.cos(lam), np.cos(phi))[0, 1]:+.2f}')
    for name, cols in [('offset + h only', [h]),
                       ('calendar: sin/cos lam', [h, np.sin(lam), np.cos(lam)]),
                       ('orbit: sin/cos phi', [h, np.sin(phi), np.cos(phi)]),
                       ('both', [h, np.sin(lam), np.cos(lam), np.sin(phi), np.cos(phi)])]:
        D = np.stack([np.ones(len(y))] + cols, 1)
        w = 1 / sy ** 2
        M = D.T @ (w[:, None] * D)
        b = np.linalg.solve(M, D.T @ (w * y))
        chi2 = np.sum(w * (y - D @ b) ** 2)
        se = np.sqrt(np.diag(np.linalg.inv(M)) * max(chi2 / (len(y) - D.shape[1]), 1))
        terms = ' '.join(f'{bb:+.3f}±{s:.3f}' for bb, s in zip(b[1:], se[1:]))
        print(f'  {name:24s} chi2 {chi2:7.1f} (dof {len(y) - D.shape[1]})   coefficients {terms}')


if __name__ == '__main__':
    main(*sys.argv[1:])
