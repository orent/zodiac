"""Per-(sector, camera) diagnostics for the epoch-level systematic.

Year-long groups of sectors differ in echo amplitude by more than the
per-sector scatter allows (sectorscatter.py). This writes one row per
(sector, camera), 2-min product throughout (uniform across all years),
reference red23, tilted geometry, alpha 1.4, with candidate drivers:

  q, sq       Z/B = A * floor / 1.15 / B, E fixed at the camera's pooled
              value (bootstrap sigma)
  A, E free   A and E fitted together
  q_lo/mid/hi q from the bands 0.1-1, 1-3, 3-8.3 mHz only: if the true
              kernel differs from the model, A depends on the frequency
              weighting, which changes with the solar and noise spectra
  floor, eps_mean, eps_p90   stray-light state
  x_lo/mid/hi mean VIRGO power per band (probe spectrum)
  y_lo/mid/hi mean TESS power per band (noise)
  n_win, n_stamps, sun_lon, earth_lon

usage: VIRGO_REF=red23 epochs.py datadir out.csv [cameras] [sectors]
"""

import csv, os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['GEOM_TILT'] = '1'
import geometry
import joint
import northsouth as ns

BANDS = {'lo': (0.1e-3, 1e-3), 'mid': (1e-3, 3e-3), 'hi': (3e-3, 8.3e-3)}
NB = 20


def amp_fixedE(t, E, wk):
    m11 = np.sum(wk * t.P[ns.ALPHA])
    m12 = ((wk * t.eps) @ t.Q[ns.ALPHA])[0]
    r1 = (wk @ t.U[ns.ALPHA])[0]
    return (r1 - E * m12) / m11


def terms(ch, lo=None, hi=None):
    flo, fhi = joint.F_LO, joint.F_HI
    if lo is not None:
        joint.F_LO, joint.F_HI = lo, hi
    try:
        return joint.Terms([(ch.f, ch.cadence, ch.X, ch.Y, ch.eps, ch.u)], None, alphas=[ns.ALPHA])
    finally:
        joint.F_LO, joint.F_HI = flo, fhi


def main(datadir, out, cameras='1,2,3,4', sectors='1-63'):
    joint.DTS = np.array([0.0])
    rng = np.random.default_rng(73)
    fields = ['sector', 'camera', 'q', 'sq', 'A_free', 'E_free', 'q_lo', 'sq_lo', 'q_mid', 'sq_mid',
              'q_hi', 'sq_hi', 'floor', 'eps_mean', 'eps_p90', 'x_lo', 'x_mid', 'x_hi',
              'y_lo', 'y_mid', 'y_hi', 'n_win', 'n_stamps', 'earth_lon_deg', 'ecl_lat_deg', 'E_pool']
    with open(out, 'w', newline='') as fo:
        w = csv.DictWriter(fo, fieldnames=fields)
        w.writeheader()
        for cam in (int(c) for c in cameras.split(',')):
            chunks = {}
            for s, _ in joint.parse_sectors(sectors):
                try:
                    chunks[s] = joint.Chunk(datadir, s, '_2min', cam, 'red')
                except FileNotFoundError:
                    continue
            std = [chunks[s] for s in chunks if s in ns_std()]
            tp = joint.Terms([(c.f, c.cadence, c.X, c.Y, c.eps, c.u) for c in std], None, alphas=[ns.ALPHA])
            E = tp.solve()[1][0, 0, 1]
            for s, ch in sorted(chunks.items()):
                B = geometry.brightness(ch.u, ns.ALPHA)
                k = ch.floor / 1.15 / B
                t = terms(ch)
                n = len(t.eps)
                A = amp_fixedE(t, E, np.ones(n))
                sA = np.std([amp_fixedE(t, E, joint.block_weights(n, rng)) for _ in range(NB)])
                Af, Ef = t.solve()[1][0, 0]
                row = dict(sector=s, camera=cam, q=A * k, sq=sA * k, A_free=Af, E_free=Ef,
                           floor=ch.floor, eps_mean=np.mean(ch.eps), eps_p90=np.percentile(ch.eps, 90),
                           n_win=n, E_pool=E,
                           earth_lon_deg=np.degrees(ch.u[3]) % 360 if len(ch.u) == 4 else np.nan,
                           ecl_lat_deg=np.degrees(np.arcsin(ch.u[2] / np.linalg.norm(ch.u[:3]))))
                for b, (lo, hi) in BANDS.items():
                    tb = terms(ch, lo, hi)
                    Ab = amp_fixedE(tb, E, np.ones(n))
                    sb = np.std([amp_fixedE(tb, E, joint.block_weights(n, rng)) for _ in range(NB)])
                    row[f'q_{b}'], row[f'sq_{b}'] = Ab * k, sb * k
                    m = (ch.f >= lo) & (ch.f < hi)
                    row[f'x_{b}'] = np.mean(np.abs(ch.X[:, m]) ** 2)
                    row[f'y_{b}'] = np.mean(np.abs(ch.Y[:, m]) ** 2)
                c = np.load(os.path.join(datadir, f's{s:04d}_2min', f'cam{cam}.npz'))
                row['n_stamps'] = float(np.median(c['n_stamps']))
                w.writerow({kk: (f'{v:.6g}' if isinstance(v, float) else v) for kk, v in row.items()})
                fo.flush()
            print(f'camera {cam}: {len(chunks)} sectors, E pool {E:+.2f}')


def ns_std():
    from sectors import STANDARD
    return STANDARD


if __name__ == '__main__':
    main(*sys.argv[1:])
