"""Does the seasonal term of the echo vary across the field of view?

The camera-level echo, normalised to the model brightness (Z/B), varies
with season (+10% at Earth longitude ~90 deg after eccentricity;
epochs_tilt.py). Per sky cell (6x6 deg, 4x4 per camera, cells.npz, 2-min
product, standard-pointing sectors 27-63): A at alpha 1.4 with E fixed
at the camera's pooled value (epochs.py tables), the cell's own floor and
model brightness B (tilted, Earth's true distance), q = A floor / 1.15 / B.
Per cell position (camera, gx, gy) the sectors are fitted with
r = q / mean = c0 + s sin(lam) + c cos(lam) (lam: Earth's heliocentric
longitude). An instrumental cause (stray light, optics) should make s
depend on the position in the field; a cause shared by the whole sky or
spacecraft should not.

usage: VIRGO_REF=red23 cellseason.py datadir epochs_cam1.csv ... out.csv
"""

import csv, os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['GEOM_TILT'] = '1'
import geometry
import joint
import timebase as tb
from aligned import highpass, load
import leadtrail as lt

SECTORS = list(range(27, 40)) + [47, 48, 49, 50, 51, 54, 55] + list(range(56, 64))
ECC, PERI_LON = 0.0167, 102.9
ALPHA = 1.4
NB = 20


def cell_rows(datadir, sector, cam, E, rng):
    product = '_2min'
    sdir = os.path.join(datadir, f's{int(sector):04d}{product}')
    C = np.load(os.path.join(sdir, 'cells.npz'))
    cols = list(C['cells_columns'])
    base = load(datadir, sector, cam, 'red', product=product)
    camf = np.load(os.path.join(sdir, f'cam{cam}.npz'))
    t_sc = camf['t_sc0'] + (camf['cadenceno'] - camf['cadenceno'][0]) * camf['t_sc_step']
    jd = np.median(t_sc) + 2457000
    sun = lt.sun_lon_deg(jd)
    lam = np.radians(sun + 180)
    r_e = (1 - ECC ** 2) / (1 + ECC * np.cos(lam - np.radians(PERI_LON)))
    win = int(round(joint.WINDOW_S / base['cadence']))
    f = np.fft.rfftfreq(win, base['cadence'])
    dec = int(C['level_decimate'])
    cad = float(C['cadence'])
    out = []
    for row in C['cells']:
        r = dict(zip(cols, row))
        if int(r['cam']) != cam:
            continue
        name = f'cam{cam}_x{int(r["gx"])}y{int(r["gy"])}'
        frac = C[f'{name}_frac'].astype(float)
        lev = np.repeat(C[f'{name}_level'].astype(float), dec)
        lev = np.concatenate([lev, np.full(len(frac) - len(lev), lev[-1] if len(lev) else np.nan)])[:len(frac)]
        y = highpass(frac, int(round(3 * 3600 / cad)))
        floor = np.nanpercentile(lev, 5)
        X, Y, L, S = joint.windows(base['x'], y * lev / floor, lev, win)
        if len(L) < 20:
            continue
        eps = (L - floor) / floor
        u = np.append(lt.to_sun_frame(np.array([r['ux'], r['uy'], r['uz']]), sun), lam)
        t = joint.Terms([(f, base['cadence'], X, Y, eps, u)], None, alphas=[ALPHA])

        def amp(wk):
            m11 = np.sum(wk * t.P[ALPHA])
            m12 = ((wk * t.eps) @ t.Q[ALPHA])[0]
            return ((wk @ t.U[ALPHA])[0] - E * m12) / m11

        n = len(t.eps)
        A = amp(np.ones(n))
        sA = np.std([amp(joint.block_weights(n, rng)) for _ in range(NB)])
        B = geometry.brightness(u, ALPHA, r_earth=r_e)
        k = floor / 1.15 / B
        out.append(dict(sector=sector, camera=cam, gx=int(r['gx']), gy=int(r['gy']),
                        xi=r['xi_deg'], eta=r['eta_deg'], n_stamps=int(r['n_stamps']),
                        q=A * k, sq=sA * k, floor=floor, earth_lon_deg=np.degrees(lam) % 360))
    return out


def main(datadir, *args):
    *tables, out = args
    joint.DTS = np.array([0.0])
    rng = np.random.default_rng(91)
    E = {}
    for p in tables:
        for r in csv.DictReader(open(p)):
            E[int(r['camera'])] = float(r['E_pool'])
    cams = sorted(E)
    with open(out, 'w', newline='') as fo:
        w = None
        for cam in cams:
            for s in SECTORS:
                try:
                    rows = cell_rows(datadir, s, cam, E[cam], rng)
                except FileNotFoundError:
                    continue
                for r in rows:
                    if w is None:
                        w = csv.DictWriter(fo, fieldnames=list(r))
                        w.writeheader()
                    w.writerow({k: (f'{v:.6g}' if isinstance(v, float) else v) for k, v in r.items()})
                fo.flush()
            print(f'camera {cam} done')


if __name__ == '__main__':
    main(*sys.argv[1:])
