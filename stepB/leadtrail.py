"""Leading/trailing test on the sky-cell product (cells.npz).

Each cell's series is split into thirds of the sector; each (cell, third)
gets its own sightline from the cell direction and the Sun's longitude at
the third's midpoint. Pieces with longitude offset from the anti-sun point
dlon > +MIN_DLON (toward Earth's apex: leading) or < -MIN_DLON (trailing)
form two groups. The model kernel (Kelsall, alpha 1.34, Hong phase) is
evaluated for each piece's geometry, so geometry differences are modelled;
an amplitude difference between the groups measures an east/west density
asymmetry (Earth's resonant ring / trailing blob). Earthshine (zero-delay,
proportional to the cell's level excess) is fitted per group.

usage: leadtrail.py datadir SECTORS [cameras] [min_dlon_deg]
"""

import os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint
import timebase as tb
from aligned import highpass

NBOOT = 200


def sun_lon_deg(jd_tdb):
    from astropy.coordinates import get_sun, GeocentricTrueEcliptic
    from astropy.time import Time
    t = Time(jd_tdb, format='jd', scale='tdb')
    return get_sun(t).transform_to(GeocentricTrueEcliptic(equinox=t)).lon.deg


def to_sun_frame(u_ecl, sun_lon):
    phi = np.radians(sun_lon + 180)
    ux = u_ecl[0] * np.cos(phi) + u_ecl[1] * np.sin(phi)
    uy = -u_ecl[0] * np.sin(phi) + u_ecl[1] * np.cos(phi)
    return np.array([ux, uy, u_ecl[2]])


def cell_pieces(datadir, sector, product, cam, min_dlon):
    """Yield (group, u, f, cadence, X, Y, eps) for each (cell, third)."""
    sdir = os.path.join(datadir, f's{int(sector):04d}{product}')
    cp = os.path.join(sdir, 'cells.npz')
    if not os.path.exists(cp):
        return
    C = np.load(cp)
    cols = list(C['cells_columns'])
    from aligned import load
    base = load(datadir, sector, cam, 'red', product=product)   # time base and VIRGO
    camf = np.load(os.path.join(sdir, f'cam{cam}.npz'))
    t_sc = camf['t_sc0'] + (camf['cadenceno'] - camf['cadenceno'][0]) * camf['t_sc_step']
    eph = tb.Ephem(os.path.join(sdir, 'ephem.npz'))
    t_tess = tb.tess_emission(t_sc, eph)
    cad = float(C['cadence'])
    win = int(round(joint.WINDOW_S / base['cadence']))
    f = np.fft.rfftfreq(win, base['cadence'])
    dec = int(C['level_decimate'])
    jd = t_sc + 2457000
    thirds = np.array_split(np.arange(len(jd)), 3)
    sun = [sun_lon_deg(np.median(jd[th])) for th in thirds]
    for row in C['cells']:
        r = dict(zip(cols, row))
        if int(r['cam']) != cam:
            continue
        name = f'cam{cam}_x{int(r["gx"])}y{int(r["gy"])}'
        frac = C[f'{name}_frac'].astype(float)
        lev = np.repeat(C[f'{name}_level'].astype(float), dec)
        lev = np.concatenate([lev, np.full(len(frac) - len(lev), lev[-1] if len(lev) else np.nan)])[:len(frac)]
        u_ecl = np.array([r['ux'], r['uy'], r['uz']])
        # Put the cell series on the same grid as the camera product.
        if base['cadence'] > 60:
            y = highpass(frac, int(round(3 * 3600 / cad)))
            lv = lev
        else:
            y = highpass(tb.bin_to_minutes(base['t'], t_tess, frac), 180)
            lv = tb.bin_to_minutes(base['t'], t_tess, lev)
        x = base['x']
        floor = np.nanpercentile(lv, 5)
        ynorm = y * lv / floor
        X, Y, L, S = joint.windows(x, ynorm, lv, win)
        if len(L) == 0:
            continue
        eps = (L - floor) / floor
        # Third of each window by its start index.
        third_idx = np.minimum((S / len(x) * 3).astype(int), 2)
        for k in range(3):
            m = third_idx == k
            if m.sum() < 3:
                continue
            u = to_sun_frame(u_ecl, sun[k])
            dlon = np.degrees(np.arctan2(u[1], u[0]))
            if abs(dlon) < min_dlon:
                continue
            group = 'lead' if dlon > 0 else 'trail'
            yield group, u, f, base['cadence'], X[m], Y[m], eps[m], dlon


def main(datadir, spec, cameras='1', min_dlon='3'):
    min_dlon = float(min_dlon)
    joint.DTS = np.array([0.0])
    rng = np.random.default_rng(13)
    for cam in (int(c) for c in cameras.split(',')):
        groups = {'lead': [], 'trail': []}
        dl = {'lead': [], 'trail': []}
        for sec, prod in joint.parse_sectors(spec):
            try:
                for g, u, f, cad, X, Y, eps, dlon in cell_pieces(datadir, sec, prod, cam, min_dlon):
                    groups[g].append((f, cad, X, Y, eps, u))
                    dl[g].append(dlon)
            except FileNotFoundError:
                continue
        res = {}
        for g, parts in groups.items():
            if not parts:
                continue
            t = joint.Terms(parts, joint.CAM_LAT[cam], alphas=[1.34])
            A, E = t.solve()[1][0, 0]
            bs = np.array([t.solve(joint.block_weights(len(t.eps), rng))[1][0, 0] for _ in range(NBOOT)])
            res[g] = (A, bs[:, 0].std(), E, bs[:, 1].std(), len(t.eps), np.median(dl[g]))
        print(f'camera {cam}, {spec}, |dlon| > {min_dlon:g} deg  (A, E per unit cell floor at alpha 1.34)')
        for g, (A, sA, E, sE, n, md) in res.items():
            print(f'  {g:5s}: A = {A:.3f} ± {sA:.3f}   E = {E:+.2f} ± {sE:.2f}   '
                  f'({n} windows, median dlon {md:+.1f} deg)')
        if len(res) == 2:
            d = res['trail'][0] - res['lead'][0]
            s = np.hypot(res['trail'][1], res['lead'][1])
            print(f'  trail - lead: {d:+.3f} ± {s:.3f}  ({d / s:+.1f} sigma); '
                  f'ratio trail/lead {res["trail"][0] / res["lead"][0]:.3f}')


if __name__ == '__main__':
    main(*sys.argv[1:])
