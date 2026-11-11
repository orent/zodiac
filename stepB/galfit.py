"""Seasonal term vs the sky each camera field sweeps (and vs free E).

For each (sector, camera) row of the epochs.py table: the field centre in
ecliptic coordinates (Earth longitude + offset from anti-Sun, and the
ecliptic latitude, from geometry.csv), its galactic latitude b, and the
angle to the LMC. Fits, per camera, r on sin/cos lam alone and with
|b| (or exp(-|b|/10 deg)) added: if a crowding effect makes the
seasonal term, the season coefficients should drop when |b| is in.
Also refits with q from A with E free (A_free) instead of E fixed.

usage: galfit.py datadir epochs_cam1.csv ...
"""

import csv, os, sys
import numpy as np
from astropy.coordinates import SkyCoord, GeocentricTrueEcliptic
import astropy.units as un

sys.path.insert(0, __file__.rsplit('/', 1)[0])
os.environ['ECCENTRIC'] = '1'
import epochs_tilt as et
import geometry

LMC = SkyCoord(ra=80.89 * un.deg, dec=-69.76 * un.deg)


def wls(D, y, sy):
    w = 1 / sy ** 2
    M = D.T @ (w[:, None] * D)
    b = np.linalg.solve(M, D.T @ (w * y))
    chi2 = np.sum(w * (y - D @ b) ** 2)
    se = np.sqrt(np.diag(np.linalg.inv(M)) * max(chi2 / (len(y) - D.shape[1]), 1))
    return b, se, chi2


def main(datadir, *tables):
    rows = et.load(datadir, tables)
    geo = {(int(r['sector']), int(r['camera'])): r for r in csv.DictReader(open(os.path.join(datadir, 'geometry.csv')))}
    lon = np.array([(r['earth_lon_deg'] + float(geo[(int(r['sector']), int(r['camera']))]['dlon_from_antisun_deg'])) % 360
                    for r in rows])
    lat = np.array([float(geo[(int(r['sector']), int(r['camera']))]['ecl_lat_deg']) for r in rows])
    c = SkyCoord(lon=lon * un.deg, lat=lat * un.deg, frame=GeocentricTrueEcliptic(equinox='J2000'))
    b = c.galactic.b.deg
    dl = c.icrs.separation(LMC).deg
    lam = np.radians([r['earth_lon_deg'] for r in rows])
    cams = np.array([int(r['camera']) for r in rows])
    y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
    print('field galactic latitude |b| by camera (median, min, max) and the sin(lam) correlation:')
    for k in (1, 2, 3, 4):
        m = cams == k
        print(f'  cam {k}: |b| median {np.median(abs(b[m])):5.1f}, range {abs(b[m]).min():5.1f}-{abs(b[m]).max():5.1f};'
              f' corr(sin lam, exp(-|b|/10)) {np.corrcoef(np.sin(lam[m]), np.exp(-abs(b[m]) / 10))[0, 1]:+.2f};'
              f' LMC within 12 deg in {np.sum(m & (dl < 12))} sectors')
    print('\nper camera: season-only vs season + crowding (exp(-|b|/10 deg)) [+ LMC flag for cam 4]')
    for k in (1, 2, 3, 4):
        m = cams == k
        ones = np.ones(m.sum())
        D0 = np.stack([ones, np.sin(lam[m]), np.cos(lam[m])], 1)
        b0, s0, c0 = wls(D0, y[m], sy[m])
        crowd = np.exp(-abs(b[m]) / 10)
        cols = [ones, np.sin(lam[m]), np.cos(lam[m]), (crowd - crowd.mean()) / (crowd.std() or 1)]
        if k == 4:
            cols.append((dl[m] < 12).astype(float))
        D1 = np.stack(cols, 1)
        b1, s1, c1 = wls(D1, y[m], sy[m])
        extra = ' '.join(f'{v:+.3f}±{e:.3f}' for v, e in zip(b1[3:], s1[3:]))
        print(f'  cam {k}: season only  s {b0[1]:+.3f}±{s0[1]:.3f} c {b0[2]:+.3f}±{s0[2]:.3f} (chi2 {c0:.1f})')
        print(f'         + crowding    s {b1[1]:+.3f}±{s1[1]:.3f} c {b1[2]:+.3f}±{s1[2]:.3f} (chi2 {c1:.1f}); '
              f'crowding{"/LMC" if k == 4 else ""} {extra}')
    print('\nsame season fit with E free per sector (q from A_free):')
    for k in (1, 2, 3, 4):
        m = cams == k
        rr = [r for r, mm in zip(rows, m) if mm]
        qf = np.array([r['A_free'] / (r['q'] / r['r']) * 0 + r['A_free'] for r in rr])
        # A_free -> q_free with the same factor as q / A (k = floor / 1.15 / B).
        fac = np.array([r['q'] / r['r'] for r in rr])           # camera mean of q
        kfac = []
        for r in rr:
            u = None
        # q = A_fixed * k; we do not store A_fixed, so use the ratio A_free / A_fixed via q_lo etc. is not
        # available: reconstruct k from floor and B.
        import joint
        for r in rr:
            u = joint.sightline(datadir, int(r['sector']), int(r['camera']))
            lam_r = np.radians(r['earth_lon_deg'])
            rE = (1 - et.ECC ** 2) / (1 + et.ECC * np.cos(lam_r - np.radians(et.PERI_LON)))
            kfac.append(r['floor'] / 1.15 / geometry.brightness(u, 1.4, r_earth=rE))
        qf = np.array([r['A_free'] for r in rr]) * np.array(kfac)
        yf = qf / np.average(qf, weights=1 / sy[m] ** 2)
        D0 = np.stack([np.ones(m.sum()), np.sin(lam[m]), np.cos(lam[m])], 1)
        bf, sf, cf = wls(D0, yf, sy[m] * 1.3)
        print(f'  cam {k}: E free  s {bf[1]:+.3f}±{sf[1]:.3f} c {bf[2]:+.3f}±{sf[2]:.3f}   '
              f'(E fixed: see above; errors inflated 1.3x for the freer fit)')


if __name__ == '__main__':
    main(*sys.argv[1:])
