"""Line-of-sight geometry per (sector, camera) and the general dust kernel.

TESS pointings are not all the standard anti-sunward 18/42/66/90 deg
strip: in year 2, sectors 14-16 and 24-26 were pointed further north
(camera 1 at ~+49 deg, cameras 3-4 past the pole, toward the Sun's side).
The sightline of each (sector, camera) is taken as the mean direction of
its targets, expressed in a Sun-centred frame at mid-sector:
  x: from the Sun through the Earth (anti-sunward), z: ecliptic north.

Kernel: Earth at (1, 0, 0) AU, dust at e + s*u. Weight per ds:
  n(r, Z) * Phi(scattering angle) / r^2,
n = r^-alpha x Kelsall fan with |Z/R| scaled by 1/v, Phi = Hong (1985)
three-term Henyey-Greenstein fit to the visible zodiacal light.
Delay tau = (r + s - 1) * 499.005 s.

usage: geometry.py table out.csv datadir targets.csv [targets.csv ...]
"""

import csv, glob, os, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 2)[0])
import sensitivity as sm

# Hong 1985, A&A 146, 67: weights and asymmetry parameters.
HONG = [(0.665, 0.70), (0.330, -0.20), (0.005, -0.81)]


def phase_hong(cos_theta):
    out = 0
    for w, g in HONG:
        out = out + w * (1 - g * g) / (1 + g * g - 2 * g * cos_theta) ** 1.5
    return out


def kernel(u, alpha=1.34, v=1.0, smax=6.0, ns=20000):
    """tau [s] and normalised weights along the sightline u (unit vector,
    Sun-centred frame described above)."""
    u = np.asarray(u, float)
    u = u / np.linalg.norm(u)
    s = np.linspace(0, smax, ns + 1)[1:] - smax / ns / 2
    p = np.array([1.0, 0.0, 0.0])[None, :] + s[:, None] * u[None, :]
    r = np.linalg.norm(p, axis=1)
    tau = (r + s - 1) * sm.AU_LIGHT_S
    zeta = np.abs(p[:, 2] / r) / v
    mu = 0.189
    g = np.where(zeta < mu, zeta ** 2 / (2 * mu), zeta - mu / 2)
    dens = r ** -alpha * np.exp(-4.14 * g ** 0.942)
    # Scattering angle: incoming along p/r (from the Sun), outgoing along -u.
    cos_theta = -(p @ u) / r
    w = dens * phase_hong(cos_theta) / r ** 2
    return tau, w / w.sum()


def transfer(f, u, alpha=1.34, v=1.0):
    tau, w = kernel(u, alpha, v)
    h = np.zeros(len(f), complex)
    nz = f > 0
    h[nz] = sm.transfer(f[nz], tau, w)
    return h


def load_table(path):
    out = {}
    for r in csv.DictReader(open(path)):
        out[(int(r['sector']), int(r['camera']))] = np.array(
            [float(r['ux']), float(r['uy']), float(r['uz'])])
    return out


def table(out, datadir, *target_csvs):
    """Mean target direction per (sector, camera), rotated into the
    Sun-centred frame using the Sun's ecliptic longitude at mid-sector."""
    from astropy.coordinates import SkyCoord, GeocentricTrueEcliptic, get_sun
    from astropy.time import Time
    import astropy.units as un
    groups = {}
    for path in target_csvs:
        for r in csv.DictReader(open(path)):
            groups.setdefault((int(r['sector']), int(r['camera'])), []).append(
                (float(r['ra']), float(r['dec'])))
    with open(out, 'w', newline='') as fo:
        wr = csv.writer(fo)
        wr.writerow(['sector', 'camera', 'ux', 'uy', 'uz', 'elongation_deg', 'ecl_lat_deg',
                     'dlon_from_antisun_deg', 'n_targets'])
        for (sec, cam), radec in sorted(groups.items()):
            if cam == 0:
                continue
            files = glob.glob(os.path.join(datadir, f's{sec:04d}*', 'cam1.npz'))
            if not files:
                continue
            c = np.load(files[0])
            t_sc = c['t_sc'] if 't_sc' in c.files else \
                c['t_sc0'] + (c['cadenceno'] - c['cadenceno'][0]) * c['t_sc_step']
            tmid = Time(2457000 + np.nanmedian(t_sc), format='jd', scale='tdb')
            sun_lon = get_sun(tmid).transform_to(GeocentricTrueEcliptic(equinox=tmid)).lon.rad
            a = np.array(radec)
            e = SkyCoord(ra=a[:, 0] * un.deg, dec=a[:, 1] * un.deg).transform_to(
                GeocentricTrueEcliptic(equinox=tmid))
            lon, lat = e.lon.rad, e.lat.rad
            vec = np.stack([np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)], 1)
            m = vec.mean(0)
            m /= np.linalg.norm(m)
            # Rotate so that the anti-sun direction (sun_lon + pi) is +x.
            phi = sun_lon + np.pi
            ux = m[0] * np.cos(phi) + m[1] * np.sin(phi)
            uy = -m[0] * np.sin(phi) + m[1] * np.cos(phi)
            uz = m[2]
            elong = np.degrees(np.arccos(-ux))
            wr.writerow([sec, cam, f'{ux:.5f}', f'{uy:.5f}', f'{uz:.5f}', f'{elong:.1f}',
                         f'{np.degrees(np.arcsin(uz)):.1f}',
                         f'{np.degrees(np.arctan2(uy, ux)):.1f}', len(a)])


if __name__ == '__main__':
    if sys.argv[1] == 'table':
        table(*sys.argv[2:])
