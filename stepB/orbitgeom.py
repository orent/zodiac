"""Per-sector TESS orbit geometry relative to the Sun (JPL Horizons).

For each sector: TESS's geocentric position at 1-h steps over the sector,
the apogee direction (mean of the positions in the top 10% of distance),
the angle between apogee and the anti-Sun direction (ecliptic longitude
difference), the apogee's ecliptic latitude, and the mean geocentric
distance. The orbit precesses in its lunar resonance, so these do not
repeat with the calendar: an orbit-driven systematic separates from a
purely seasonal one.

usage: orbitgeom.py datadir out.csv [sectors]
"""

import csv, json, os, re, sys, urllib.parse, urllib.request
import numpy as np
from astropy.time import Time

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint
import timebase as tb


def vectors(body, center, start, stop, step='1h'):
    q = {'format': 'json', 'COMMAND': f"'{body}'", 'CENTER': f"'{center}'",
         'MAKE_EPHEM': "'YES'", 'EPHEM_TYPE': "'VECTORS'", 'VEC_TABLE': "'2'",
         'START_TIME': f"'{start}'", 'STOP_TIME': f"'{stop}'", 'STEP_SIZE': f"'{step}'",
         'CSV_FORMAT': "'NO'", 'TIME_TYPE': "'TDB'", 'OUT_UNITS': "'KM-S'", 'REF_PLANE': "'ECLIPTIC'"}
    text = json.load(urllib.request.urlopen(tb.HORIZONS + '?' + urllib.parse.urlencode(q), timeout=300))['result']
    body_ = text[text.index('$$SOE') + 5:text.index('$$EOE')]
    num = r'([-+]?\d*\.?\d+(?:[EeDd][-+]?\d+)?)'
    xyz = re.findall(rf'X\s*=\s*{num}\s*Y\s*=\s*{num}\s*Z\s*=\s*{num}', body_)
    return np.array([[float(v) for v in t] for t in xyz])


def main(datadir, out, sectors='1-63'):
    with open(out, 'w', newline='') as fo:
        w = csv.writer(fo)
        w.writerow(['sector', 'earth_lon_deg', 'apogee_minus_antisun_deg', 'apogee_lat_deg',
                    'mean_dist_re', 'max_dist_re'])
        for s, _ in joint.parse_sectors(sectors):
            p = os.path.join(datadir, f's{s:04d}_2min', 'cam1.npz')
            if not os.path.exists(p):
                continue
            c = np.load(p)
            t_sc = c['t_sc0'] + (c['cadenceno'] - c['cadenceno'][0]) * c['t_sc_step']
            t0 = Time(2457000 + t_sc[0], format='jd', scale='tdb').iso[:16]
            t1 = Time(2457000 + t_sc[-1], format='jd', scale='tdb').iso[:16]
            v = vectors('-95', '500@399', t0, t1)
            sun = vectors('10', '500@399', t0, t1, '1d')
            d = np.linalg.norm(v, axis=1)
            top = v[d >= np.percentile(d, 90)]
            ap = top.mean(0)
            ap_lon = np.degrees(np.arctan2(ap[1], ap[0]))
            ap_lat = np.degrees(np.arcsin(ap[2] / np.linalg.norm(ap)))
            s_mean = sun.mean(0)
            antisun = np.degrees(np.arctan2(-s_mean[1], -s_mean[0]))
            earth_lon = (antisun) % 360          # Earth's heliocentric longitude = anti-Sun direction
            dlon = (ap_lon - antisun + 180) % 360 - 180
            w.writerow([s, f'{earth_lon:.2f}', f'{dlon:.2f}', f'{ap_lat:.2f}',
                        f'{d.mean() / 6371:.2f}', f'{d.max() / 6371:.2f}'])
            fo.flush()
            print(s, f'{earth_lon:6.1f}', f'{dlon:+7.1f}', f'{ap_lat:+6.1f}', f'{d.mean() / 6371:6.1f}')


if __name__ == '__main__':
    main(*sys.argv[1:])
