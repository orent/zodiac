"""Common time base: every sample is referred to the time its light left
the Sun (TDB), so the 499 s Sun-to-1 AU travel time, the ~5 s SOHO (L1)
vs TESS offset and TESS's orbital ±1 s all drop out. What remains between
VIRGO and TESS is only the extra path through the dust.

Conventions (see TIMEBASE.md):
- TESS: TIME is mid-cadence BTJD (TDB); TIME - TIMECORR is the spacecraft
  time in TDB, JD - 2457000.
- VIRGO SPM red (Montillet F60d product): days since 1996-01-23 in UTC,
  stamp = mid point of the 1-min sample.
- VIRGO PMO6 TSI minute (V8): TAI seconds since 1958-01-01, stamp = mid point.
- Light time: t_emit = t_obs - r_helio(t_obs) / c, with r from JPL Horizons
  (TESS = -95, SOHO = -21, centre = Sun body centre).

usage: timebase.py ephem datadir sector [suffix]   (fetch and store Horizons ranges)
"""

import json, os, sys, urllib.parse, urllib.request
import numpy as np
from astropy.time import Time

C_AU_PER_DAY = 173.1446326742403
HORIZONS = 'https://ssd.jpl.nasa.gov/api/horizons.api'
RED_EPOCH = Time('1996-01-23T00:00:00', scale='utc')
TAI_EPOCH = Time('1958-01-01T00:00:00', scale='tai')
# Stamp position within the sample, as a fraction of the sample length
# (0 = start, 0.5 = mid). VIRGO files give the mid point (Appourchaux et
# al. 2018, A&A 617, A108, Table A.1).
VIRGO_STAMP = 0.5


def horizons_range(body, start, stop, step='10m'):
    """Heliocentric distance [AU] of a spacecraft vs TDB JD."""
    q = {'format': 'json', 'COMMAND': f"'{body}'", 'CENTER': "'500@10'",
         'MAKE_EPHEM': "'YES'", 'EPHEM_TYPE': "'VECTORS'", 'VEC_TABLE': "'2'",
         'START_TIME': f"'{start}'", 'STOP_TIME': f"'{stop}'", 'STEP_SIZE': f"'{step}'",
         'CSV_FORMAT': "'YES'", 'TIME_TYPE': "'TDB'", 'OUT_UNITS': "'AU-D'",
         'REF_PLANE': "'ECLIPTIC'"}
    url = HORIZONS + '?' + urllib.parse.urlencode(q)
    text = json.load(urllib.request.urlopen(url, timeout=300))['result']
    body_ = text[text.index('$$SOE') + 5:text.index('$$EOE')]
    jd, r = [], []
    for line in body_.strip().splitlines():
        f = [x.strip() for x in line.split(',')]
        jd.append(float(f[0]))
        r.append(np.sqrt(sum(float(v) ** 2 for v in f[2:5])))
    return np.array(jd), np.array(r)


def fetch_ephem(datadir, sector, t_sc, suffix=''):
    """Store TESS and SOHO heliocentric ranges covering a sector."""
    jd0 = 2457000 + np.nanmin(t_sc) - 1
    jd1 = 2457000 + np.nanmax(t_sc) + 1
    start = Time(jd0, format='jd', scale='tdb').iso[:16]
    stop = Time(jd1, format='jd', scale='tdb').iso[:16]
    out = {}
    for name, body in (('tess', '-95'), ('soho', '-21')):
        jd, r = horizons_range(body, start, stop)
        out[f'{name}_jd_tdb'], out[f'{name}_r_au'] = jd, r
    path = os.path.join(datadir, f's{int(sector):04d}{suffix}', 'ephem.npz')
    np.savez_compressed(path, **out)
    return path


class Ephem:
    def __init__(self, path):
        e = np.load(path)
        self.e = {k: e[k] for k in e.files}

    def light_time_days(self, name, jd_tdb):
        r = np.interp(jd_tdb, self.e[f'{name}_jd_tdb'], self.e[f'{name}_r_au'])
        return r / C_AU_PER_DAY


def tess_emission(t_sc, eph):
    """TESS spacecraft TDB (JD - 2457000) -> emission time at the Sun,
    in seconds TDB since J2000 (a convenient float64 scale)."""
    jd = t_sc + 2457000
    jd_emit = jd - eph.light_time_days('tess', jd)
    return (jd_emit - 2451545.0) * 86400


def virgo_red_emission(days, eph, stamp=VIRGO_STAMP):
    """SPM red stamps -> emission time of the sample mid point (s TDB since J2000)."""
    t = (RED_EPOCH + days).tdb.jd + (0.5 - stamp) * 60 / 86400
    return (t - eph.light_time_days('soho', t) - 2451545.0) * 86400


def virgo_tsi_emission(tai_s, eph, stamp=VIRGO_STAMP):
    t = Time(TAI_EPOCH.jd1, TAI_EPOCH.jd2 + tai_s / 86400, format='jd', scale='tai').tdb.jd
    t = t + (0.5 - stamp) * 60 / 86400
    return (t - eph.light_time_days('soho', t) - 2451545.0) * 86400


def bin_to_minutes(t_min, t_s, x, sample=20.0, width=60.0, min_cover=2 / 3):
    """Overlap-weighted average of `sample`-s box samples (centres t_s)
    into `width`-s boxes centred on t_min. Each output is then a box of
    exactly `width` s centred on its stamp, independent of the relative
    phase of the two grids. Outputs with less than min_cover of the box
    covered by valid samples are NaN."""
    ok = np.isfinite(x)
    t_s, x = t_s[ok], x[ok]
    order = np.argsort(t_min)
    tm = t_min[order]
    acc = np.zeros(len(tm))
    cov = np.zeros(len(tm))
    # A sample can overlap at most ceil(sample/width)+1 output boxes.
    i0 = np.searchsorted(tm, t_s - (width + sample) / 2) - 1
    for k in range(int(np.ceil(sample / width)) + 3):
        j = i0 + k
        valid = (j >= 0) & (j < len(tm))
        jj = np.clip(j, 0, len(tm) - 1)
        lo = np.maximum(t_s - sample / 2, tm[jj] - width / 2)
        hi = np.minimum(t_s + sample / 2, tm[jj] + width / 2)
        w = np.where(valid, np.clip(hi - lo, 0, None), 0)
        np.add.at(acc, jj, w * x)
        np.add.at(cov, jj, w)
    with np.errstate(invalid='ignore'):
        out = acc / cov
    out[cov < min_cover * width] = np.nan
    res = np.empty_like(out)
    res[order] = out
    return res


if __name__ == '__main__':
    if sys.argv[1] == 'ephem':
        datadir, sector = sys.argv[2:4]
        suffix = sys.argv[4] if len(sys.argv) > 4 else ''
        c = np.load(os.path.join(datadir, f's{int(sector):04d}{suffix}', 'cam1.npz'))
        t_sc = c['t_sc'] if 't_sc' in c.files else \
            c['t_sc0'] + (c['cadenceno'] - c['cadenceno'][0]) * c['t_sc_step']
        print(fetch_ephem(datadir, sector, t_sc, suffix))
