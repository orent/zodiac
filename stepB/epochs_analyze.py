"""Analysis of the per-(sector, camera) table from epochs.py.

1. Normalised amplitude r = q / (camera weighted mean), standard pointing.
2. Is the per-sector deviation common to the cameras? Pairwise
   correlation of the normalised residuals, and the excess scatter of
   the per-sector camera average (a common factor) over its errors.
3. How much of the scatter is year-level? Year means and the chi^2 of
   year means vs the per-sector errors.
4. Which covariate tracks r? Weighted regressions of r on one covariate
   at a time (standardised), all cameras pooled.
5. Frequency dependence: r in three bands, by year.

usage: epochs_analyze.py cam1.csv [cam2.csv ...]
"""

import csv, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from sectors import STANDARD as STD
YEARS = [('Y1 S', range(1, 14)), ('Y2 N', range(17, 24)), ('Y3 S', range(27, 40)),
         ('Y4 N', (47, 48, 49, 50, 51, 54, 55)), ('Y5 N', range(56, 61)), ('Y5 S', range(61, 70)),
         ('Y6 N', (73, 80, 81, 82, 83, 84, 85, 86)), ('Y7 S', range(87, 91))]


def year_of(s):
    for name, rg in YEARS:
        if s in rg:
            return name
    return None


def wmean(x, s):
    w = 1 / s ** 2
    return np.sum(w * x) / np.sum(w), 1 / np.sqrt(np.sum(w))


def main(*paths):
    rows = [r for p in paths for r in csv.DictReader(open(p))]
    for r in rows:
        for k, v in r.items():
            r[k] = float(v)
    rows = [r for r in rows if int(r['sector']) in STD and r['sq'] > 0]
    cams = sorted({int(r['camera']) for r in rows})
    # 1. normalise per camera
    for c in cams:
        rc = [r for r in rows if r['camera'] == c]
        m, _ = wmean(np.array([r['q'] for r in rc]), np.array([r['sq'] for r in rc]))
        for r in rc:
            r['r'], r['sr'] = r['q'] / m, r['sq'] / m
            for b in ('lo', 'mid', 'hi'):
                r[f'r_{b}'], r[f'sr_{b}'] = r[f'q_{b}'] / m, r[f'sq_{b}'] / m
    by = {(int(r['sector']), int(r['camera'])): r for r in rows}
    secs = sorted({int(r['sector']) for r in rows})

    print('2. per-sector deviations: common to the cameras?')
    for i in cams:
        for j in cams:
            if j <= i:
                continue
            s = [x for x in secs if (x, i) in by and (x, j) in by]
            a = np.array([(by[(x, i)]['r'] - 1) / by[(x, i)]['sr'] for x in s])
            b = np.array([(by[(x, j)]['r'] - 1) / by[(x, j)]['sr'] for x in s])
            print(f'   cam {i}-{j}: correlation of normalised residuals {np.corrcoef(a, b)[0, 1]:+.2f} '
                  f'(n {len(s)}, 1/sqrt(n) = {1 / np.sqrt(len(s)):.2f})')
    avg, sav = [], []
    for x in secs:
        rs = [by[(x, c)] for c in cams if (x, c) in by]
        if len(rs) == len(cams):
            m, sm = wmean(np.array([r['r'] for r in rs]), np.array([r['sr'] for r in rs]))
            avg.append(m), sav.append(sm)
    avg, sav = np.array(avg), np.array(sav)
    chi = np.sum(((avg - wmean(avg, sav)[0]) / sav) ** 2) / (len(avg) - 1)
    ex = np.sqrt(max(np.var(avg) - np.mean(sav ** 2), 0))
    print(f'   camera-average per sector: chi2/dof {chi:.2f} over {len(avg)} sectors; '
          f'common-factor scatter ~{ex:.2f} (median error of the average {np.median(sav):.2f})')

    print('\n3. year means (r, weighted; per-sector bootstrap errors only)')
    print('   year   ' + ''.join(f'cam {c}'.rjust(14) for c in cams) + '   all cameras')
    for name, rg in YEARS:
        cells, allr, alls = [], [], []
        for c in cams:
            rc = [by[(s, c)] for s in rg if (s, c) in by]
            if not rc:
                cells.append(' ' * 14); continue
            m, sm = wmean(np.array([r['r'] for r in rc]), np.array([r['sr'] for r in rc]))
            cells.append(f'{m:.2f}±{sm:.2f}'.rjust(14))
            allr += [r['r'] for r in rc]; alls += [r['sr'] for r in rc]
        m, sm = wmean(np.array(allr), np.array(alls))
        print(f'   {name:5s} ' + ''.join(cells) + f'   {m:.2f}±{sm:.2f}')
    for c in cams:
        ms, ss, ok = [], [], []
        for name, rg in YEARS:
            rc = [by[(s, c)] for s in rg if (s, c) in by]
            if rc:
                m, sm = wmean(np.array([r['r'] for r in rc]), np.array([r['sr'] for r in rc]))
                ms.append(m), ss.append(sm)
        ms, ss = np.array(ms), np.array(ss)
        chi = np.sum(((ms - wmean(ms, ss)[0]) / ss) ** 2)
        print(f'   cam {c}: chi2 of year means {chi:.1f} for {len(ms) - 1} dof')

    print('\n4. single-covariate weighted regressions of r (all cameras; slope per 1 sd of the covariate)')
    def cov(r, name):
        if name == 'floor/median':
            med = np.median([x['floor'] for x in rows if x['camera'] == r['camera']])
            return r['floor'] / med
        if name == 'VIRGO lo/hi power':
            return np.log(r['x_lo'] / r['x_hi'])
        if name == 'VIRGO mid power':
            return np.log(r['x_mid'])
        if name == 'TESS lo-band noise / camera median':
            med = np.median([x['y_lo'] for x in rows if x['camera'] == r['camera']])
            return np.log(r['y_lo'] / med)
        if name == 'TESS mid-band noise / camera median':
            med = np.median([x['y_mid'] for x in rows if x['camera'] == r['camera']])
            return np.log(r['y_mid'] / med)
        if name == 'sin earth_lon':
            return np.sin(np.radians(r['earth_lon_deg']))
        if name == 'cos earth_lon':
            return np.cos(np.radians(r['earth_lon_deg']))
        if name == 'north (1) / south (0)':
            return float(r['ecl_lat_deg'] > 0)
        if name == 'E_free - E_pool':
            return r['E_free'] - r['E_pool']
        if name == 'sector number':
            return r['sector']
        return r[name]
    names = ['floor/median', 'eps_mean', 'eps_p90', 'E_free - E_pool', 'VIRGO lo/hi power',
             'VIRGO mid power', 'TESS lo-band noise / camera median', 'TESS mid-band noise / camera median',
             'n_win', 'n_stamps', 'sin earth_lon', 'cos earth_lon', 'north (1) / south (0)', 'sector number']
    y = np.array([r['r'] for r in rows]); sy = np.array([r['sr'] for r in rows])
    w = 1 / sy ** 2
    for name in names:
        x = np.array([cov(r, name) for r in rows])
        ok = np.isfinite(x)
        xs = (x[ok] - np.mean(x[ok])) / (np.std(x[ok]) or 1)
        D = np.stack([np.ones(ok.sum()), xs], 1)
        M = D.T @ (w[ok, None] * D)
        beta = np.linalg.solve(M, D.T @ (w[ok] * y[ok]))
        res = y[ok] - D @ beta
        chi = np.sum(w[ok] * res ** 2) / (ok.sum() - 2)
        se = np.sqrt(np.diag(np.linalg.inv(M)) * max(chi, 1))
        print(f'   {name:38s} slope {beta[1]:+.3f} ± {se[1]:.3f}  ({beta[1] / se[1]:+.1f} sigma)')

    print('\n5. frequency bands: r_band by year (all cameras, weighted)')
    print('   year    0.1-1 mHz      1-3 mHz        3-8.3 mHz')
    for name, rg in YEARS:
        cells = []
        for b in ('lo', 'mid', 'hi'):
            rc = [r for r in rows if int(r['sector']) in rg and r[f'sr_{b}'] > 0]
            m, sm = wmean(np.array([r[f'r_{b}'] for r in rc]), np.array([r[f'sr_{b}'] for r in rc]))
            cells.append(f'{m:5.2f}±{sm:.2f}')
        print(f'   {name:6s} ' + '   '.join(cells))


if __name__ == '__main__':
    main(*sys.argv[1:])
