"""Look up TIC coordinates and Tmag for every target in MAST bulk-download
scripts (20-s *_fast-lc or 2-min *_lc), then assign TESS camera/CCD with
tess-point."""

import json, re, sys, urllib.parse, urllib.request
import numpy as np
from tess_stars2px import tess_stars2px_function_entry as stars2px

MAST = 'https://mast.stsci.edu/api/v0/invoke'


def tic_query(ids):
    req = {'service': 'Mast.Catalogs.Filtered.Tic', 'format': 'json',
           'params': {'columns': 'ID,ra,dec,Tmag',
                      'filters': [{'paramName': 'ID', 'values': [str(i) for i in ids]}]},
           'pagesize': 5000}
    data = urllib.parse.urlencode({'request': json.dumps(req)}).encode()
    out = json.load(urllib.request.urlopen(MAST, data, timeout=300))
    return {int(r['ID']): (r['ra'], r['dec'], r['Tmag']) for r in out['data']}


def main(out, *scripts):
    rows = []
    for path in scripts:
        for line in open(path):
            m = re.search(r'(tess\d+-s(\d{4})-(\d{16})-\d{4}-\w_(?:fast-)?lc\.fits) (\S+)', line)
            if m:
                rows.append((int(m[2]), int(m[3]), m[1], m[4]))
    tics = sorted({r[1] for r in rows})
    coords = {}
    for i in range(0, len(tics), 500):
        coords.update(tic_query(tics[i:i + 500]))
        print(f'{len(coords)}/{len(tics)}', file=sys.stderr)
    by_sector = {}
    for row in rows:
        by_sector.setdefault(row[0], []).append(row)
    with open(out, 'w') as f:
        f.write('sector,tic,ra,dec,tmag,camera,ccd,file,url\n')
        for sector, srows in sorted(by_sector.items()):
            tic = np.array([r[1] for r in srows])
            ra = np.array([coords[t][0] for t in tic])
            dec = np.array([coords[t][1] for t in tic])
            res = stars2px(tic, ra, dec, trySector=sector)
            # One output row per (target, sector, camera) hit; keep the hit
            # for this sector (a target is on at most one camera per sector).
            cam = {}
            for i, sec, c, ccd in zip(res[0], res[3], res[4], res[5]):
                if sec == sector:
                    cam[int(i)] = (int(c), int(ccd))
            for (_, t, fn, url), r_, d_ in zip(srows, ra, dec):
                c, ccd = cam.get(int(t), (0, 0))
                f.write(f'{sector},{t},{r_},{d_},{coords[t][2]},{c},{ccd},{fn},{url}\n')


if __name__ == '__main__':
    main(*sys.argv[1:])
