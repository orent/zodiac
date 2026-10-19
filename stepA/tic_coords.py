"""Look up TIC coordinates and Tmag for every 20-s target in the MAST
bulk-download scripts, then assign TESS camera/CCD with tess-point."""

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
            m = re.search(r'(tess\d+-s(\d{4})-(\d{16})-\d{4}-\w_fast-lc\.fits) (\S+)', line)
            if m:
                rows.append((int(m[2]), int(m[3]), m[1], m[4]))
    tics = sorted({r[1] for r in rows})
    coords = {}
    for i in range(0, len(tics), 500):
        coords.update(tic_query(tics[i:i + 500]))
        print(f'{len(coords)}/{len(tics)}', file=sys.stderr)
    with open(out, 'w') as f:
        f.write('sector,tic,ra,dec,tmag,camera,ccd,file,url\n')
        for sector, tic, fn, url in rows:
            ra, dec, tmag = coords[tic]
            res = stars2px(tic, ra, dec, trySector=sector)
            cam, ccd = (res[4][0], res[5][0]) if res[3][0] == sector else (0, 0)
            f.write(f'{sector},{tic},{ra},{dec},{tmag},{cam},{ccd},{fn},{url}\n')


if __name__ == '__main__':
    main(*sys.argv[1:])
