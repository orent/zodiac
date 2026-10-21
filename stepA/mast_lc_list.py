"""Write a bulk-download-style file list for TESS light curves of one sector
from the MAST CAOM API (a fallback for the archive.stsci.edu bulk scripts).
The output lines match what tic_coords.py parses.

usage: mast_lc_list.py sector cadence_s out.sh
"""

import json, sys, urllib.parse, urllib.request

MAST = 'https://mast.stsci.edu/api/v0/invoke'
DL = 'https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:TESS/product/'


def main(sector, cadence, out):
    sector, cadence = int(sector), float(cadence)
    suffix = '_fast-lc.fits' if cadence < 60 else '_lc.fits'
    rows, page = [], 1
    while True:
        req = {'service': 'Mast.Caom.Filtered', 'format': 'json',
               'params': {'columns': 'obs_id', 'filters': [
                   {'paramName': 'obs_collection', 'values': ['TESS']},
                   {'paramName': 'dataproduct_type', 'values': ['timeseries']},
                   {'paramName': 'sequence_number', 'values': [sector]},
                   {'paramName': 't_exptime', 'values': [{'min': cadence - 1, 'max': cadence + 1}]}]},
               'pagesize': 25000, 'page': page}
        data = urllib.parse.urlencode({'request': json.dumps(req)}).encode()
        res = json.load(urllib.request.urlopen(MAST, data, timeout=600))
        rows += [r['obs_id'] for r in res['data']]
        if page >= res['paging']['pagesFiltered']:
            break
        page += 1
    with open(out, 'w') as f:
        f.write('#!/bin/sh\n')
        for obs in rows:
            fn = obs + suffix
            f.write(f'curl -C - -L -o {fn} {DL}{fn}\n')
    print(f'sector {sector}: {len(rows)} files')


if __name__ == '__main__':
    main(*sys.argv[1:])
