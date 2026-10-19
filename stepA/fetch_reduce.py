"""Download TESS 20-s light curves, keep only the columns needed for
background reflectometry, and delete the original FITS file.

usage: fetch_reduce.py targets.csv sector camera outdir [nthreads]
"""

import csv, os, sys, tempfile, urllib.request
import multiprocessing.pool
import numpy as np
from astropy.io import fits

COLUMNS = ['TIME', 'TIMECORR', 'CADENCENO', 'SAP_FLUX', 'SAP_BKG', 'SAP_BKG_ERR',
           'QUALITY', 'POS_CORR1', 'POS_CORR2', 'MOM_CENTR1', 'MOM_CENTR2']
HEADER = ['TICID', 'SECTOR', 'CAMERA', 'CCD', 'TESSMAG', 'RA_OBJ', 'DEC_OBJ',
          'TSTART', 'TSTOP', 'DATA_REL', 'PROCVER']


def reduce(path, out):
    with fits.open(path, memmap=False) as f:
        hdr = {k: f[0].header.get(k) for k in HEADER}
        lc = f[1].data
        arrays = {c.lower(): np.asarray(lc[c]) for c in COLUMNS}
        mask = np.asarray(f[2].data)
        # Aperture image bits: 2 = optimal aperture, 4 = background pixel.
        hdr['NPIX_AP'] = int(((mask & 2) > 0).sum())
        hdr['NPIX_BKG'] = int(((mask & 4) > 0).sum())
        hdr['BKG_IN_AP'] = int(f[1].header.get('BACKAPP', True))
    # Write then rename, so an interrupted run never leaves a partial file
    # that a rerun would treat as cached.
    np.savez_compressed(out + '.part.npz', aperture=mask, header=np.array(repr(hdr)), **arrays)
    os.replace(out + '.part.npz', out)


def job(row, outdir):
    out = os.path.join(outdir, f"s{int(row['sector']):04d}_{int(row['tic'])}.npz")
    if os.path.exists(out):
        return out, 'cached'
    with tempfile.NamedTemporaryFile(suffix='.fits', dir=outdir) as tmp:
        for attempt in range(4):
            try:
                with urllib.request.urlopen(row['url'], timeout=300) as r:
                    tmp.write(r.read())
                break
            except OSError as e:
                if attempt == 3:
                    return out, f'failed: {e}'
        tmp.flush()
        reduce(tmp.name, out)
    return out, 'ok'


def main(targets, sector, camera, outdir, nthreads=8):
    os.makedirs(outdir, exist_ok=True)
    rows = [r for r in csv.DictReader(open(targets))
            if r['sector'] == sector and r['camera'] == camera]
    pool = multiprocessing.pool.ThreadPool(int(nthreads))
    for i, (out, status) in enumerate(pool.imap_unordered(lambda r: job(r, outdir), rows)):
        print(f'{i + 1}/{len(rows)} {os.path.basename(out)} {status}', flush=True)


if __name__ == '__main__':
    main(*sys.argv[1:])
