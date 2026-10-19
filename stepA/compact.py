"""Write the compact per-sector product (a few MB) from the reduced stamp
files, and the TESS-era cut of the VIRGO series. Everything downstream
(timing, spectra, deconvolution, Earthshine, nulls) runs from this layer.

usage:
  compact.py sector reduced_dir noise.npz outdir
  compact.py virgo red.fits tsi.fits outdir
"""

import ast, csv, glob, os, re, sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 2)[0] + '/stepB')
sys.path.insert(0, __file__.rsplit('/', 1)[0])
import noise as nz
from quicklook import cadence_clock

# VIRGO cut: TESS start (2018-07-25) minus a margin for null shifts.
RED_EPOCH_JD = 2450105.5          # 1996-01-23T00:00 UTC
TESS_ERA_START_JD = 2458300.5     # 2018-07-01


def sector(sec, reduced, noise_npz, outdir):
    sec = int(sec)
    # Product directory: sNNNN for 20-s data, sNNNN<suffix> otherwise (e.g. _2min).
    out = os.path.join(outdir, f's{sec:04d}' + os.environ.get('TESS_PRODUCT_SUFFIX', ''))
    os.makedirs(out, exist_ok=True)
    clock = cadence_clock(reduced)
    d = np.load(noise_npz)
    rows = ast.literal_eval(re.sub(r'np\.\w+\(([^)]*)\)', r'\1', str(d['rows'])))
    c0 = min(np.load(p)['cadenceno'].min() for p in glob.glob(f'{reduced}/*.npz'))

    stamps = []
    for p in sorted(glob.glob(f'{reduced}/*.npz')):
        if p.endswith('.part.npz'):
            continue
        h, s = nz.load(p)
        b = s['sap_bkg'] / h['NPIX_AP']
        good = ((s['quality'] & nz.BADBITS) == 0) & np.isfinite(b)
        stamps.append((h['CAMERA'], h['CCD'], s['cadenceno'], np.where(good, b, np.nan)))

    def write(name, frac, members):
        ncad = len(frac)
        cad = c0 + np.arange(ncad)
        stack = np.full((len(members), ncad), np.nan)
        for i, (_, _, cn, b) in enumerate(members):
            stack[i, cn - c0] = b
        with np.errstate(all='ignore'):
            level = np.nanmedian(stack, 0)
        np.savez_compressed(
            os.path.join(out, f'{name}.npz'),
            cadenceno=cad.astype(np.int32),
            t_sc=clock(cad),                        # spacecraft TDB, JD - 2457000
            frac=frac.astype(np.float32),
            level=level.astype(np.float32),
            n_stamps=np.isfinite(stack).sum(0).astype(np.int16),
            cadence=np.float32(nz.CADENCE))
        print(f'{name}: {ncad} cadences, {len(members)} stamps')

    for cam in sorted({r['cam'] for r in rows}):
        write(f'cam{cam}', d[f'cam{cam}_series'], [s for s in stamps if s[0] == cam])
        # Per-CCD lines of sight, when noise.py produced them.
        for ccd in (1, 2, 3, 4):
            key = f'cam{cam}_ccd{ccd}_series'
            if key in d.files:
                write(f'cam{cam}_ccd{ccd}', d[key], [s for s in stamps if s[0] == cam and s[1] == ccd])

    keys = ['tic', 'cam', 'ccd', 'tmag', 'nbkg', 'level', 'pred', 'leak']
    bands = [f'psd_{lo*1e3:g}_{hi*1e3:g}mHz' for lo, hi in nz.BANDS]
    with open(os.path.join(out, 'stamps.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(keys + bands)
        for r in rows:
            w.writerow([r.get(k, '') if not isinstance(r.get(k), float) else f'{r[k]:.6g}' for k in keys] +
                       [f'{x:.6g}' for x in r['bands']])
    np.savez_compressed(os.path.join(out, 'noise_spectra.npz'),
                        **{k: d[k] for k in d.files if k != 'rows' and not k.endswith('_series')})


def virgo(red_fits, tsi_fits, outdir):
    from astropy.io import fits
    os.makedirs(outdir, exist_ok=True)
    r = fits.getdata(red_fits)
    m = r[:, 0] + RED_EPOCH_JD >= TESS_ERA_START_JD
    np.savez_compressed(os.path.join(outdir, 'spm_red_1min.npz'),
                        days_since_1996_01_23_utc=r[m, 0],
                        red_ppm=r[m, 1].astype(np.float32))
    t = fits.getdata(tsi_fits)
    tai_start = (TESS_ERA_START_JD - 2436204.5) * 86400   # TAI seconds since 1958-01-01
    m = t[:, 0] >= tai_start
    np.savez_compressed(os.path.join(outdir, 'tsi_pmo6_1min.npz'),
                        tai_seconds_since_1958=t[m, 0],
                        tsi_wm2_wrr=t[m, 1].astype(np.float32),
                        tsi_wm2_si=t[m, 2].astype(np.float32))
    print('virgo written')


if __name__ == '__main__':
    {'sector': sector, 'virgo': virgo}[sys.argv[1]](*sys.argv[2:])
