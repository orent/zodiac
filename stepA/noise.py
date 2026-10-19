"""Step A: noise budget of the TESS 20-s background (SAP_BKG) for one sector.

For every stamp: fractional background fluctuation, its PSD, and the
photon + read noise prediction from the background level and the number
of background pixels. Then per camera: the inverse-variance combination of
all stamps, split into random halves. The half-difference keeps only noise
that is independent between stamps; the half-sum also keeps the common
mode. Their difference is the floor that more stamps cannot beat.

usage: noise.py reduced_dir [out.npz]
"""

import ast, glob, os, sys
import numpy as np
from scipy.ndimage import median_filter, uniform_filter1d

# Cadence of the input light curves [s]: 20 (fast) or 120 (2-min). 2-min
# data has onboard cosmic-ray mitigation (8 of 10 frames kept per 20 s).
CADENCE = float(os.environ.get('TESS_CADENCE', 20))
EXPOSED = 0.99 if CADENCE <= 20 else 0.8
# QUALITY bits rejected: attitude tweak, safe mode, coarse point, earth point,
# Argabrightening, desaturation, aperture cosmic, manual exclude, impulsive
# outlier, collateral cosmic, straylight, scattered light, insufficient targets.
BADBITS = 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 | 512 | 1024 | 2048 | 4096 | 32768
READ_NOISE = 10.0           # e-/pixel per 2 s frame
SMOOTH = 3 * 3600 / CADENCE  # high-pass: divide by a 3 h running median
WINDOW = int(6 * 3600 / CADENCE)
BANDS = [(0.1e-3, 0.5e-3), (0.5e-3, 2e-3), (2e-3, 5e-3), (5e-3, 25e-3)]


def load(path):
    d = np.load(path)
    h = ast.literal_eval(str(d['header']))
    return h, {k: d[k].astype(float) if d[k].dtype.kind == 'f' else d[k] for k in d.files}


def fractional(h, d):
    """Fractional background fluctuation and predicted white-noise sigma."""
    b = d['sap_bkg'] / h['NPIX_AP']                       # e-/s per pixel
    good = (d['quality'] & BADBITS) == 0
    good &= np.isfinite(b) & np.isfinite(d['time']) & (b > 0)
    level = np.full_like(b, np.nan)
    idx = np.flatnonzero(good)
    level[idx] = median_filter(b[idx], size=int(SMOOTH), mode='nearest')
    x = b / level - 1
    # Cosmic rays and other single-cadence spikes: 5 sigma against a short median.
    resid = x[idx] - median_filter(x[idx], size=9, mode='nearest')
    mad = 1.4826 * np.median(np.abs(resid))
    spike = np.zeros_like(good)
    spike[idx[np.abs(resid) > 5 * mad]] = True
    spike |= np.roll(spike, 1) | np.roll(spike, -1)
    good &= ~spike
    texp = CADENCE * EXPOSED
    var_px = level * texp + (CADENCE / 2) * READ_NOISE**2
    sigma = np.sqrt(var_px / h['NPIX_BKG']) / (level * texp)
    x[~good] = np.nan
    return x, sigma


def welch(x, cadence=CADENCE, window=WINDOW, min_fill=0.9):
    """One-sided PSD in ppm^2/uHz of a gappy series (NaN = missing)."""
    w = np.hanning(window)
    f = np.fft.rfftfreq(window, cadence)
    acc, n = np.zeros(len(f)), 0
    for i in range(0, len(x) - window + 1, window // 2):
        seg = x[i:i + window]
        ok = np.isfinite(seg)
        if ok.mean() < min_fill:
            continue
        t = np.arange(window)
        p = np.polyfit(t[ok], seg[ok], 1)
        s = np.where(ok, seg - np.polyval(p, t), 0) / np.sqrt(ok.mean())
        acc += np.abs(np.fft.rfft(s * w)) ** 2
        n += 1
    psd = 2 * acc / max(n, 1) * cadence / np.sum(w**2)
    return f, psd * 1e6, n          # frac^2/Hz -> ppm^2/uHz


def cross(x, y, cadence=CADENCE, window=WINDOW, min_fill=0.9):
    """Cross-spectral coherence |Sxy|^2 / (Sxx Syy), Welch-averaged."""
    w = np.hanning(window)
    sxy = sxx = syy = 0
    n = 0
    for i in range(0, len(x) - window + 1, window // 2):
        a, b = x[i:i + window], y[i:i + window]
        ok = np.isfinite(a) & np.isfinite(b)
        if ok.mean() < min_fill:
            continue
        fa = np.fft.rfft(np.where(ok, a - np.nanmean(a[ok]), 0) * w)
        fb = np.fft.rfft(np.where(ok, b - np.nanmean(b[ok]), 0) * w)
        sxy = sxy + fa * fb.conj()
        sxx = sxx + np.abs(fa) ** 2
        syy = syy + np.abs(fb) ** 2
        n += 1
    return np.fft.rfftfreq(window, cadence), np.abs(sxy) ** 2 / (sxx * syy), n


def band_mean(f, p, lo, hi):
    m = (f >= lo) & (f < hi)
    return np.mean(p[m])


def combine(xs, sigmas):
    """Inverse-variance weighted mean per cadence; returns series and
    predicted white-noise PSD level (ppm^2/uHz) of the combination."""
    w = np.where(np.isfinite(xs), 1 / sigmas**2, 0)
    tot = w.sum(0)
    with np.errstate(invalid='ignore', divide='ignore'):
        comb = np.nansum(np.where(np.isfinite(xs), xs, 0) * w, 0) / tot
        pred = 2 * CADENCE / np.nanmedian(tot[tot > 0]) * 1e6
    comb[tot < 0.5 * np.nanmedian(tot)] = np.nan
    return comb, pred


def main(reduced, out=None):
    stamps = []
    for path in sorted(glob.glob(f'{reduced}/*.npz')):
        try:
            h, d = load(path)
        except Exception as e:
            print('skip', path, e)
            continue
        stamps.append((h, d))
    c0 = min(d['cadenceno'].min() for h, d in stamps)
    c1 = max(d['cadenceno'].max() for h, d in stamps)
    ncad = c1 - c0 + 1

    rows, series = [], {}
    for h, d in stamps:
        x, sigma = fractional(h, d)
        f, p, nseg = welch(x)
        if nseg < 20:
            continue
        pred = 2 * CADENCE * np.nanmedian(sigma) ** 2 * 1e6
        # Target light leaking into the background estimate. SAP_FLUX is
        # background-subtracted, so compare against the raw aperture flux;
        # only meaningful where the star dominates the aperture.
        raw = d['sap_flux'] + d['sap_bkg']
        fx, coh, _ = cross(x, raw / np.nanmedian(raw) - 1)
        leak = band_mean(fx, coh, 0.1e-3, 2e-3)
        rows.append(dict(tic=h['TICID'], cam=h['CAMERA'], ccd=h['CCD'], tmag=h['TESSMAG'],
                         nbkg=h['NPIX_BKG'], level=np.nanmedian(d['sap_bkg']) / h['NPIX_AP'],
                         pred=pred, bands=[band_mean(f, p, *b) for b in BANDS], leak=leak))
        full_x = np.full(ncad, np.nan)
        full_s = np.full(ncad, np.nan)
        full_x[d['cadenceno'] - c0] = x
        full_s[d['cadenceno'] - c0] = sigma
        series[h['TICID']] = (h['CAMERA'], full_x, full_s, rows[-1])

    print(f'{len(rows)} stamps with >=20 good 6-h windows\n')
    print('Per-stamp PSD, median over stamps [ppm^2/uHz]; excess = measured / photon+read prediction')
    hdr = ' cam  n   pred  ' + '  '.join(f'{lo*1e3:.1f}-{hi*1e3:.1f}mHz(x)' for lo, hi in BANDS) + '  leak'
    print(hdr)
    for cam in sorted({r['cam'] for r in rows}):
        rs = [r for r in rows if r['cam'] == cam]
        pred = np.median([r['pred'] for r in rs])
        meas = [np.median([r['bands'][i] for r in rs]) for i in range(len(BANDS))]
        ex = [np.median([r['bands'][i] / r['pred'] for r in rs]) for i in range(len(BANDS))]
        print(f'  {cam}  {len(rs):3d} {pred:6.0f}  ' +
              '  '.join(f'{m:9.0f} ({e:4.1f})' for m, e in zip(meas, ex)) +
              f'  {np.median([r["leak"] for r in rs]):.3f}')

    print('\nCamera combination (vetted: Tmag<10 with raw-flux coherence > 0.05; in-band excess > 3x median)')
    rng = np.random.default_rng(1)
    combos, results, ccd_combos = {}, {}, {}
    for cam in sorted({v[0] for v in series.values()}):
        vs = [v for v in series.values() if v[0] == cam]
        exc = np.array([v[3]['bands'][1] / v[3]['pred'] for v in vs])
        keep = [v for v, e in zip(vs, exc) if e < 3 * np.median(exc)
                and not (v[3]['tmag'] < 10 and v[3]['leak'] > 0.05)]
        xs = np.array([v[1] for v in keep])
        ss = np.array([v[2] for v in keep])
        comb, pred = combine(xs, ss)
        perm = rng.permutation(len(keep))
        a, _ = combine(xs[perm[::2]], ss[perm[::2]])
        b, _ = combine(xs[perm[1::2]], ss[perm[1::2]])
        f, p_all, nseg = welch(comb)
        _, p_sum, _ = welch((a + b) / 2)
        _, p_diff, _ = welch((a - b) / 2)
        combos[cam] = comb
        results[cam] = dict(f=f, total=p_all, half_sum=p_sum, half_diff=p_diff, pred=pred)
        # Per-CCD combinations (same vetting): 4 lines of sight per camera.
        for ccd in sorted({v[3]['ccd'] for v in keep}):
            kc = [k for k, v in enumerate(keep) if v[3]['ccd'] == ccd]
            if len(kc) >= 5:
                ccd_combos[(cam, ccd)] = combine(xs[kc], ss[kc])[0]
        print(f'  camera {cam}: {len(keep)}/{len(vs)} stamps kept, {nseg} windows, '
              f'predicted white floor {pred:.2f} ppm^2/uHz')
        for lo, hi in BANDS:
            t, s, dd = (band_mean(f, q, lo, hi) for q in (p_all, p_sum, p_diff))
            print(f'     {lo*1e3:4.1f}-{hi*1e3:4.1f} mHz: total {t:8.2f}  '
                  f'independent {dd:8.2f}  common {max(s - dd, 0):8.2f}  (x{t/pred:.1f} of photon)')
    if 1 in combos and 2 in combos:
        f, coh, n = cross(combos[1], combos[2])
        print(f'\n  camera 1 vs 2 coherence ({n} windows, bias ~{1/n:.3f}): ' +
              '  '.join(f'{lo*1e3:.1f}-{hi*1e3:.1f}mHz {band_mean(f, coh, lo, hi):.3f}'
                        for lo, hi in BANDS))
    if out:
        np.savez(out, rows=np.array(repr(rows)),
                 **{f'cam{c}_{k}': v for c, r in results.items() for k, v in r.items()},
                 **{f'cam{c}_series': v for c, v in combos.items()},
                 **{f'cam{c}_ccd{k}_series': v for (c, k), v in ccd_combos.items()})


if __name__ == '__main__':
    main(*sys.argv[1:])
