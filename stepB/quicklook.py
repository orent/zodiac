"""Quick look: does the TESS background respond to solar variability at all?

Takes the camera-combined fractional background series from step A,
puts it on the VIRGO 1-min grid, and estimates the band-averaged transfer
function H = sum S_yx / sum S_xx (y = TESS, x = VIRGO) in a few frequency
bands. Significance comes from a null distribution: the same estimate with
VIRGO taken from other, unrelated times.

Light-time differences (~5 s) are ignored at this stage.

usage: quicklook.py reduced_dir noise.npz red.fits
"""

import glob, sys
import numpy as np
from astropy.io import fits
from astropy.time import Time
from scipy.ndimage import median_filter

WINDOW = 360                     # 6 h of 1-min samples
BANDS = [(0.1e-3, 0.5e-3), (0.5e-3, 1e-3), (1e-3, 2e-3), (2e-3, 4e-3), (4e-3, 8.3e-3)]
RED_EPOCH = Time('1996-01-23T00:00:00', scale='utc')


def cadence_clock(reduced):
    """Spacecraft time (TDB, JD - 2457000) of each cadence number:
    TIME minus the per-target barycentric correction TIMECORR."""
    cad, tsc = [], []
    for path in sorted(glob.glob(f'{reduced}/*.npz'))[:50]:
        d = np.load(path)
        ok = np.isfinite(d['time']) & np.isfinite(d['timecorr'])
        cad.append(d['cadenceno'][ok])
        tsc.append(d['time'][ok] - d['timecorr'][ok])
    cad, tsc = np.concatenate(cad), np.concatenate(tsc)
    p = np.polyfit(cad - cad.min(), tsc, 1)
    resid = (tsc - np.polyval(p, cad - cad.min())) * 86400
    print(f'cadence clock: step {p[0] * 86400:.6f} s, scatter across targets {resid.std():.3f} s')
    return lambda c: np.polyval(p, c - cad.min())


def highpass(x, size=180):
    y = np.full_like(x, np.nan)
    i = np.flatnonzero(np.isfinite(x))
    y[i] = x[i] - median_filter(x[i], size=size, mode='nearest')
    return y


def to_minutes(t_minute, t_samples, x):
    """Average 20-s samples into 1-min bins centred on t_minute; require
    at least 2 of the 3 samples."""
    edges = np.concatenate([t_minute - 30, [t_minute[-1] + 30]])
    idx = np.searchsorted(edges, t_samples) - 1
    ok = np.isfinite(x) & (idx >= 0) & (idx < len(t_minute))
    s = np.bincount(idx[ok], x[ok], len(t_minute))
    n = np.bincount(idx[ok], minlength=len(t_minute))
    with np.errstate(invalid='ignore'):
        out = s / n
    out[n < 2] = np.nan
    return out


def spectra(x, y):
    """Welch-summed cross and auto spectra over windows where both are complete."""
    w = np.hanning(WINDOW)
    f = np.fft.rfftfreq(WINDOW, 60.0)
    syx = np.zeros(len(f), complex)
    sxx = np.zeros(len(f))
    syy = np.zeros(len(f))
    n = 0
    for i in range(0, len(x) - WINDOW + 1, WINDOW // 2):
        a, b = x[i:i + WINDOW], y[i:i + WINDOW]
        if not (np.isfinite(a).all() and np.isfinite(b).all()):
            continue
        fa = np.fft.rfft((a - a.mean()) * w)
        fb = np.fft.rfft((b - b.mean()) * w)
        syx += fb * fa.conj()
        sxx += np.abs(fa) ** 2
        syy += np.abs(fb) ** 2
        n += 1
    return f, syx, sxx, syy, n


def band_h(f, syx, sxx):
    return np.array([syx[(f >= lo) & (f < hi)].sum() / sxx[(f >= lo) & (f < hi)].sum()
                     for lo, hi in BANDS])


def main(reduced, noise_npz, red_fits):
    clock = cadence_clock(reduced)
    nz = np.load(noise_npz)
    red = fits.getdata(red_fits)
    t_red = (RED_EPOCH + red[:, 0]).tai           # VIRGO minute stamps
    v_red = red[:, 1] * 1e-6

    c0 = None
    for path in glob.glob(f'{reduced}/*.npz'):
        cmin = np.load(path)['cadenceno'].min()
        c0 = cmin if c0 is None else min(c0, cmin)
    for cam in (1, 2):
        y20 = nz[f'cam{cam}_series']
        t20 = Time(clock(c0 + np.arange(len(y20))) + 2457000, format='jd', scale='tdb').tai
        sel = (t_red > t20[0] - 1) & (t_red < t20[-1] + 1)
        sec = lambda t: (t - t20[0]).sec
        tm = sec(t_red[sel])
        y = highpass(to_minutes(tm, sec(t20), y20))
        x_all = highpass(v_red[np.flatnonzero(sel)[0] - 400 * 1440:np.flatnonzero(sel)[-1] + 400 * 1440])
        off = 400 * 1440
        x = x_all[off:off + len(tm)]

        f, syx, sxx, syy, n = spectra(x, y)
        h = band_h(f, syx, sxx)
        coh = np.abs(syx) ** 2 / (sxx * syy)

        # Null: VIRGO from other times, shifted by whole days (>= 30 d away).
        null = []
        for shift in list(range(-400, -29, 3)) + list(range(30, 400, 3)):
            xs = x_all[off + shift * 1440:off + shift * 1440 + len(tm)]
            fs, a, b, _, m = spectra(xs, y)
            if m > 0.7 * n:
                null.append(band_h(fs, a, b))
        null = np.array(null)
        sig = np.sqrt(0.5 * (null.real.var(0) + null.imag.var(0)))
        print(f'\ncamera {cam}: {n} windows, {len(null)} null shifts')
        print('  band [mHz]     H real   H imag     |H|   phase   |H|/sigma_null  null 95% |H|')
        for (lo, hi), hb, s, nb in zip(BANDS, h, sig, null.T):
            q95 = np.percentile(np.abs(nb), 95)
            print(f'  {lo*1e3:4.1f}-{hi*1e3:4.1f}    {hb.real:+.3f}   {hb.imag:+.3f}   {abs(hb):.3f}  '
                  f'{np.angle(hb):+.2f}   {abs(hb)/s:6.1f}          {q95:.3f}')
        np.savez(f'quicklook_cam{cam}.npz', f=f, syx=syx, sxx=sxx, syy=syy, coh=coh,
                 h=h, null=null, n=n)


if __name__ == '__main__':
    main(*sys.argv[1:])
