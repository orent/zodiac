"""Lag-domain response, including negative (acausal) lags, as a validation.

Fits H(f) = sum_j a_j B_j(f), where B_j is the transfer function of a
boxcar delay bin, to the Welch-summed cross spectra by weighted least
squares. Bins cover negative lags as well: a physical response must be
zero there. Errors come from repeating the fit with VIRGO taken from
unrelated times (null shifts).

usage: lagfit.py reduced_dir noise.npz red.fits
"""

import sys
import numpy as np
from astropy.io import fits
from astropy.time import Time

sys.path.insert(0, __file__.rsplit('/', 2)[0])
sys.path.insert(0, __file__.rsplit('/', 1)[0])
import sensitivity as sm
from quicklook import RED_EPOCH, cadence_clock, highpass, to_minutes, spectra

EDGES = [int(e) for e in __import__("os").environ.get("LAG_EDGES", "-2400,-1200,-600,-240,0,240,600,1200,2400").split(",")]
F_LO, F_HI = 0.1e-3, 8.3e-3


def basis(f):
    out = []
    for a, b in zip(EDGES[:-1], EDGES[1:]):
        out.append((np.exp(-2j * np.pi * f * a) - np.exp(-2j * np.pi * f * b)) /
                   (2j * np.pi * f * (b - a)))
    return np.array(out)


def fit(f, syx, sxx, syy):
    """Weighted LS for real bin weights a_j: syx ~ sxx * sum a_j B_j.
    Weight per frequency ~ sxx / syy (signal power over noise power)."""
    m = (f >= F_LO) & (f <= F_HI)
    B = basis(f[m])
    wgt = 1 / syy[m]                      # var(syx) ~ sxx * syy
    A = (B * sxx[m]).T                    # model columns
    A_ri = np.vstack([A.real * np.sqrt(wgt)[:, None], A.imag * np.sqrt(wgt)[:, None]])
    y_ri = np.concatenate([syx[m].real * np.sqrt(wgt), syx[m].imag * np.sqrt(wgt)])
    a, *_ = np.linalg.lstsq(A_ri, y_ri, rcond=None)
    return a


def model_bins(zl_frac=0.8, lat=18.0):
    s, tau, w = sm.kernel(lat)
    return np.array([zl_frac * w[(tau >= a) & (tau < b)].sum() if a >= 0 else 0
                     for a, b in zip(EDGES[:-1], EDGES[1:])])


def main(reduced, noise_npz, red_fits):
    import glob
    clock = cadence_clock(reduced)
    nz = np.load(noise_npz)
    red = fits.getdata(red_fits)
    t_red = (RED_EPOCH + red[:, 0]).tai
    v_red = red[:, 1] * 1e-6
    c0 = min(np.load(p)['cadenceno'].min() for p in glob.glob(f'{reduced}/*.npz'))
    expect = model_bins()

    results = {}
    for cam in (1, 2):
        y20 = nz[f'cam{cam}_series']
        t20 = Time(clock(c0 + np.arange(len(y20))) + 2457000, format='jd', scale='tdb').tai
        sel = np.flatnonzero((t_red > t20[0] - 1) & (t_red < t20[-1] + 1))
        tm = (t_red[sel] - t20[0]).sec
        y = highpass(to_minutes(tm, (t20 - t20[0]).sec, y20))
        off = 400 * 1440
        x_all = highpass(v_red[sel[0] - off:sel[-1] + off])
        f, syx, sxx, syy, n = spectra(x_all[off:off + len(tm)], y)
        a = fit(f, syx, sxx, syy)
        null = []
        for shift in list(range(-400, -29, 3)) + list(range(30, 400, 3)):
            xs = x_all[off + shift * 1440:off + shift * 1440 + len(tm)]
            fs, b1, b2, b3, m = spectra(xs, y)
            if m > 0.7 * n:
                null.append(fit(fs, b1, b2, b3))
        null = np.array(null)
        results[cam] = (a, null)

    print('Bin weights = fraction of the solar fractional variation reappearing in the')
    print('TESS fractional background per delay bin. Model: 80% ZL, kernel at 18 deg.\n')
    print('  lag bin [s]        cam 1           cam 2          combined       model')
    comb_a, comb_s = [], []
    for j, (lo, hi) in enumerate(zip(EDGES[:-1], EDGES[1:])):
        cells = []
        vals, errs = [], []
        for cam in (1, 2):
            a, null = results[cam]
            s = null[:, j].std()
            cells.append(f'{a[j]:+.3f}±{s:.3f}')
            vals.append(a[j]); errs.append(s)
        w = 1 / np.array(errs) ** 2
        ca = np.sum(np.array(vals) * w) / w.sum()
        cs = 1 / np.sqrt(w.sum())
        comb_a.append(ca); comb_s.append(cs)
        print(f'  {lo:+6d}..{hi:+6d}   {cells[0]:>14s}  {cells[1]:>14s}  '
              f'{ca:+.3f}±{cs:.3f} ({ca/cs:+4.1f}σ)   {expect[j]:.3f}')
    comb_a, comb_s = np.array(comb_a), np.array(comb_s)
    neg = np.array(EDGES[:-1]) < 0
    for label, m in (('negative lags (must be 0)', neg), ('positive lags', ~neg)):
        tot = comb_a[m].sum()
        # Error of the sum from the null distribution of the summed weights,
        # keeping bin-to-bin covariance.
        sn = []
        for cam in (1, 2):
            sn.append(results[cam][1][:, m].sum(1).std())
        w = 1 / np.array(sn) ** 2
        tot_c = np.sum([results[c][0][m].sum() for c in (1, 2)] * w) / w.sum()
        print(f'\n  sum over {label}: {tot_c:+.3f} ± {1/np.sqrt(w.sum()):.3f}'
              f'  (model {expect[m].sum():.3f})')
    np.savez('lagfit.npz', edges=EDGES, expect=expect,
             **{f'cam{c}_a': r[0] for c, r in results.items()},
             **{f'cam{c}_null': r[1] for c, r in results.items()})


if __name__ == '__main__':
    main(*sys.argv[1:])
