"""Earthshine split test.

Earth- and Moon-scattered light carries the solar signal at ~zero delay.
Work in absolute units (e-/s/px): the zodiacal response is then roughly
constant over a sector, while a scattered-light term grows with the
scattered-light level. Windows are split by background level above the
sector floor (a scattered-light proxy), and each subset gets:
  - the lag-bin response (near-zero bin vs. later bins),
  - a two-component fit: model ZL kernel (amplitude A) + zero-delay term
    (amplitude E), distinguished by frequency dependence (kernel ~1/f,
    zero-delay flat).
Errors from null shifts (VIRGO from unrelated dates).

usage: earthshine.py reduced_dir noise.npz red.fits
"""

import glob, sys
import numpy as np
from astropy.io import fits
from astropy.time import Time

sys.path.insert(0, __file__.rsplit('/', 2)[0])
sys.path.insert(0, __file__.rsplit('/', 1)[0])
sys.path.insert(0, __file__.rsplit('/', 2)[0] + '/stepA')
import sensitivity as sm
import noise as nz
from quicklook import RED_EPOCH, cadence_clock, highpass, to_minutes

WINDOW = 360
F_LO, F_HI = 0.1e-3, 8.3e-3
EDGES = [-600, -240, 0, 240, 600, 1200]
NSPLIT = 3


def camera_level(reduced, cam, c0, ncad):
    """Median absolute background per pixel across the camera's stamps."""
    rows = []
    for p in sorted(glob.glob(f'{reduced}/*.npz')):
        h, d = nz.load(p)
        if h['CAMERA'] != cam:
            continue
        b = d['sap_bkg'] / h['NPIX_AP']
        good = ((d['quality'] & nz.BADBITS) == 0) & np.isfinite(b)
        full = np.full(ncad, np.nan)
        full[d['cadenceno'] - c0] = np.where(good, b, np.nan)
        rows.append(full)
    return np.nanmedian(np.array(rows), 0)


def window_ffts(x, y, lev):
    """Per-window FFTs of complete windows, with each window's mean level."""
    w = np.hanning(WINDOW)
    fx, fy, lv = [], [], []
    for i in range(0, len(x) - WINDOW + 1, WINDOW // 2):
        a, b, l = x[i:i + WINDOW], y[i:i + WINDOW], lev[i:i + WINDOW]
        if np.isfinite(a).all() and np.isfinite(b).all():
            fx.append(np.fft.rfft((a - a.mean()) * w))
            fy.append(np.fft.rfft((b - b.mean()) * w))
            lv.append(np.nanmean(l))
    return np.array(fx), np.array(fy), np.array(lv)


def fit(f, fx, fy, cols):
    """Weighted LS for real amplitudes of model columns H_j(f):
    syx ~ sxx * sum a_j H_j."""
    syx = (fy * fx.conj()).sum(0)
    sxx = (np.abs(fx) ** 2).sum(0)
    syy = (np.abs(fy) ** 2).sum(0)
    m = (f >= F_LO) & (f <= F_HI)
    sw = np.sqrt(1 / syy[m])
    A = np.array([sxx[m] * c[m] for c in cols]).T
    Ari = np.vstack([A.real * sw[:, None], A.imag * sw[:, None]])
    yri = np.concatenate([syx[m].real * sw, syx[m].imag * sw])
    return np.linalg.lstsq(Ari, yri, rcond=None)[0]


def main(reduced, noise_npz, red_fits):
    clock = cadence_clock(reduced)
    nzd = np.load(noise_npz)
    red = fits.getdata(red_fits)
    t_red = (RED_EPOCH + red[:, 0]).tai
    v_red = red[:, 1] * 1e-6
    c0 = min(np.load(p)['cadenceno'].min() for p in glob.glob(f'{reduced}/*.npz'))
    f = np.fft.rfftfreq(WINDOW, 60.0)
    fz = np.where(f > 0, f, 1e-9)
    s, tau, w = sm.kernel(18.0)
    hk = np.zeros(len(f), complex)
    hk[1:] = sm.transfer(f[1:], tau, w)
    bins = [(np.exp(-2j * np.pi * fz * a) - np.exp(-2j * np.pi * fz * b)) /
            (2j * np.pi * fz * (b - a)) for a, b in zip(EDGES[:-1], EDGES[1:])]
    two = [hk, np.ones(len(f), complex)]      # ZL kernel, zero-delay term

    for cam in (1, 2):
        frac = nzd[f'cam{cam}_series']
        lev20 = camera_level(reduced, cam, c0, len(frac))
        t20 = Time(clock(c0 + np.arange(len(frac))) + 2457000, format='jd', scale='tdb').tai
        sel = np.flatnonzero((t_red > t20[0] - 1) & (t_red < t20[-1] + 1))
        tm = (t_red[sel] - t20[0]).sec
        ts = (t20 - t20[0]).sec
        lev = to_minutes(tm, ts, lev20)
        # Absolute fluctuation: fractional series times the local level.
        y = highpass(to_minutes(tm, ts, frac) * lev)
        off = 400 * 1440
        x_all = highpass(v_red[sel[0] - off:sel[-1] + off])
        fx, fy, lv = window_ffts(x_all[off:off + len(tm)], y, lev)
        floor = np.percentile(lv, 5)
        cuts = np.percentile(lv, np.linspace(0, 100, NSPLIT + 1))
        groups = [(lv >= lo) & (lv <= hi) for lo, hi in zip(cuts[:-1], cuts[1:])]

        # Null fits per group, VIRGO shifted to unrelated dates. The window
        # selection is kept fixed so the null sees the same TESS data.
        nulls_b, nulls_2 = [[] for _ in groups], [[] for _ in groups]
        for shift in list(range(-400, -29, 3)) + list(range(30, 400, 3)):
            xs = x_all[off + shift * 1440:off + shift * 1440 + len(tm)]
            gx, gy, gl = window_ffts(xs, y, lev)
            if len(gl) < 0.7 * len(lv):
                continue
            gg = [(gl >= lo) & (gl <= hi) for lo, hi in zip(cuts[:-1], cuts[1:])]
            for k, g in enumerate(gg):
                nulls_b[k].append(fit(f, gx[g], gy[g], bins))
                nulls_2[k].append(fit(f, gx[g], gy[g], two))

        print(f'\ncamera {cam}: {len(lv)} windows, level floor (5th pct) {floor:.0f} e-/s/px')
        print('  subset by level   n   excess   lag bins [e-/s/px per unit solar fraction]'
              '                          | ZL kernel A        zero-delay E')
        print('                              ' +
              '  '.join(f'{a:+5d}..{b:+5d}' for a, b in zip(EDGES[:-1], EDGES[1:])))
        for k, g in enumerate(groups):
            ab = fit(f, fx[g], fy[g], bins)
            a2 = fit(f, fx[g], fy[g], two)
            sb = np.array(nulls_b[k]).std(0)
            s2 = np.array(nulls_2[k]).std(0)
            print(f'  {cuts[k]:4.0f}-{cuts[k+1]:4.0f}     {g.sum():3d}  {lv[g].mean() - floor:5.0f}   ' +
                  '  '.join(f'{v:+6.0f}±{e:3.0f}' for v, e in zip(ab, sb)) +
                  f'  | {a2[0]:+6.0f}±{s2[0]:3.0f}   {a2[1]:+6.0f}±{s2[1]:3.0f}')


if __name__ == '__main__':
    main(*sys.argv[1:])
