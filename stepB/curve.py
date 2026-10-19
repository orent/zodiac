"""Visual impulse response: band-limited, Wiener-weighted inverse transform of
H = S_yx / S_xx, both cameras combined, with a null envelope from VIRGO
taken at unrelated times, and the model kernel passed through the same
filter.

usage: curve.py reduced_dir noise.npz red.fits out.png
"""

import glob, sys
import numpy as np
from astropy.io import fits
from astropy.time import Time

sys.path.insert(0, __file__.rsplit('/', 2)[0])
sys.path.insert(0, __file__.rsplit('/', 1)[0])
import sensitivity as sm
from quicklook import RED_EPOCH, cadence_clock, highpass, to_minutes, spectra

F_LO, F_HI = 0.1e-3, 8.3e-3
TAU = np.arange(-1800, 2401, 20)


def inverse(f, h, wgt):
    """h(tau) per 60 s from a one-sided transfer function on f."""
    df = f[1] - f[0]
    ph = np.exp(2j * np.pi * np.outer(TAU, f))
    return 2 * (ph @ (wgt * h)).real * df * 60


def main(reduced, noise_npz, red_fits, out_png):
    clock = cadence_clock(reduced)
    nz = np.load(noise_npz)
    red = fits.getdata(red_fits)
    t_red = (RED_EPOCH + red[:, 0]).tai
    v_red = red[:, 1] * 1e-6
    c0 = min(np.load(p)['cadenceno'].min() for p in glob.glob(f'{reduced}/*.npz'))

    acc = {}
    for cam in (1, 2):
        y20 = nz[f'cam{cam}_series']
        t20 = Time(clock(c0 + np.arange(len(y20))) + 2457000, format='jd', scale='tdb').tai
        sel = np.flatnonzero((t_red > t20[0] - 1) & (t_red < t20[-1] + 1))
        tm = (t_red[sel] - t20[0]).sec
        y = highpass(to_minutes(tm, (t20 - t20[0]).sec, y20))
        off = 400 * 1440
        x_all = highpass(v_red[sel[0] - off:sel[-1] + off])
        f, syx, sxx, syy, n = spectra(x_all[off:off + len(tm)], y)
        nulls = []
        for shift in list(range(-400, -29, 3)) + list(range(30, 400, 3)):
            xs = x_all[off + shift * 1440:off + shift * 1440 + len(tm)]
            fs, a, b, c, m = spectra(xs, y)
            if m > 0.7 * n:
                nulls.append((a, b))
        acc[cam] = (f, syx, sxx, syy, nulls)

    # Combine cameras by summing cross and auto spectra (equal footing).
    f = acc[1][0]
    syx = acc[1][1] + acc[2][1]
    sxx = acc[1][2] + acc[2][2]
    band = (f >= F_LO) & (f <= F_HI)
    # Wiener-like weight: roll off where the solar probe is weak.
    c = np.median(sxx[(f > 3e-3) & (f < 5e-3)])
    wgt = np.where(band, sxx / (sxx + c), 0)
    with np.errstate(invalid='ignore', divide='ignore'):
        h_meas = np.where(band, syx / sxx, 0)
    meas = inverse(f, h_meas, wgt)

    k = min(len(acc[1][4]), len(acc[2][4]))
    null_curves = []
    for i in range(k):
        a = acc[1][4][i][0] + acc[2][4][i][0]
        b = acc[1][4][i][1] + acc[2][4][i][1]
        with np.errstate(invalid='ignore', divide='ignore'):
            null_curves.append(inverse(f, np.where(band, a / b, 0), wgt))
    null_curves = np.array(null_curves)

    s, tau, w = sm.kernel(18.0)
    hk = np.zeros(len(f), complex)
    hk[1:] = sm.transfer(f[1:], tau, w)
    model = inverse(f, hk, wgt)
    # Least-squares amplitude of the filtered model against the measured curve.
    amp = np.dot(meas, model) / np.dot(model, model)
    # True (unfiltered) model kernel per 60 s, for reference.
    kern = np.interp(TAU, tau, w / np.gradient(tau) * 60, left=0, right=0)

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    ink, ink2, grid = '#0b0b0b', '#52514e', '#e4e3df'
    blue, orange = '#2a78d6', '#eb6834'
    fig, ax = plt.subplots(figsize=(9, 5), dpi=150)
    fig.patch.set_facecolor('#fcfcfb')
    ax.set_facecolor('#fcfcfb')
    tm = TAU / 60
    lo2, lo1, hi1, hi2 = np.percentile(null_curves, [2.5, 16, 84, 97.5], axis=0)
    ax.fill_between(tm, lo2, hi2, color='#d9d8d3', lw=0, label='null 95% (VIRGO from other dates)')
    ax.fill_between(tm, lo1, hi1, color='#bdbcb6', lw=0, label='null 68%')
    ax.plot(tm, amp * model, color=orange, lw=2, ls='--',
            label=f'model kernel, same filter (×{amp:.2f})')
    ax.plot(tm, amp * kern, color=orange, lw=1, alpha=0.6,
            label='model kernel, unfiltered')
    ax.plot(tm, meas, color=blue, lw=2, label='measured, cameras 1+2, sector 27')
    ax.axvline(0, color=ink2, lw=1)
    ax.axhline(0, color=ink2, lw=0.8)
    ax.set_xlim(tm[0], tm[-1])
    ax.set_xlabel('delay of TESS background after VIRGO [min]', color=ink)
    ax.set_ylabel('response per minute of delay', color=ink)
    ax.set_title('Zodiacal light impulse response: TESS 20-s background vs VIRGO red',
                 color=ink, loc='left', fontsize=11)
    ax.grid(color=grid, lw=0.6)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(ink2)
    ax.tick_params(colors=ink2)
    ax.annotate('causal side', xy=(1, 0.97), xycoords=('data', 'axes fraction'),
                color=ink2, fontsize=9, va='top')
    ax.annotate('acausal side: must be ~0', xy=(-29, 0.97), xycoords=('data', 'axes fraction'),
                color=ink2, fontsize=9, va='top')
    leg = ax.legend(loc='upper right', fontsize=8, frameon=False, bbox_to_anchor=(1, 0.9))
    for t in leg.get_texts():
        t.set_color(ink)
    fig.tight_layout()
    fig.savefig(out_png, facecolor=fig.get_facecolor())

    neg = TAU < -120
    print(f'model amplitude {amp:.2f}; peak measured {meas.max():.3f} at {TAU[np.argmax(meas)]} s; '
          f'null 1-sigma at 0 s {(hi1 - lo1)[TAU == 0][0] / 2:.3f}')
    print(f'fraction of acausal points outside null 95%: '
          f'{np.mean((meas[neg] < lo2[neg]) | (meas[neg] > hi2[neg])):.2f} (expect ~0.05)')


if __name__ == '__main__':
    main(*sys.argv[1:])
