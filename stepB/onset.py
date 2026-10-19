"""Onset scan: fit the model kernel shape, shifted to trial onset tau0, with a
single free amplitude. A physical response peaks at tau0 ~ 0; a response at
negative tau0 would mean acausal (timing or instrumental) correlation.

usage: onset.py reduced_dir noise.npz red.fits
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
TAU0 = np.arange(-1200, 1201, 30)


def scan(f, syx, sxx, syy, hk):
    """Amplitude and chi^2 per trial onset. hk = kernel transfer on f."""
    m = (f >= F_LO) & (f <= F_HI)
    wgt = 1 / syy[m]
    amp, chi = [], []
    for t0 in TAU0:
        model = sxx[m] * hk[m] * np.exp(-2j * np.pi * f[m] * t0)
        a = np.sum(wgt * (syx[m] * model.conj()).real) / np.sum(wgt * np.abs(model) ** 2)
        amp.append(a)
        chi.append(np.sum(wgt * np.abs(syx[m] - a * model) ** 2))
    return np.array(amp), np.array(chi)


def main(reduced, noise_npz, red_fits):
    clock = cadence_clock(reduced)
    nz = np.load(noise_npz)
    red = fits.getdata(red_fits)
    t_red = (RED_EPOCH + red[:, 0]).tai
    v_red = red[:, 1] * 1e-6
    c0 = min(np.load(p)['cadenceno'].min() for p in glob.glob(f'{reduced}/*.npz'))
    s, tau, w = sm.kernel(18.0)

    tot_amp, tot_null = 0, 0
    for cam in (1, 2):
        y20 = nz[f'cam{cam}_series']
        t20 = Time(clock(c0 + np.arange(len(y20))) + 2457000, format='jd', scale='tdb').tai
        sel = np.flatnonzero((t_red > t20[0] - 1) & (t_red < t20[-1] + 1))
        tm = (t_red[sel] - t20[0]).sec
        y = highpass(to_minutes(tm, (t20 - t20[0]).sec, y20))
        off = 400 * 1440
        x_all = highpass(v_red[sel[0] - off:sel[-1] + off])
        f, syx, sxx, syy, n = spectra(x_all[off:off + len(tm)], y)
        hk = np.zeros(len(f), complex)
        hk[1:] = sm.transfer(f[1:], tau, w)
        amp, chi = scan(f, syx, sxx, syy, hk)
        null = []
        for shift in list(range(-400, -29, 3)) + list(range(30, 400, 3)):
            xs = x_all[off + shift * 1440:off + shift * 1440 + len(tm)]
            fs, b1, b2, b3, m = spectra(xs, y)
            if m > 0.7 * n:
                null.append(scan(fs, b1, b2, b3, hk)[0])
        null = np.array(null)
        sig = null.std(0)
        w_cam = 1 / sig**2
        tot_amp = tot_amp + amp * w_cam
        tot_null = tot_null + w_cam
        best = np.argmin(chi)
        print(f'camera {cam}: best onset {TAU0[best]:+d} s, amplitude {amp[best]:.3f} '
              f'({amp[best] / sig[best]:.1f} sigma); amplitude at 0 s {amp[TAU0 == 0][0]:.3f}')
    comb = tot_amp / tot_null
    csig = 1 / np.sqrt(tot_null)
    snr = comb / csig
    print('\ncombined amplitude/sigma vs trial onset:')
    for t0, a, z in zip(TAU0, comb, snr):
        if t0 % 120 == 0:
            print(f'  {t0:+5d} s  {a:+.3f}  {z:+5.1f}  ' + '#' * max(0, int(round(z * 4))))
    i = np.argmax(snr)
    # Onset uncertainty: where the S/N drops by 1 from its peak (delta chi^2 = 1).
    ok = snr >= snr[i] - 1
    print(f'\npeak at {TAU0[i]:+d} s (S/N {snr[i]:.1f}); range within 1 sigma of peak: '
          f'{TAU0[ok].min():+d} .. {TAU0[ok].max():+d} s')
    np.savez('onset.npz', tau0=TAU0, amp=comb, sigma=csig)


if __name__ == '__main__':
    main(*sys.argv[1:])
