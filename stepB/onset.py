"""Onset scan: fit the model kernel shape, shifted to trial onset tau0, with a
single free amplitude. A physical response peaks at tau0 ~ 0; a response at
negative tau0 would mean acausal (timing or instrumental) correlation.

usage: onset.py datadir sector [red|tsi] [virgo_stamp_fraction]

Runs on the compact data branch with the emission-time base (timebase.py).
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 2)[0])
sys.path.insert(0, __file__.rsplit('/', 1)[0])
import sensitivity as sm
from quicklook import spectra

F_LO, F_HI = 0.1e-3, 8.3e-3
TAU0 = np.arange(-600, 601, 10)


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


def main(datadir, sector, ref='red', stamp='0.5'):
    from aligned import load
    s, tau, w = sm.kernel(18.0)
    tot_amp, tot_null = 0, 0
    for cam in (1, 2):
        a = load(datadir, sector, cam, ref, float(stamp))
        x, y, x_all, off, n_min = a['x'], a['y'], a['x_all'], a['off'], a['n']
        f, syx, sxx, syy, n = spectra(x, y)
        hk = np.zeros(len(f), complex)
        hk[1:] = sm.transfer(f[1:], tau, w)
        amp, chi = scan(f, syx, sxx, syy, hk)
        null = []
        for shift in list(range(-400, -29, 3)) + list(range(30, 400, 3)):
            i = off + shift * 1440
            if i < 0 or i + n_min > len(x_all):
                continue
            fs, b1, b2, b3, m = spectra(x_all[i:i + n_min], y)
            if m > 0.7 * n:
                null.append(scan(fs, b1, b2, b3, hk)[0])
        null = np.array(null)
        sig = null.std(0)
        tot_amp = tot_amp + amp / sig**2
        tot_null = tot_null + 1 / sig**2
        best = np.argmin(chi)
        print(f'camera {cam}: {n} windows, {len(null)} nulls; best onset {TAU0[best]:+d} s, '
              f'amplitude {amp[best]:.3f} ({amp[best] / sig[best]:.1f} sigma)')
    comb = tot_amp / tot_null
    snr = comb / (1 / np.sqrt(tot_null))
    i = np.argmax(snr)
    # Finer peak location: parabola through the S/N peak and its neighbours.
    if 0 < i < len(TAU0) - 1:
        y0, y1, y2 = snr[i - 1:i + 2]
        dx = 0.5 * (y0 - y2) / (y0 - 2 * y1 + y2) * (TAU0[1] - TAU0[0])
    else:
        dx = 0.0
    # 1-sigma onset interval: chi^2 = const - S/N^2, so delta chi^2 = 1
    # where S/N^2 has dropped by 1 from its peak.
    tf = np.arange(TAU0[0], TAU0[-1] + 0.1, 0.5)
    sf = np.interp(tf, TAU0, snr)
    ok = sf ** 2 >= sf.max() ** 2 - 1
    print(f'combined: peak at {TAU0[i] + dx:+.0f} s (S/N {snr[i]:.1f}); '
          f'1-sigma interval {tf[ok].min():+.0f} .. {tf[ok].max():+.0f} s')
    np.savez('onset.npz', tau0=TAU0, amp=comb, snr=snr)


if __name__ == '__main__':
    main(*sys.argv[1:])
