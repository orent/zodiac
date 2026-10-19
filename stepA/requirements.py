"""Turn the measured step-A noise spectra into data-volume requirements,
using the solar and kernel models from sensitivity.py.

usage: requirements.py noise.npz [zl_fraction]
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 2)[0])
import sensitivity as sm


def requirements(f_meas, sn_meas, zl_frac, f_lo=0.1e-3, f_hi=25e-3, lat=18.0):
    f = np.arange(f_lo, f_hi, 0.2e-6)
    # Measured PSD is in ppm^2/uHz of total background; the signal is a
    # fraction of the zodiacal part only.
    sn = np.interp(f, f_meas, sn_meas) * 1e-6 / zl_frac**2
    s, tau, w = sm.kernel(lat)
    h = sm.transfer(f, tau, w)
    sx = sm.solar_psd(f)
    dh = -2j * np.pi * f * sm.transfer(f, tau, w * tau)
    f_amp = sm.fisher(f, h[None], sx, sn)[0, 0]
    cov_k = np.linalg.inv(sm.fisher(f, np.vstack([h, dh]), sx, sn))[1, 1]
    edges = [0, 300, 1000, 4000]
    basis, true = [], []
    for a, b in zip(edges[:-1], edges[1:]):
        m = (tau >= a) & (tau < b)
        true.append(w[m].sum())
        basis.append((np.exp(-2j * np.pi * f * a) - np.exp(-2j * np.pi * f * b)) /
                     (2j * np.pi * f * (b - a)))
    cov_b = np.linalg.inv(sm.fisher(f, np.array(basis), sx, sn))
    worst = max(cov_b[i, i] / true[i] ** 2 for i in range(len(true)))
    contrib = 2 * np.abs(h) ** 2 * sx / sn
    share = [contrib[(f >= a) & (f < b)].sum() / contrib.sum()
             for a, b in [(0, 0.5e-3), (0.5e-3, 2e-3), (2e-3, 1)]]
    return dict(detect=25 / f_amp / 86400, scale=25 * cov_k / 86400,
                bins3=9 * worst / 86400, share=share)


def main(path, zl_frac=0.8):
    d = np.load(path)
    zl_frac = float(zl_frac)
    good_days = 184 * 3 / 24     # 184 half-overlapping 6-h windows
    print(f'ZL fraction of background assumed {zl_frac}; one sector ~ {good_days:.0f} usable days\n')
    print('                                   detect 5sig   delay scale 20%   3-bin profile   SNR^2 share <0.5/0.5-2/>2 mHz')
    for cam in (1, 2):
        f = d[f'cam{cam}_f']
        total = d[f'cam{cam}_total']
        common = np.clip(d[f'cam{cam}_half_sum'] - d[f'cam{cam}_half_diff'], 0, None)
        for label, sn, f_lo in (('measured, all stamps', total, 0.1e-3),
                                ('measured, f > 0.5 mHz', total, 0.5e-3),
                                ('common-mode floor only', np.maximum(common, 0.05 * total), 0.1e-3)):
            ok = f > 0
            r = requirements(f[ok], sn[ok], zl_frac, f_lo=f_lo)
            sec = lambda days: f'{days:8.1f} d ({days / good_days:5.1f} sec)'
            print(f'cam {cam} {label:26s} {sec(r["detect"])} {sec(r["scale"])} {sec(r["bins3"])}  ' +
                  '/'.join(f'{x:.0%}' for x in r['share']))
    print('\n"sec" = sectors of camera data at this sector\'s stamp count; '
          'common-mode floor = limit with infinitely many stamps per sector')


if __name__ == '__main__':
    main(*sys.argv[1:])
