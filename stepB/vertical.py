"""Joint 3-D fit across cameras: shared radial index alpha and vertical
scale factor v (v = 1: Kelsall fan profile; v > 1: dust extends higher
above the symmetry plane), per-camera amplitude A_c (zodiacal share of
the floor, coherence) and Earthshine E_c, timing fixed.

The four cameras look anti-sunward at ecliptic latitudes 18/42/66/90 deg,
so they cut the cloud at different heights: camera 1 mostly constrains
alpha, cameras 3-4 mostly v. Errors: profile likelihood and block
bootstrap over windows.

usage: vertical.py datadir SECTORS [cameras] [dt]
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 2)[0])
sys.path.insert(0, __file__.rsplit('/', 1)[0])
import sensitivity as sm
import geometry
import joint

ALPHAS = np.round(np.arange(0.2, 2.61, 0.2), 2)
VS = np.round(np.arange(0.5, 2.51, 0.25), 2)
NBOOT = 200


class Terms:
    """Per-window sums for every (alpha, v) at fixed dt."""

    def __init__(self, parts, lat, dt):
        acc = {k: [] for k in ('eps', 'R', 'V', 'YY')}
        self.P = {}
        self.Q = {}
        self.U = {}
        cache = {}
        for f, cad, X, Y, eps, u in parts:
            m = (f >= joint.F_LO) & (f <= joint.F_HI)
            fm = f[m]
            Xm, Ym = X[:, m], Y[:, m]
            W = joint.noise_weights(Ym)
            XX = W * np.abs(Xm) ** 2
            YX = W * Ym * Xm.conj()
            acc['eps'].append(eps)
            acc['R'].append(XX.sum(1))
            acc['V'].append(YX.real.sum(1))
            acc['YY'].append((W * np.abs(Ym) ** 2).sum(1))
            ph = np.exp(-2j * np.pi * fm * dt)
            for al in ALPHAS:
                for v in VS:
                    key = (tuple(np.round(u, 2)), al, v, cad)
                    if key not in cache:
                        cache[key] = geometry.transfer(fm, u, al, v)
                    h = cache[key] * ph
                    self.P.setdefault((al, v), []).append(XX @ np.abs(h) ** 2)
                    self.Q.setdefault((al, v), []).append(XX @ h.real)
                    self.U.setdefault((al, v), []).append((YX @ h.conj()).real)
        for k, val in acc.items():
            setattr(self, k, np.concatenate(val))
        for d in (self.P, self.Q, self.U):
            for k in d:
                d[k] = np.concatenate(d[k])

    def solve(self, wk=None):
        wk = np.ones(len(self.eps)) if wk is None else wk
        e = self.eps
        m22 = np.sum(wk * e * e * self.R)
        r2 = np.sum(wk * e * self.V)
        yy = np.sum(wk * self.YY)
        chi = np.empty((len(ALPHAS), len(VS)))
        AE = np.empty((len(ALPHAS), len(VS), 2))
        for i, al in enumerate(ALPHAS):
            for j, v in enumerate(VS):
                m11 = np.sum(wk * self.P[(al, v)])
                m12 = np.sum(wk * e * self.Q[(al, v)])
                r1 = np.sum(wk * self.U[(al, v)])
                det = m11 * m22 - m12 ** 2
                A = (r1 * m22 - r2 * m12) / det
                E = (r2 * m11 - r1 * m12) / det
                AE[i, j] = A, E
                chi[i, j] = 2 * (yy - (A * r1 + E * r2))
        return chi, AE


def main(datadir, spec, cameras='1,2,3,4', dt='0'):
    items = joint.parse_sectors(spec)
    cams = tuple(int(c) for c in cameras.split(','))
    dt = float(dt)
    terms = {}
    for c in cams:
        parts = []
        for sec, prod in items:
            try:
                ch = joint.Chunk(datadir, sec, prod, c, 'red')
            except FileNotFoundError:
                continue
            parts.append((ch.f, ch.cadence, ch.X, ch.Y, ch.eps, ch.u))
        terms[c] = Terms(parts, joint.CAM_LAT[c], dt)
        print(f'camera {c}: {len(terms[c].eps)} windows')
    res = {c: terms[c].solve() for c in cams}
    tot = sum(res[c][0] for c in cams)
    i, j = np.unravel_index(np.argmin(tot), tot.shape)
    d = tot - tot.min()
    print(f'\nshared best: alpha = {ALPHAS[i]:.1f}, v = {VS[j]:.2f}  (dt fixed {dt:+.0f} s)')
    print('profile delta chi^2 (rows alpha, columns v):')
    print('        ' + ' '.join(f'{v:6.2f}' for v in VS))
    for a, row in zip(ALPHAS, d):
        print(f'  {a:4.1f}  ' + ' '.join(f'{x:6.1f}' for x in row))
    pa, pv = d.min(1), d.min(0)
    print(f'alpha 68% (profile): {ALPHAS[pa <= 1].min():.1f} .. {ALPHAS[pa <= 1].max():.1f};  '
          f'v 68%: {VS[pv <= 1].min():.2f} .. {VS[pv <= 1].max():.2f}')
    print(f'Kelsall point (alpha 1.4, v 1.0): delta chi^2 = '
          f'{d[np.argmin(abs(ALPHAS - 1.4)), np.argmin(abs(VS - 1.0))]:.1f}')
    for c in cams:
        A, E = res[c][1][i, j]
        print(f'  camera {c}: A = {A:.2f}, E = {E:+.2f}, '
              f'own best alpha/v: {ALPHAS[np.unravel_index(np.argmin(res[c][0]), tot.shape)[0]]:.1f}/'
              f'{VS[np.unravel_index(np.argmin(res[c][0]), tot.shape)[1]]:.2f}')

    rng = np.random.default_rng(3)
    bs = []
    for _ in range(NBOOT):
        t = 0
        for c in cams:
            t = t + terms[c].solve(joint.block_weights(len(terms[c].eps), rng))[0]
        bi, bj = np.unravel_index(np.argmin(t), t.shape)
        bs.append((ALPHAS[bi], VS[bj]))
    bs = np.array(bs)
    print(f'bootstrap 68%: alpha {np.percentile(bs[:, 0], 16):.1f} .. {np.percentile(bs[:, 0], 84):.1f}; '
          f'v {np.percentile(bs[:, 1], 16):.2f} .. {np.percentile(bs[:, 1], 84):.2f}')


if __name__ == '__main__':
    main(*sys.argv[1:])
