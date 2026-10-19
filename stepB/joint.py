"""Joint estimator: dust kernel shape, Earthshine and timing, fitted together.

Per 6-h window k and frequency f, in absolute units (e-/s/px):

    Y_k(f) = X_k(f) * [ A * H_alpha(f) * exp(-2 pi i f dt) + E * eps_k ] + noise

  X        VIRGO fractional variation (emission-time base, aligned.py)
  Y        TESS background fluctuation, frac * level
  H_alpha  model kernel with density ~ r^-alpha (vertical profile as Kelsall)
  A        zodiacal amplitude [e-/s/px per unit solar fraction]
  E        Earthshine coefficient, per e-/s/px of level excess eps_k
           (eps_k = window mean level - camera floor); zero-delay term
  dt       timing offset, shared by all cameras

alpha and dt are on a grid; A and E are linear. Windows are weighted by
1 / smoothed |Y_k(f)|^2. Errors: block bootstrap over windows; the
significance of A from null shifts (VIRGO from unrelated dates).

Also a non-parametric fit: non-negative log-spaced delay bins plus E,
at the best dt.

usage: joint.py datadir sector[,sector...] [red|tsi] [cameras, e.g. 1,2,3,4]
"""

import sys
import numpy as np
from scipy.ndimage import uniform_filter1d
from scipy.optimize import nnls

sys.path.insert(0, __file__.rsplit('/', 2)[0])
sys.path.insert(0, __file__.rsplit('/', 1)[0])
import sensitivity as sm
from aligned import load

WIN = 360
F_LO, F_HI = 0.1e-3, 8.3e-3
CAM_LAT = {1: 18.0, 2: 42.0, 3: 66.0, 4: 90.0}
ALPHAS = np.round(np.arange(0.4, 4.01, 0.2), 2)
DTS = np.arange(-300, 301, 10)
BIN_EDGES = [0, 60, 180, 420, 900, 2000, 4000]
NBOOT = 300
BLOCK = 4                      # windows per bootstrap block (50% overlap)
NULL_SHIFTS = list(range(-400, -29, 3)) + list(range(30, 400, 3))


def windows(x, y, lev):
    """FFTs of complete 6-h windows (50% overlap), their mean level and
    start indices."""
    w = np.hanning(WIN)
    X, Y, L, S = [], [], [], []
    for i in range(0, len(x) - WIN + 1, WIN // 2):
        a, b, l = x[i:i + WIN], y[i:i + WIN], lev[i:i + WIN]
        if np.isfinite(a).all() and np.isfinite(b).all():
            X.append(np.fft.rfft((a - a.mean()) * w))
            Y.append(np.fft.rfft((b - b.mean()) * w))
            L.append(np.nanmean(l))
            S.append(i)
    return np.array(X), np.array(Y), np.array(L), np.array(S)


def x_at(x, starts):
    """FFTs of x at given window starts; NaN rows where incomplete."""
    w = np.hanning(WIN)
    out = np.full((len(starts), WIN // 2 + 1), np.nan, complex)
    for k, i in enumerate(starts):
        a = x[i:i + WIN]
        if len(a) == WIN and np.isfinite(a).all():
            out[k] = np.fft.rfft((a - a.mean()) * w)
    return out


def camera_data(datadir, sectors, cam, ref):
    """Windows from all sectors for one camera, plus null X per shift."""
    Xs, Ys, eps, floors, nulls = [], [], [], [], {s: [] for s in NULL_SHIFTS}
    for sec in sectors:
        a = load(datadir, sec, cam, ref)
        yabs = a['y'] * a['level']
        X, Y, L, S = windows(a['x'], yabs, a['level'])
        floor = np.percentile(L, 5)
        Xs.append(X), Ys.append(Y), eps.append(L - floor), floors.append(np.full(len(L), floor))
        for sh in NULL_SHIFTS:
            i = a['off'] + sh * 1440
            if 0 <= i and i + a['n'] <= len(a['x_all']):
                nulls[sh].append(x_at(a['x_all'][i:i + a['n']], S))
            else:
                nulls[sh].append(np.full((len(S), WIN // 2 + 1), np.nan, complex))
    # Keep shifts where at least 70% of the windows are complete.
    nulls = {s: np.concatenate(v) for s, v in nulls.items()}
    nulls = {s: v for s, v in nulls.items() if np.isfinite(v[:, 1]).mean() > 0.7}
    return (np.concatenate(Xs), np.concatenate(Ys), np.concatenate(eps),
            np.concatenate(floors), nulls)


def kernel_h(f, lat, alpha):
    s, tau, w = sm.kernel(lat)
    b = np.radians(lat)
    r = np.hypot(1 + s * np.cos(b), s * np.sin(b))
    w = w * r ** -(alpha - 1.34)
    w /= w.sum()
    h = np.zeros(len(f), complex)
    h[1:] = sm.transfer(f[1:], tau, w)
    return h, tau, w


class Terms:
    """Per-window sums over frequency, so that bootstrap re-weighting of
    windows only needs cheap sums over k."""

    def __init__(self, f, X, Y, eps, lat):
        m = (f >= F_LO) & (f <= F_HI)
        self.f = f[m]
        X, Y = X[:, m], Y[:, m]
        W = 1 / uniform_filter1d(np.abs(Y) ** 2, 9, axis=1)
        self.eps = eps
        self.XX = W * np.abs(X) ** 2                     # (k, f)
        self.YX = W * Y * X.conj()                       # (k, f)
        self.R = self.XX.sum(1)
        self.V = self.YX.real.sum(1)
        self.YY = (W * np.abs(Y) ** 2).sum(1)
        self.P, self.Q, self.U = {}, {}, {}
        ph = np.exp(-2j * np.pi * np.outer(DTS, self.f))  # (dt, f)
        for al in ALPHAS:
            h = kernel_h(self.f, lat, al)[0]
            hd = h[None, :] * ph                          # (dt, f)
            self.P[al] = self.XX @ np.abs(h) ** 2         # (k,)
            self.Q[al] = self.XX @ hd.real.T              # (k, dt)
            self.U[al] = (self.YX @ hd.conj().T).real     # (k, dt)

    def solve(self, wk=None):
        """chi^2 grid (alpha, dt) and the linear (A, E) at each point."""
        wk = np.ones(len(self.eps)) if wk is None else wk
        e = self.eps
        m22 = np.sum(wk * e * e * self.R)
        r2 = np.sum(wk * e * self.V)
        chi = np.empty((len(ALPHAS), len(DTS)))
        AE = np.empty((len(ALPHAS), len(DTS), 2))
        for i, al in enumerate(ALPHAS):
            m11 = np.sum(wk * self.P[al])
            m12 = (wk * e) @ self.Q[al]
            r1 = wk @ self.U[al]
            det = m11 * m22 - m12 ** 2
            A = (r1 * m22 - r2 * m12) / det
            E = (r2 * m11 - r1 * m12) / det
            AE[i, :, 0], AE[i, :, 1] = A, E
            # x2: -2 ln L for circular complex Gaussian noise of variance S is 2|r|^2/S.
            chi[i] = 2 * (np.sum(wk * self.YY) - (A * r1 + E * r2))
        return chi, AE


def block_weights(n, rng):
    nb = int(np.ceil(n / BLOCK))
    pick = rng.integers(0, nb, nb)
    w = np.zeros(n)
    for b in pick:
        w[b * BLOCK:(b + 1) * BLOCK] += 1
    return w


def fit_bins(f, X, Y, eps, dt):
    """Non-negative log-binned kernel + non-negative Earthshine, at fixed dt."""
    m = (f >= F_LO) & (f <= F_HI)
    fm = f[m]
    X, Y = X[:, m], Y[:, m]
    sw = 1 / np.sqrt(uniform_filter1d(np.abs(Y) ** 2, 9, axis=1))
    ph = np.exp(-2j * np.pi * fm * dt)
    cols = []
    for a, b in zip(BIN_EDGES[:-1], BIN_EDGES[1:]):
        B = (np.exp(-2j * np.pi * fm * a) - np.exp(-2j * np.pi * fm * b)) / (2j * np.pi * fm * (b - a))
        cols.append((sw * X * B * ph).ravel())
    cols.append((sw * X * eps[:, None]).ravel())
    D = np.array(cols).T
    D = np.vstack([D.real, D.imag])
    y = (sw * Y).ravel()
    y = np.concatenate([y.real, y.imag])
    return D, y


def main(datadir, sectors, ref='red', cameras='1,2'):
    sectors = [int(s) for s in sectors.split(',')]
    rng = np.random.default_rng(7)
    f = np.fft.rfftfreq(WIN, 60.0)
    cams = tuple(int(c) for c in cameras.split(','))
    data, terms, grids = {}, {}, {}
    for cam in cams:
        X, Y, eps, floor, nulls = camera_data(datadir, sectors, cam, ref)
        data[cam] = (X, Y, eps, floor, nulls)
        terms[cam] = Terms(f, X, Y, eps, CAM_LAT[cam])
        grids[cam] = terms[cam].solve()
        print(f'camera {cam}: {len(X)} windows from sectors {sectors}, {len(nulls)} null shifts, '
              f'level excess 0..{eps.max():.0f} e-/s/px')

    # Shared dt: sum over cameras of each camera's chi^2 minimised over alpha.
    def joint_best(chis):
        tot = sum(c.min(0) for c in chis)
        j = int(np.argmin(tot))
        return j, [int(np.argmin(c[:, j])) for c in chis]

    j, ia = joint_best([grids[c][0] for c in cams])
    # Bootstrap: same window resampling scheme per camera, independent draws.
    boot = {c: [] for c in cams}
    boot_dt = []
    for _ in range(NBOOT):
        res = {c: terms[c].solve(block_weights(len(terms[c].eps), rng)) for c in cams}
        jb, iab = joint_best([res[c][0] for c in cams])
        boot_dt.append(DTS[jb])
        for c, i in zip(cams, iab):
            boot[c].append((ALPHAS[i], *res[c][1][i, jb]))
    lo, hi = np.percentile(boot_dt, [16, 84])
    # bootstrap interval and the profile shape.
    print(f'\nshared timing offset dt = {DTS[j]:+d} s  (bootstrap 68%: {lo:+.0f} .. {hi:+.0f} s)')

    print('\nper camera at the shared dt:')
    print('  cam  lat   alpha [68%]          A [e-/s/px] [68%]      E [per e-/s/px] [68%]   A/null_sigma')
    for c, i in zip(cams, ia):
        chi, AE = grids[c]
        A, E = AE[i, j]
        bt = np.array(boot[c])
        q = lambda col: np.percentile(bt[:, col], [16, 84])
        # Null: A at fixed (alpha, dt) with VIRGO from unrelated dates.
        X, Y, eps, floor, nulls = data[c]
        nA = []
        for sh, Xn in nulls.items():
            ok = np.isfinite(Xn[:, 1])
            Xn, Yn, en = Xn[ok], Y[ok], eps[ok]
            m = (f >= F_LO) & (f <= F_HI)
            W = 1 / uniform_filter1d(np.abs(Yn[:, m]) ** 2, 9, axis=1)
            h = kernel_h(f[m], CAM_LAT[c], ALPHAS[i])[0] * np.exp(-2j * np.pi * f[m] * DTS[j])
            XXn = W * np.abs(Xn[:, m]) ** 2
            YXn = W * Yn[:, m] * Xn[:, m].conj()
            m11 = np.sum(XXn @ np.abs(h) ** 2)
            m12 = np.sum(en * (XXn @ h.real))
            m22 = np.sum(en ** 2 * XXn.sum(1))
            r1 = np.sum((YXn @ h.conj()).real)
            r2 = np.sum(en * YXn.real.sum(1))
            nA.append((r1 * m22 - r2 * m12) / (m11 * m22 - m12 ** 2))
        nsig = np.std(nA)
        al_lo, al_hi = q(0)
        print(f'  {c}   {CAM_LAT[c]:4.0f}  {ALPHAS[i]:.1f} [{al_lo:.1f}, {al_hi:.1f}]'
              f'        {A:6.1f} [{q(1)[0]:.1f}, {q(1)[1]:.1f}]'
              f'        {E:+.2f} [{q(2)[0]:+.2f}, {q(2)[1]:+.2f}]          {A / nsig:5.1f}')

    tot = sum(grids[c][0].min(0) for c in cams)
    ok = tot - tot.min() <= 1
    print(f'shared dt profile-likelihood 68% (delta chi^2 = 1): {DTS[ok].min():+d} .. {DTS[ok].max():+d} s')
    print('\nexpected A if the quiet floor is all zodiacal light: floor x ~1.15 (TESS band vs 862 nm)')
    for c, i in zip(cams, ia):
        fl = np.median(data[c][3])
        k14 = int(np.argmin(abs(ALPHAS - 1.4)))
        A14, E14 = grids[c][1][k14, j]
        dchi = grids[c][0][k14, j] - grids[c][0][i, j]
        chi_a = grids[c][0][:, j]
        oka = chi_a - chi_a.min() <= 1
        print(f'  cam {c}: floor {fl:.0f} -> expected A ~{1.15 * fl:.0f};  '
              f'alpha 68% (delta chi^2=1) {ALPHAS[oka].min():.1f}..{ALPHAS[oka].max():.1f};  '
              f'at alpha=1.4: A={A14:.0f}, E={E14:+.2f}, delta chi^2={dchi:.1f}')
    print('\nprofile chi^2 vs alpha (min over A, E at the shared dt), relative to best:')
    for c, i in zip(cams, ia):
        chi = grids[c][0][:, j]
        print(f'  cam {c}: ' + ' '.join(f'{al:.1f}:{v - chi.min():.1f}' for al, v in zip(ALPHAS, chi)
                                     if al in (0.6, 1.0, 1.4, 1.8, 2.2, 2.6, 3.0, 3.6)))

    print(f'\nnon-negative log-binned kernel at dt = {DTS[j]:+d} s '
          f'(units: e-/s/px per unit solar fraction, per bin; bootstrap 68%):')
    print('  bin [s]       ' + '  '.join(f'{a}-{b}'.rjust(14) for a, b in zip(BIN_EDGES[:-1], BIN_EDGES[1:]))
          + '   Earthshine E')
    for c, i in zip(cams, ia):
        X, Y, eps, floor, _ = data[c]
        D, y = fit_bins(f, X, Y, eps, DTS[j])
        sol, _ = nnls(D, y)
        # Block bootstrap: rows are (window, freq) for real then imag.
        nk = len(eps)
        nf = D.shape[0] // 2 // nk
        bs = []
        for _ in range(100):
            wk = block_weights(nk, rng)
            rw = np.sqrt(np.tile(np.repeat(wk, nf), 2))
            bs.append(nnls(D * rw[:, None], y * rw)[0])
        bs = np.array(bs)
        lo_, hi_ = np.percentile(bs, [16, 84], axis=0)
        _, tau, w = kernel_h(f, CAM_LAT[c], ALPHAS[i])
        A = grids[c][1][i, j, 0]
        model = [A * w[(tau >= a) & (tau < b)].sum() for a, b in zip(BIN_EDGES[:-1], BIN_EDGES[1:])]
        print(f'  cam {c} fit   ' + '  '.join(f'{v:5.0f} [{l:3.0f},{h:3.0f}]' for v, l, h in
                                         zip(sol[:-1], lo_[:-1], hi_[:-1])) +
              f'   {sol[-1]:.2f} [{lo_[-1]:.2f},{hi_[-1]:.2f}]')
        area = bs[:, :-1].sum(1)
        print(f'  cam {c} total area {sol[:-1].sum():.0f} [{np.percentile(area, 16):.0f}, '
              f'{np.percentile(area, 84):.0f}]  vs expected ~{1.15 * np.median(floor):.0f}')
        print(f'  cam {c} model ' + '  '.join(f'{v:5.0f}'.ljust(14) for v in model) +
              f'   (best parametric alpha={ALPHAS[i]:.1f}, A={A:.0f})')


if __name__ == '__main__':
    main(*sys.argv[1:])
