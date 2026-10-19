"""Joint estimator: dust kernel shape, Earthshine and timing, fitted together.

Per 6-h window k and frequency f:

    Y_k(f) = X_k(f) * [ A * H_alpha(f) * exp(-2 pi i f dt) + E * eps_k ] + noise

  X        VIRGO fractional variation (emission-time base, aligned.py)
  Y        TESS background fluctuation in units of the sector's quiet floor:
           frac * level / floor
  H_alpha  model kernel with density ~ r^-alpha (vertical profile as Kelsall)
  A        zodiacal amplitude per unit floor (~1.15 x zodiacal fraction of
           the floor, the 1.15 being TESS band vs 862 nm)
  E        Earthshine coefficient; eps_k = (window level - floor) / floor;
           zero-delay term
  dt       timing offset, shared by all cameras

Windows from different sectors and products (20-s on the VIRGO minute
grid, 2-min on the TESS 120-s grid) have their own frequency grids; the
fit only needs per-window sums over frequency, so they are pooled at that
level. alpha and dt are on a grid; A and E are linear. Errors: block
bootstrap over windows; the significance of A from null shifts (VIRGO
from unrelated dates). Also a non-parametric fit: non-negative log-spaced
delay bins plus E, at the best dt.

usage: joint.py datadir SECTORS [red|tsi] [cameras, e.g. 1,2,3,4]
  SECTORS: comma list of items N, N-M, optionally with a product suffix,
  e.g. 27-38,1-26_2min
"""

import os, re, sys
import numpy as np
from scipy.ndimage import uniform_filter1d
from scipy.optimize import nnls

sys.path.insert(0, __file__.rsplit('/', 2)[0])
sys.path.insert(0, __file__.rsplit('/', 1)[0])
import sensitivity as sm
import geometry
from aligned import load

WINDOW_S = 6 * 3600
F_LO, F_HI = 0.1e-3, 8.3e-3
CAM_LAT = {1: 18.0, 2: 42.0, 3: 66.0, 4: 90.0}
ALPHAS = np.round(np.arange(0.4, 4.01, 0.2), 2)
DTS = np.arange(-300, 301, 10)
BIN_EDGES = [0, 60, 180, 420, 900, 2000, 4000]
NBOOT = 300
BLOCK = 4                      # windows per bootstrap block (50% overlap)
NULL_SHIFTS = list(range(-400, -29, 3)) + list(range(30, 400, 3))


def noise_weights(Y):
    """Inverse noise power per (window, frequency) that does not depend on
    the particular window's reference signal: the chunk-average spectrum
    (smoothed over frequency) times one scale factor per window (median
    ratio over frequency, which tracks the window's background level).
    Per-window |Y|^2 weights correlate with |X|^2 through the echo and bias
    A low by ~30% and the shape toward compact (injection.py)."""
    P = np.abs(Y) ** 2
    S = uniform_filter1d(P.mean(0), 9)
    s = np.median(P / S, axis=1)
    s /= np.median(s)
    return 1 / (s[:, None] * S[None, :])


def parse_sectors(spec):
    out = []
    for item in spec.split(','):
        m = re.fullmatch(r'(\d+)(?:-(\d+))?(_\w+)?', item.strip())
        a, b, suffix = int(m[1]), int(m[2] or m[1]), m[3] or ''
        out += [(s, suffix) for s in range(a, b + 1)]
    return out


def windows(x, y, lev, win):
    """FFTs of complete windows (50% overlap), mean level, start indices."""
    w = np.hanning(win)
    X, Y, L, S = [], [], [], []
    for i in range(0, len(x) - win + 1, win // 2):
        a, b, l = x[i:i + win], y[i:i + win], lev[i:i + win]
        if np.isfinite(a).all() and np.isfinite(b).all():
            X.append(np.fft.rfft((a - a.mean()) * w))
            Y.append(np.fft.rfft((b - b.mean()) * w))
            L.append(np.nanmean(l))
            S.append(i)
    return np.array(X), np.array(Y), np.array(L), np.array(S)


def x_at(x, starts, win):
    """FFTs of x at given window starts; NaN rows where incomplete."""
    w = np.hanning(win)
    out = np.full((len(starts), win // 2 + 1), np.nan, complex)
    for k, i in enumerate(starts):
        a = x[i:i + win]
        if len(a) == win and np.isfinite(a).all():
            out[k] = np.fft.rfft((a - a.mean()) * w)
    return out


class Chunk:
    """Windows of one (sector, product, camera), on its own frequency grid."""

    def __init__(self, datadir, sector, product, cam, ref):
        a = load(datadir, sector, cam, ref, product=product)
        self.label = f'{sector}{product}'
        self.cadence = a['cadence']
        win = int(round(WINDOW_S / self.cadence))
        self.f = np.fft.rfftfreq(win, self.cadence)
        _, _, L0, _ = windows(a['x'], a['y'], a['level'], win)
        self.floor = np.percentile(L0, 5)
        ynorm = a['y'] * a['level'] / self.floor
        self.X, self.Y, L, S = windows(a['x'], ynorm, a['level'], win)
        self.eps = (L - self.floor) / self.floor
        # Raw series kept for time-domain injection tests (injection.py).
        self.raw = dict(x_all=a['x_all'], off=a['off'], per_day=a['per_day'], n=a['n'],
                        y_norm=ynorm, level=a['level'], win=win)
        self.starts = S
        self.u = sightline(datadir, sector, cam)

    def null_X(self, sh):
        """Window FFTs of VIRGO shifted by sh days (computed on demand; storing
        them for every shift and chunk does not fit in memory)."""
        r = self.raw
        i = r['off'] + sh * r['per_day']
        if i < 0 or i + r['n'] > len(r['x_all']):
            return None
        return x_at(r['x_all'][i:i + r['n']], self.starts, r['win'])


_geometry = {}


def sightline(datadir, sector, cam):
    """Unit sightline vector of (sector, camera) from geometry.csv; the
    standard anti-sunward strip at CAM_LAT if the table is missing."""
    if datadir not in _geometry:
        p = os.path.join(datadir, 'geometry.csv')
        _geometry[datadir] = geometry.load_table(p) if os.path.exists(p) else {}
    u = _geometry[datadir].get((int(sector), int(cam)))
    if u is None:
        b = np.radians(CAM_LAT[cam])
        u = np.array([np.cos(b), 0.0, np.sin(b)])
    return u


_kernel_cache = {}


def kernel_h(f, lat, alpha, key=None):
    """Kernel transfer function. lat: a sightline unit vector (geometry.py)
    or an ecliptic latitude for the anti-sunward strip."""
    u = np.asarray(lat, float)
    if u.ndim == 0:
        b = np.radians(float(lat))
        u = np.array([np.cos(b), 0.0, np.sin(b)])
    ck = (tuple(np.round(u, 2)), alpha, key)
    if key is not None and ck in _kernel_cache:
        return _kernel_cache[ck]
    tau, w = geometry.kernel(u, alpha)
    h = np.zeros(len(f), complex)
    nz = f > 0
    h[nz] = sm.transfer(f[nz], tau, w)
    out = (h, tau, w)
    if key is not None:
        _kernel_cache[ck] = out
    return out


class Terms:
    """Per-window sums over frequency, pooled over chunks, so that the
    linear solve and bootstrap re-weighting only need sums over windows."""

    def __init__(self, parts, lat, alphas=ALPHAS):
        """parts: list of (f, cadence, X, Y, eps) with matching rows."""
        self.alphas = alphas
        acc = {k: [] for k in ('eps', 'R', 'V', 'YY')}
        P = {al: [] for al in alphas}
        Q = {al: [] for al in alphas}
        U = {al: [] for al in alphas}
        for f, cad, X, Y, eps, u in (p if len(p) == 6 else (*p, lat) for p in parts):
            m = (f >= F_LO) & (f <= F_HI)
            fm = f[m]
            Xm, Ym = X[:, m], Y[:, m]
            W = noise_weights(Ym)
            XX = W * np.abs(Xm) ** 2
            YX = W * Ym * Xm.conj()
            acc['eps'].append(eps)
            acc['R'].append(XX.sum(1))
            acc['V'].append(YX.real.sum(1))
            acc['YY'].append((W * np.abs(Ym) ** 2).sum(1))
            ph = np.exp(-2j * np.pi * np.outer(DTS, fm))
            for al in alphas:
                h = kernel_h(fm, u, al, key=cad)[0]
                hd = h[None, :] * ph
                P[al].append(XX @ np.abs(h) ** 2)
                Q[al].append(XX @ hd.real.T)
                U[al].append((YX @ hd.conj().T).real)
        for k, v in acc.items():
            setattr(self, k, np.concatenate(v))
        self.P = {al: np.concatenate(v) for al, v in P.items()}
        self.Q = {al: np.concatenate(v) for al, v in Q.items()}
        self.U = {al: np.concatenate(v) for al, v in U.items()}

    def solve(self, wk=None):
        """chi^2 grid (alpha, dt) and the linear (A, E) at each point."""
        wk = np.ones(len(self.eps)) if wk is None else wk
        e = self.eps
        m22 = np.sum(wk * e * e * self.R)
        r2 = np.sum(wk * e * self.V)
        chi = np.empty((len(self.alphas), len(DTS)))
        AE = np.empty((len(self.alphas), len(DTS), 2))
        for i, al in enumerate(self.alphas):
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


def bin_rows(chunks, dt):
    """Design matrix for the non-negative log-binned kernel + Earthshine,
    rows = (window, frequency), real then imaginary; also the window index
    of each row for the bootstrap."""
    Ds, ys, ks = [], [], []
    k0 = 0
    for ch in chunks:
        m = (ch.f >= F_LO) & (ch.f <= F_HI)
        fm = ch.f[m]
        X, Y = ch.X[:, m], ch.Y[:, m]
        sw = np.sqrt(noise_weights(Y))
        ph = np.exp(-2j * np.pi * fm * dt)
        cols = []
        for a, b in zip(BIN_EDGES[:-1], BIN_EDGES[1:]):
            B = (np.exp(-2j * np.pi * fm * a) - np.exp(-2j * np.pi * fm * b)) / (2j * np.pi * fm * (b - a))
            cols.append((sw * X * B * ph).ravel())
        cols.append((sw * X * ch.eps[:, None]).ravel())
        D = np.array(cols).T
        Ds += [D.real, D.imag]
        y = (sw * Y).ravel()
        ys += [y.real, y.imag]
        kk = np.repeat(np.arange(len(ch.eps)) + k0, len(fm))
        ks += [kk, kk]
        k0 += len(ch.eps)
    return np.vstack(Ds), np.concatenate(ys), np.concatenate(ks)


def main(datadir, spec, ref='red', cameras='1,2'):
    items = parse_sectors(spec)
    rng = np.random.default_rng(7)
    cams = tuple(int(c) for c in cameras.split(','))
    chunks, terms, grids = {}, {}, {}
    for cam in cams:
        chunks[cam] = []
        for sec, prod in items:
            try:
                chunks[cam].append(Chunk(datadir, sec, prod, cam, ref))
            except FileNotFoundError:
                pass
        cs = chunks[cam]
        terms[cam] = Terms([(c.f, c.cadence, c.X, c.Y, c.eps, c.u) for c in cs], CAM_LAT[cam])
        grids[cam] = terms[cam].solve()
        print(f'camera {cam}: {len(terms[cam].eps)} windows from {len(cs)} sector products '
              f'({",".join(c.label for c in cs)})')

    def joint_best(chis):
        tot = sum(c.min(0) for c in chis)
        j = int(np.argmin(tot))
        return j, [int(np.argmin(c[:, j])) for c in chis]

    j, ia = joint_best([grids[c][0] for c in cams])
    boot = {c: [] for c in cams}
    boot_dt = []
    for _ in range(NBOOT):
        res = {c: terms[c].solve(block_weights(len(terms[c].eps), rng)) for c in cams}
        jb, iab = joint_best([res[c][0] for c in cams])
        boot_dt.append(DTS[jb])
        for c, i in zip(cams, iab):
            boot[c].append((ALPHAS[i], *res[c][1][i, jb]))
    lo, hi = np.percentile(boot_dt, [16, 84])
    tot = sum(grids[c][0].min(0) for c in cams)
    ok = tot - tot.min() <= 1
    print(f'\nshared timing offset dt = {DTS[j]:+d} s  (profile 68%: {DTS[ok].min():+d} .. '
          f'{DTS[ok].max():+d} s; bootstrap 68%: {lo:+.0f} .. {hi:+.0f} s)')

    print('\nper camera at the shared dt (A, E per unit quiet floor; expected A ~1.15 x ZL fraction):')
    print('  cam  lat  alpha best [boot 68%] [dchi2=1]   A [boot 68%]         E [boot 68%]        '
          'A/null_sigma   A at alpha=1.4')
    for c, i in zip(cams, ia):
        chi, AE = grids[c]
        A, E = AE[i, j]
        bt = np.array(boot[c])
        q = lambda col: np.percentile(bt[:, col], [16, 84])
        # Null: A at the best (alpha, dt) with VIRGO from unrelated dates.
        nA = []
        for sh in NULL_SHIFTS:
            parts = []
            for ch in chunks[c]:
                Xn = ch.null_X(sh)
                if Xn is None:
                    continue
                okr = np.isfinite(Xn[:, 1])
                if okr.mean() < 0.7:
                    continue
                parts.append((ch.f, ch.cadence, Xn[okr], ch.Y[okr], ch.eps[okr], ch.u))
            if len(parts) < 0.7 * len(chunks[c]):
                continue
            tn = Terms(parts, CAM_LAT[c], alphas=[ALPHAS[i]])
            nA.append(tn.solve()[1][0, j, 0])
        nsig = np.std(nA)
        chi_a = chi[:, j]
        oka = chi_a - chi_a.min() <= 1
        k14 = int(np.argmin(abs(ALPHAS - 1.4)))
        print(f'  {c}   {CAM_LAT[c]:4.0f}  {ALPHAS[i]:.1f} [{q(0)[0]:.1f}, {q(0)[1]:.1f}] '
              f'[{ALPHAS[oka].min():.1f}, {ALPHAS[oka].max():.1f}]'
              f'    {A:5.2f} [{q(1)[0]:.2f}, {q(1)[1]:.2f}]'
              f'    {E:+.2f} [{q(2)[0]:+.2f}, {q(2)[1]:+.2f}]'
              f'    {A / nsig:5.1f} ({len(nA)} nulls)'
              f'    {AE[k14, j, 0]:.2f} (dchi2 {chi[k14, j] - chi[i, j]:.1f})')

    print('\nprofile chi^2 vs alpha at the shared dt, relative to best:')
    for c, i in zip(cams, ia):
        chi = grids[c][0][:, j]
        print(f'  cam {c}: ' + ' '.join(f'{al:.1f}:{v - chi.min():.1f}' for al, v in zip(ALPHAS, chi)
                                     if al in (0.6, 1.0, 1.4, 1.8, 2.2, 2.6, 3.0, 3.6)))

    print(f'\nnon-negative log-binned kernel at dt = {DTS[j]:+d} s (per unit floor, per bin; '
          f'bootstrap 68%):')
    print('  bin [s]        ' + '  '.join(f'{a}-{b}'.rjust(16) for a, b in
                                         zip(BIN_EDGES[:-1], BIN_EDGES[1:])) + '   Earthshine E')
    for c, i in zip(cams, ia):
        D, y, krow = bin_rows(chunks[c], DTS[j])
        sol, _ = nnls(D, y)
        nk = len(terms[c].eps)
        bs = []
        for _ in range(100):
            rw = np.sqrt(block_weights(nk, rng)[krow])
            bs.append(nnls(D * rw[:, None], y * rw)[0])
        bs = np.array(bs)
        lo_, hi_ = np.percentile(bs, [16, 84], axis=0)
        print(f'  cam {c} fit    ' + '  '.join(f'{v:5.3f} [{l:.2f},{h:.2f}]' for v, l, h in
                                          zip(sol[:-1], lo_[:-1], hi_[:-1])) +
              f'   {sol[-1]:.2f} [{lo_[-1]:.2f},{hi_[-1]:.2f}]')
        area = bs[:, :-1].sum(1)
        _, tau, w = kernel_h(np.array([1e-3]), chunks[c][0].u, 1.34)
        model = [1.15 * w[(tau >= a) & (tau < b)].sum() for a, b in zip(BIN_EDGES[:-1], BIN_EDGES[1:])]
        print(f'  cam {c} model  ' + '  '.join(f'{v:5.3f}'.ljust(16) for v in model) +
              '   (alpha 1.34, A 1.15)')
        print(f'  cam {c} total area {sol[:-1].sum():.2f} [{np.percentile(area, 16):.2f}, '
              f'{np.percentile(area, 84):.2f}]  (expected ~1.15 x ZL fraction)')


if __name__ == '__main__':
    main(*sys.argv[1:])
