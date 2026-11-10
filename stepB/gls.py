"""Four-camera joint fit with the cameras' noise cross-covariance (GLS).

The low-frequency background noise (0.1-0.5 mHz) is common to the four
cameras (coherence 0.2-0.75 between camera means; pointing jitter
explains ~1%), i.e. a spacecraft-wide common mode. Subtracting one
camera from another would also remove the common part of the echo; a
joint generalised-least-squares fit with the measured 4x4 noise
cross-spectrum is unbiased and gains wherever the echo's pattern across
the cameras differs from the noise pattern.

Per (sector, product): windows common to all four cameras; per window w
and frequency f the 4-vector Y = X * D theta + noise, noise covariance
  Sigma_w(f) = diag(sqrt s_w) S(f) diag(sqrt s_w),
S(f) the chunk-average cross-spectrum (smoothed over 9 frequency bins),
s_w the per-window, per-camera scale (as joint.noise_weights). With S
diagonal this is exactly the independent per-camera fit, which is run on
the same windows for comparison.

Models: (a) A_i H_i(alpha) + E_i eps_i per camera, alpha fixed;
(b) non-negative delay bins (joint.BIN_EDGES) per camera + E_i.
Errors: block bootstrap over windows, and the scatter of A over null
shifts (VIRGO from unrelated dates), the honest error for (a).

usage: gls.py datadir [SECTORS] [alpha] [nnull]
"""

import sys
import numpy as np
from scipy.ndimage import uniform_filter1d
from scipy.optimize import nnls

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint

CAMS = (1, 2, 3, 4)
SPEC = '27-38,1-13_2min,17-23_2min'
NBOOT = 200


class Joint4:
    """Common windows of one (sector, product) for the four cameras."""

    def __init__(self, datadir, sec, prod):
        chs = [joint.Chunk(datadir, sec, prod, c, 'red') for c in CAMS]
        common = sorted(set.intersection(*[set(c.starts.tolist()) for c in chs]))
        rows = [np.searchsorted(c.starts, common) for c in chs]
        self.label = chs[0].label
        self.starts = np.array(common)
        m = (chs[0].f >= joint.F_LO) & (chs[0].f <= joint.F_HI)
        self.f = chs[0].f[m]
        self.cadence = chs[0].cadence
        self.X = chs[0].X[rows[0]][:, m]
        self.Y = np.stack([c.Y[r][:, m] for c, r in zip(chs, rows)], 1)       # (w, 4, f)
        self.eps = np.stack([c.eps[r] for c, r in zip(chs, rows)], 1)          # (w, 4)
        self.u = [c.u for c in chs]
        self.raw = chs[0].raw
        self.win = chs[0].raw['win']
        # Noise cross-spectrum and per-window scales.
        Yt = self.Y.transpose(0, 2, 1)                                          # (w, f, 4)
        S = np.einsum('wfi,wfj->fij', Yt, Yt.conj()) / len(Yt)
        S = uniform_filter1d(S.real, 9, axis=0) + 1j * uniform_filter1d(S.imag, 9, axis=0)
        P = np.abs(Yt) ** 2 / np.einsum('fii->fi', S).real[None]
        s = np.median(P, axis=1)
        self.s = s / np.median(s, axis=0)
        self.Sinv = {'gls': np.linalg.inv(S),
                     'diag': np.linalg.inv(np.einsum('fij,ij->fij', S, np.eye(4)))}

    def null_X(self, sh):
        r = self.raw
        i = r['off'] + sh * r['per_day']
        if i < 0 or i + r['n'] > len(r['x_all']):
            return None
        Xn = joint.x_at(r['x_all'][i:i + r['n']], self.starts, self.win)
        m = (np.fft.rfftfreq(self.win, self.cadence) >= joint.F_LO) & \
            (np.fft.rfftfreq(self.win, self.cadence) <= joint.F_HI)
        return Xn[:, m]

    def design(self, model, alpha):
        """Fixed part of D: (f, 4, P); E columns are added per window."""
        nf = len(self.f)
        if model == 'param':
            D = np.zeros((nf, 4, 8), complex)
            for i, u in enumerate(self.u):
                D[:, i, i] = joint.kernel_h(self.f, u, alpha, key=self.cadence)[0]
            return D
        nb = len(joint.BIN_EDGES) - 1
        D = np.zeros((nf, 4, 4 * nb + 4), complex)
        fm = self.f
        for b, (a, c) in enumerate(zip(joint.BIN_EDGES[:-1], joint.BIN_EDGES[1:])):
            B = (np.exp(-2j * np.pi * fm * a) - np.exp(-2j * np.pi * fm * c)) / (2j * np.pi * fm * (c - a))
            for i in range(4):
                D[:, i, i * nb + b] = B
        return D

    def normal(self, kind, D0, X=None):
        """Per-window normal equations M_w (w, P, P), v_w (w, P)."""
        X = self.X if X is None else X
        ok = np.isfinite(X).all(1)
        X, Y, eps, sc = X[ok], self.Y[ok], self.eps[ok], self.s[ok]
        P = D0.shape[2]
        D = np.broadcast_to(D0, (len(X),) + D0.shape).copy()                   # (w, f, 4, P)
        for i in range(4):
            D[:, :, i, P - 4 + i] = eps[:, i][:, None]
        isq = 1 / np.sqrt(sc)                                                   # (w, 4)
        Si = self.Sinv[kind][None] * isq[:, None, :, None] * isq[:, None, None, :]
        T = np.einsum('wfij,wfjp->wfip', Si, D)
        ax2 = np.abs(X) ** 2
        M = np.einsum('wf,wfiq,wfip->wqp', ax2, D.conj(), T).real
        SY = np.einsum('wfij,wjf->wfi', Si, Y)
        v = np.einsum('wfiq,wfi,wf->wq', D.conj(), SY, X.conj()).real
        return M, v


def solve(M, v, nonneg=False):
    if not nonneg:
        return np.linalg.solve(M, v)
    R = np.linalg.cholesky(M).T
    z = np.linalg.solve(R.T, v)
    return nnls(R, z)[0]


def boot_weights(nw, rng):
    return joint.block_weights(nw, rng)


def main(datadir, spec=SPEC, alpha='1.4', nnull='60'):
    alpha, nnull = float(alpha), int(nnull)
    rng = np.random.default_rng(41)
    js = []
    for sec, prod in joint.parse_sectors(spec):
        try:
            js.append(Joint4(datadir, sec, prod))
        except FileNotFoundError:
            continue
    nw = sum(len(j.starts) for j in js)
    print(f'GLS four-camera fit, {spec}: {len(js)} sector products, {nw} common windows, alpha {alpha}, dt 0')
    out = {}
    for kind in ('diag', 'gls'):
        Mp, vp, Mb, vb = [], [], [], []
        for j in js:
            M, v = j.normal(kind, j.design('param', alpha)); Mp.append(M); vp.append(v)
            M, v = j.normal(kind, j.design('bins', alpha)); Mb.append(M); vb.append(v)
        Mp, vp, Mb, vb = (np.concatenate(a) for a in (Mp, vp, Mb, vb))
        th = solve(Mp.sum(0), vp.sum(0))
        bs = np.array([solve(np.tensordot(w, Mp, 1), w @ vp) for w in
                       (boot_weights(len(vp), rng) for _ in range(NBOOT))])
        bins = solve(Mb.sum(0), vb.sum(0), nonneg=True)
        bb = np.array([solve(np.tensordot(w, Mb, 1), w @ vb, nonneg=True) for w in
                       (boot_weights(len(vb), rng) for _ in range(NBOOT))])
        # Null shifts.
        nulls = []
        for sh in joint.NULL_SHIFTS[::max(1, len(joint.NULL_SHIFTS) // nnull)][:nnull]:
            Ms, vs = [], []
            for j in js:
                Xn = j.null_X(sh)
                if Xn is None:
                    continue
                M, v = j.normal(kind, j.design('param', alpha), Xn); Ms.append(M); vs.append(v)
            if len(Ms) >= 0.7 * len(js):
                Ms, vs = np.concatenate(Ms), np.concatenate(vs)
                nulls.append(solve(Ms.sum(0), vs.sum(0)))
        nulls = np.array(nulls)
        out[kind] = (th, bs, bins, bb, nulls)

    nb = len(joint.BIN_EDGES) - 1
    print('\n(a) A at fixed alpha (per unit floor): estimate, bootstrap sigma, null-shift sigma, A/null sigma')
    for i, c in enumerate(CAMS):
        line = []
        for kind in ('diag', 'gls'):
            th, bs, _, _, nulls = out[kind]
            sn = nulls[:, i].std()
            line.append(f'{kind:4s} A {th[i]:.3f} ± {bs[:, i].std():.3f} (null σ {sn:.3f}, '
                        f'{th[i] / sn:5.1f}σ)  E {th[4 + i]:+.2f}')
        ratio = out['diag'][4][:, i].std() / out['gls'][4][:, i].std()
        print(f'  cam {c}: ' + '   '.join(line) + f'   null-σ ratio diag/gls {ratio:.2f}')
    print('\n(b) non-negative delay bins (per unit floor; bootstrap 68% half-width)')
    print('  bin [s]  ' + ''.join(f'{a}-{b}'.rjust(16) for a, b in zip(joint.BIN_EDGES[:-1], joint.BIN_EDGES[1:]))
          + '        E')
    for i, c in enumerate(CAMS):
        for kind in ('diag', 'gls'):
            _, _, bins, bb, _ = out[kind]
            lo, hi = np.percentile(bb, [16, 84], axis=0)
            cells = [f'{bins[i * nb + b]:.3f}±{(hi[i * nb + b] - lo[i * nb + b]) / 2:.3f}'.rjust(16)
                     for b in range(nb)]
            e = 4 * nb + i
            print(f'  cam {c} {kind:4s}' + ''.join(cells) + f'   {bins[e]:.2f}±{(hi[e] - lo[e]) / 2:.2f}')


if __name__ == '__main__':
    main(*sys.argv[1:])
