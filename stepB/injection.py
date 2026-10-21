"""Injection-recovery: synthetic echoes injected in the time domain into the
real TESS backgrounds, paired with VIRGO from unrelated dates (so the real
echo is absent and all real noise is kept), then run through the same joint
estimator. Gives bias and realistic scatter for dt, alpha, A, E and the
non-negative delay bins, and checks the per-window (multiplicative)
approximation for long delays.

Injected kernels (per unit floor, camera geometry):
  model     alpha = 1.34 (Kelsall-like), A = 0.7
  extended  alpha = 0.6,                 A = 0.7
  bump      model + 0.05 in a 300-s-wide bump at 1500 s

usage: injection.py datadir SECTORS [cameras] [ntrials]
"""

import sys
import numpy as np
from scipy.optimize import nnls

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint

joint.DTS = np.arange(-120, 121, 5)
A_INJ = 0.7


def kernel_samples(u, alpha, cadence, bump=0.0):
    """Causal kernel sampled per cadence step (weights summing to 1, plus
    an optional extra bump at 1350-1650 s), sightline u."""
    _, tau, w = joint.kernel_h(np.array([1e-3]), u, alpha)
    nb = int(4000 / cadence) + 2
    # Fractional delays: split each weight linearly between neighbouring lags.
    pos = np.minimum(tau / cadence, nb - 1.001)
    j = pos.astype(int)
    r = pos - j
    k = np.bincount(j, w * (1 - r), nb) + np.bincount(j + 1, w * r, nb)
    if bump:
        a, b = int(1350 / cadence), int(1650 / cadence)
        k[a:b + 1] += bump / (b - a + 1)
    return k


def true_bins(u, alpha, bump=0.0):
    _, tau, w = joint.kernel_h(np.array([1e-3]), u, alpha)
    out = np.array([w[(tau >= a) & (tau < b)].sum() for a, b in
                    zip(joint.BIN_EDGES[:-1], joint.BIN_EDGES[1:])])
    if bump:
        out[joint.BIN_EDGES.index(900)] += bump
    return A_INJ * out


def injected_chunk(ch, shift, k):
    """Window FFTs of (null VIRGO, real TESS + injected echo of null VIRGO)."""
    r = ch.raw
    i = r['off'] + shift * r['per_day']
    if i < 0 or i + r['n'] > len(r['x_all']):
        return None
    xn = r['x_all'][i:i + r['n']]
    x0 = np.where(np.isfinite(xn), xn, 0.0)
    echo = A_INJ * np.convolve(x0, k)[:r['n']]
    echo[~np.isfinite(xn)] = np.nan
    X, Y, L, _ = joint.windows(xn, r['y_norm'] + echo, r['level'], r['win'])
    if len(L) < 0.7 * len(ch.eps):
        return None
    return (ch.f, ch.cadence, X, Y, (L - ch.floor) / ch.floor, ch.u)


def fit_trial(parts_by_cam, cams):
    terms = {c: joint.Terms(parts_by_cam[c], joint.CAM_LAT[c]) for c in cams}
    grids = {c: terms[c].solve() for c in cams}
    tot = sum(grids[c][0].min(0) for c in cams)
    j = int(np.argmin(tot))
    okd = tot - tot.min() <= 1
    out = {'dt': joint.DTS[j], 'dt_lo': joint.DTS[okd].min(), 'dt_hi': joint.DTS[okd].max()}
    for c in cams:
        chi, AE = grids[c]
        i = int(np.argmin(chi[:, j]))
        oka = chi[:, j] - chi[:, j].min() <= 1
        out[c] = dict(alpha=joint.ALPHAS[i], A=AE[i, j, 0], E=AE[i, j, 1],
                      a_lo=joint.ALPHAS[oka].min(), a_hi=joint.ALPHAS[oka].max())
        # Non-negative bins at the fitted dt.
        fake = [type('C', (), dict(f=p[0], X=p[2], Y=p[3], eps=p[4])) for p in parts_by_cam[c]]
        D, y, _ = joint.bin_rows(fake, joint.DTS[j])
        out[c]['bins'] = nnls(D, y)[0][:-1]
    return out


def main(datadir, spec, cameras='1,2,3,4', ntrials='24'):
    items = joint.parse_sectors(spec)
    cams = tuple(int(c) for c in cameras.split(','))
    chunks = {c: [] for c in cams}
    for c in cams:
        for sec, prod in items:
            try:
                chunks[c].append(joint.Chunk(datadir, sec, prod, c, 'red'))
            except FileNotFoundError:
                pass
    shifts = joint.NULL_SHIFTS[::max(1, len(joint.NULL_SHIFTS) // int(ntrials))][:int(ntrials)]
    cases = {'model': (1.34, 0.0), 'extended': (0.6, 0.0), 'bump': (1.34, 0.05)}
    print(f'injection-recovery on {spec}, cameras {cams}, {len(shifts)} null shifts per case, '
          f'A_inj = {A_INJ}, dt_inj = 0')
    for name, (alpha, bump) in cases.items():
        res = []
        for sh in shifts:
            parts = {}
            for c in cams:
                parts[c] = [p for p in (injected_chunk(ch, sh, kernel_samples(ch.u, alpha, ch.cadence, bump))
                                        for ch in chunks[c]) if p is not None]
            if all(len(parts[c]) >= 0.7 * len(chunks[c]) for c in cams):
                res.append(fit_trial(parts, cams))
        dts = np.array([r['dt'] for r in res])
        print(f'\n== {name} (alpha {alpha}, bump {bump}): {len(res)} trials')
        print(f'  dt: median {np.median(dts):+.1f} s, scatter (16-84%) '
              f'{np.percentile(dts, 16):+.0f} .. {np.percentile(dts, 84):+.0f} s')
        cov = np.mean([r['dt_lo'] <= 0 <= r['dt_hi'] for r in res])
        print(f'  coverage: true dt = 0 inside the profile 68% interval in {cov:.0%} of trials')
        for c in cams:
            al = np.array([r[c]['alpha'] for r in res])
            A = np.array([r[c]['A'] for r in res])
            E = np.array([r[c]['E'] for r in res])
            bins = np.array([r[c]['bins'] for r in res])
            tb = true_bins(chunks[c][0].u, alpha, bump)
            acov = np.mean([r[c]['a_lo'] - 0.1 <= alpha <= r[c]['a_hi'] + 0.1 for r in res])
            print(f'  cam {c}: alpha interval coverage {acov:.0%}; A scatter {A.std():.3f}')
            print(f'  cam {c}: alpha {np.median(al):.1f} [{np.percentile(al, 16):.1f},'
                  f'{np.percentile(al, 84):.1f}] (true {alpha}); '
                  f'A {np.median(A):.2f} [{np.percentile(A, 16):.2f},{np.percentile(A, 84):.2f}]'
                  f' (true {A_INJ}); E {np.median(E):+.2f} (true 0)')
            print('     bins true ' + ' '.join(f'{v:.3f}' for v in tb))
            print('     bins rec  ' + ' '.join(f'{v:.3f}' for v in np.median(bins, 0)))
            print('     bins 68%  ' + ' '.join(f'{(h - l) / 2:.3f}' for l, h in
                                              zip(*np.percentile(bins, [16, 84], axis=0))))


if __name__ == '__main__':
    main(*sys.argv[1:])
