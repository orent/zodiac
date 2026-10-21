"""Validation suite on the compact data: jackknife splits and a mirror test.

For each subset of sector products / windows: per camera, alpha at the
best (grid), A and E at the Kelsall alpha = 1.4 with block-bootstrap
errors, so subsets compare on a common footing. dt fixed.

Mirror test: amplitude of a time-reversed (acausal) kernel, conj(H), fitted
alongside nothing else. A physical echo gives ~0 there.

usage: validate.py datadir [cameras] [dt]
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint

ALL = __import__('os').environ.get('VALIDATE_ALL', '27-38,1-13_2min,17-23_2min')          # standard pointing only
SPLITS = [
    ('all (standard pointing)', ALL, None),
    ('odd sectors', '27,29,31,33,35,37,1_2min,3_2min,5_2min,7_2min,9_2min,11_2min,13_2min,'
                    '17_2min,19_2min,21_2min,23_2min', None),
    ('even sectors', '28,30,32,34,36,38,2_2min,4_2min,6_2min,8_2min,10_2min,12_2min,'
                     '18_2min,20_2min,22_2min', None),
    ('year 1 (S1-13, 2-min, south)', '1-13_2min', None),
    ('year 2 standard (S17-23, 2-min, north)', '17-23_2min', None),
    ('year 3 (S27-38, 20-s, south)', '27-38', None),
    ('quiet windows (level < median)', ALL, 'quiet'),
    ('bright windows (level >= median)', ALL, 'bright'),
]
NBOOT = 100


def subset(ch, which):
    if which is None:
        return ch.X, ch.Y, ch.eps
    m = ch.eps < np.median(ch.eps) if which == 'quiet' else ch.eps >= np.median(ch.eps)
    return ch.X[m], ch.Y[m], ch.eps[m]


def fit(chunks, cam, which, rng):
    parts = [(ch.f, ch.cadence, *subset(ch, which), ch.u) for ch in chunks]
    t = joint.Terms(parts, joint.CAM_LAT[cam])
    chi, AE = t.solve()
    j = int(np.argmin(abs(joint.DTS - 0)))
    ia = int(np.argmin(chi[:, j]))
    k14 = int(np.argmin(abs(joint.ALPHAS - 1.4)))
    A, E = AE[k14, j]
    bs = np.array([t.solve(joint.block_weights(len(t.eps), rng))[1][k14, j] for _ in range(NBOOT)])
    return joint.ALPHAS[ia], A, bs[:, 0].std(), E, bs[:, 1].std(), len(t.eps)


def mirror(chunks, cam, rng):
    """Causal (H) and time-reversed (conj H) Kelsall kernels fitted
    together; the acausal coefficient must be ~0. Fitting conj(H) alone is
    not a test: at low frequency H and conj(H) are both nearly real and
    overlap. Block-bootstrap errors over windows."""
    rows = {k: [] for k in ('cc', 'mm', 'cm', 'yc', 'ym')}
    for ch in chunks:
        m = (ch.f >= joint.F_LO) & (ch.f <= joint.F_HI)
        fm = ch.f[m]
        h = joint.kernel_h(fm, ch.u, 1.34, key=ch.cadence)[0]
        X, Y = ch.X[:, m], ch.Y[:, m]
        W = joint.noise_weights(Y)
        XX = W * np.abs(X) ** 2
        YX = W * Y * X.conj()
        rows['cc'].append(XX @ np.abs(h) ** 2)
        rows['mm'].append(XX @ np.abs(h) ** 2)
        rows['cm'].append(XX @ (h * h).real)
        rows['yc'].append((YX @ h.conj()).real)
        rows['ym'].append((YX @ h).real)
    r = {k: np.concatenate(v) for k, v in rows.items()}

    def solve(w):
        M = np.array([[np.sum(w * r['cc']), np.sum(w * r['cm'])],
                      [np.sum(w * r['cm']), np.sum(w * r['mm'])]])
        v = np.array([np.sum(w * r['yc']), np.sum(w * r['ym'])])
        return np.linalg.solve(M, v)

    Ac, Am = solve(np.ones(len(r['cc'])))
    bs = np.array([solve(joint.block_weights(len(r['cc']), rng)) for _ in range(NBOOT)])
    return Ac, bs[:, 0].std(), Am, bs[:, 1].std()


def main(datadir, cameras='1,2,3,4', dt='0'):
    joint.DTS = np.array([float(dt)])
    cams = tuple(int(c) for c in cameras.split(','))
    rng = np.random.default_rng(5)
    cache = {}

    def chunks_for(spec, cam):
        out = []
        for sec, prod in joint.parse_sectors(spec):
            key = (sec, prod, cam)
            if key not in cache:
                try:
                    cache[key] = joint.Chunk(datadir, sec, prod, cam, 'red')
                except FileNotFoundError:
                    cache[key] = None
            if cache[key] is not None:
                out.append(cache[key])
        return out

    print(f'validation: A, E per unit floor at alpha = 1.4 (bootstrap sigma); best alpha on the grid; dt = {dt} s')
    print(f'{"split":44s} ' + '  '.join(f'cam {c}: A ± σ        E ± σ    α_best' for c in cams))
    for name, spec, which in SPLITS:
        cells = []
        for c in cams:
            ab, A, sA, E, sE, n = fit(chunks_for(spec, c), c, which, rng)
            cells.append(f'{A:5.2f} ± {sA:.2f}  {E:+5.2f} ± {sE:.2f}  {ab:.1f}')
        print(f'{name:44s} ' + '   '.join(cells))

    print('\nmirror test: causal and acausal Kelsall kernels fitted together (acausal must be ~0):')
    for c in cams:
        Ac, sc, Am, sm_ = mirror(chunks_for(ALL, c), c, rng)
        print(f'  cam {c}: causal A = {Ac:.3f} ± {sc:.3f}   mirrored A = {Am:+.3f} ± {sm_:.3f}  '
              f'({Am / sm_:+.1f} sigma)')


if __name__ == '__main__':
    main(*sys.argv[1:])
