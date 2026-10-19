"""Fine timing: the offset dt between the TESS response and the model on a
1-s grid, per camera, all cameras combined, and per sector (drift check).
Uses the joint estimator (joint.py): at each dt, A and E are solved
linearly and chi^2 is minimised over the kernel index alpha.

usage: timing.py datadir SECTORS [red|tsi] [cameras]
"""

import sys
import numpy as np

sys.path.insert(0, __file__.rsplit('/', 1)[0])
import joint

joint.DTS = np.arange(-240, 241, 2)


def best_and_interval(chi):
    """Parabola through the minimum and its neighbours; 68% interval at
    delta chi^2 = 1 by interpolation on the grid."""
    dts = joint.DTS
    j = int(np.argmin(chi))
    if 0 < j < len(chi) - 1:
        y0, y1, y2 = chi[j - 1:j + 2]
        den = y0 - 2 * y1 + y2
        best = dts[j] + (0.5 * (y0 - y2) / den if den > 0 else 0.0)
    else:
        best = float(dts[j])
    fine = np.arange(dts[0], dts[-1] + 0.01, 0.1)
    cf = np.interp(fine, dts, chi)
    ok = cf - cf.min() <= 1
    return best, fine[ok].min(), fine[ok].max()


def main(datadir, spec, ref='red', cameras='1,2,3,4'):
    items = joint.parse_sectors(spec)
    cams = tuple(int(c) for c in cameras.split(','))
    chunks = {c: [] for c in cams}
    for cam in cams:
        for sec, prod in items:
            try:
                chunks[cam].append(joint.Chunk(datadir, sec, prod, cam, ref))
            except FileNotFoundError:
                pass

    print(f'timing offset dt [s] on a 2-s grid (chi^2 minimised over alpha, A, E); '
          f'{spec}, ref {ref}')
    total = 0
    alpha_best = {}
    for c in cams:
        t = joint.Terms([(ch.f, ch.cadence, ch.X, ch.Y, ch.eps, ch.u) for ch in chunks[c]], joint.CAM_LAT[c])
        chi, _ = t.solve()
        prof = chi.min(0)
        alpha_best[c] = joint.ALPHAS[int(np.argmin(chi[:, int(np.argmin(prof))]))]
        total = total + prof
        b, lo, hi = best_and_interval(prof)
        print(f'  camera {c}: dt = {b:+6.1f} s  [{lo:+.1f}, {hi:+.1f}]   (alpha {alpha_best[c]:.1f})')
    b, lo, hi = best_and_interval(total)
    print(f'  combined: dt = {b:+6.1f} s  [{lo:+.1f}, {hi:+.1f}]')

    # Drift: per sector, cameras combined, alpha fixed per camera.
    print('\nper sector (all cameras, alpha fixed at the per-camera best):')
    labels = sorted({ch.label for c in cams for ch in chunks[c]},
                    key=lambda s: (s.endswith('min'), int(s.split('_')[0])))
    rows = []
    for lab in labels:
        tot = 0
        for c in cams:
            parts = [(ch.f, ch.cadence, ch.X, ch.Y, ch.eps, ch.u) for ch in chunks[c] if ch.label == lab]
            if not parts:
                continue
            t = joint.Terms(parts, joint.CAM_LAT[c], alphas=[alpha_best[c]])
            tot = tot + t.solve()[0][0]
        b, lo, hi = best_and_interval(tot)
        rows.append((lab, b, lo, hi))
        print(f'  sector {lab:>8s}: dt = {b:+6.1f} s  [{lo:+.1f}, {hi:+.1f}]')
    # Minima at the grid edge have meaningless (truncated) intervals.
    edge = joint.DTS[-1] - 5
    keep = [r for r in rows if abs(r[1]) < edge]
    v = np.array([r[1] for r in keep])
    e = np.array([(r[3] - r[2]) / 2 for r in keep])
    w = 1 / e ** 2
    mean = np.sum(w * v) / w.sum()
    chi2 = np.sum(w * (v - mean) ** 2)
    print(f'\n  {len(rows) - len(keep)} sector(s) with the minimum at the grid edge excluded')
    print(f'  weighted mean {mean:+.1f} +- {1 / np.sqrt(w.sum()):.1f} s; median {np.median(v):+.1f} s; '
          f'scatter chi^2 = {chi2:.1f} for {len(v) - 1} dof')


if __name__ == '__main__':
    main(*sys.argv[1:])
