import sys, numpy as np
sys.path.insert(0, 'stepB')
from aligned import highpass
W = sys.argv[1]
BANDS = [(0.1e-3, 0.5e-3), (0.5e-3, 2e-3), (2e-3, 8e-3)]

def segs(arrs, win):
    n = len(arrs[0]); out = []
    for s in range(0, n - win, win // 2):
        seg = [a[s:s + win] for a in arrs]
        if all(np.isfinite(x).all() for x in seg):
            out.append(seg)
    return out

def run(sec, prod):
    d = f'{W}/s{sec:04d}{prod}'
    C = np.load(f'{d}/cells.npz')
    for cam in (1, 2, 3, 4):
        c = np.load(f'{d}/cam{cam}.npz')
        cad = float(c['cadence']); win = int(6 * 3600 / cad)
        hp = int(round(3 * 3600 / cad))
        y = highpass(c['frac'].astype(float), hp)
        p = [highpass(C[f'cam{cam}_poscorr{k}'].astype(float), hp) for k in (1, 2)]
        S = segs([y] + p, win)
        if len(S) < 5:
            print(sec, prod, cam, 'few segments'); continue
        hann = np.hanning(win)
        F = [np.array([np.fft.rfft(hann * (s[i] - s[i].mean())) for s in S]) for i in range(3)]
        f = np.fft.rfftfreq(win, cad)
        Y, P1, P2 = F
        # Multiple coherence of y with (p1, p2) per frequency, averaged over segments.
        res = []
        for lo, hi in BANDS:
            m = (f >= lo) & (f < hi)
            # Pool over frequencies in the band: least-squares y ~ a p1 + b p2 (complex, per band).
            A = np.stack([P1[:, m].ravel(), P2[:, m].ravel()], 1)
            yy = Y[:, m].ravel()
            coef, *_ = np.linalg.lstsq(A, yy, rcond=None)
            r = yy - A @ coef
            res.append(1 - np.sum(abs(r) ** 2) / np.sum(abs(yy) ** 2))
        print(f'S{sec}{prod} cam{cam}: fraction of background power explained by POS_CORR  '
              + '  '.join(f'{lo*1e3:.1f}-{hi*1e3:.1f} mHz: {v:5.1%}' for (lo, hi), v in zip(BANDS, res))
              + f'  ({len(S)} segs)')

for sec in (27, 30, 34):
    for prod in ('', '_2min'):
        run(sec, prod)
