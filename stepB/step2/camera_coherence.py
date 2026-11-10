import sys, numpy as np
sys.path.insert(0, 'stepB')
from aligned import highpass
W = sys.argv[1]
BANDS = [(0.1e-3, 0.5e-3), (0.5e-3, 2e-3), (2e-3, 8e-3)]

def spectra(series, cad):
    win = int(6 * 3600 / cad); n = len(series[0]); hann = np.hanning(win); out = []
    for s in range(0, n - win, win // 2):
        seg = [a[s:s + win] for a in series]
        if all(np.isfinite(x).all() for x in seg):
            out.append([np.fft.rfft(hann * (x - x.mean())) for x in seg])
    return np.array(out), np.fft.rfftfreq(win, cad)   # (nseg, nser, nf)

def coh(F, f, i, j):
    r = []
    for lo, hi in BANDS:
        m = (f >= lo) & (f < hi)
        a, b = F[:, i, m], F[:, j, m]
        r.append(abs(np.sum(a * b.conj())) ** 2 / (np.sum(abs(a) ** 2) * np.sum(abs(b) ** 2)))
    return r

for sec, prod in ((27, ''), (34, ''), (27, '_2min')):
    d = f'{W}/s{sec:04d}{prod}'
    ys = []
    for cam in (1, 2, 3, 4):
        c = np.load(f'{d}/cam{cam}.npz'); cad = float(c['cadence'])
        ys.append(highpass(c['frac'].astype(float), int(round(3 * 3600 / cad))))
    F, f = spectra(ys, cad)
    print(f'S{sec}{prod}: cross-camera coherence (bands 0.1-0.5 / 0.5-2 / 2-8 mHz), {len(F)} segs')
    for i, j in ((0, 1), (1, 2), (2, 3), (0, 3)):
        print(f'   cam{i+1}-cam{j+1}: ' + '  '.join(f'{v:.3f}' for v in coh(F, f, i, j)))
    # Within camera 1 and 3: cells far apart.
    C = np.load(f'{d}/cells.npz'); cols = list(C['cells_columns'])
    for cam in (1, 3):
        names = [f'cam{cam}_x{int(r[cols.index("gx")])}y{int(r[cols.index("gy")])}' for r in C['cells'] if int(r[0]) == cam]
        if len(names) < 2: continue
        a, b = names[0], names[-1]
        cad2 = float(C['cadence'])
        ser = [highpass(C[f'{n}_frac'].astype(float), int(round(3 * 3600 / cad2))) for n in (a, b)]
        F2, f2 = spectra(ser, cad2)
        print(f'   within cam{cam}: {a} vs {b}: ' + '  '.join(f'{v:.3f}' for v in coh(F2, f2, 0, 1)) + f'  ({len(F2)} segs)')
