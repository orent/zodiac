# Quick look: TESS background vs VIRGO red, sector 27

Inputs:
- the camera-combined fractional background from step A (197 and 199
  stamps), binned to VIRGO's 1-min grid;
- VIRGO SPM red (862 nm).

Both series are high-passed with a 3-h running median. Spectra use 6-h
Welch windows: 149 windows for camera 1, 147 for camera 2.

Method:
- H = ΣS_yx / ΣS_xx, summed over each frequency band.
- Null: the same estimate with VIRGO taken from 213 unrelated stretches,
  30–400 days away.
- Light-time differences (~5 s) are not corrected yet.

| band [mHz] | cam 1 \|H\| | phase | \|H\|/σ_null | cam 2 \|H\| | phase | \|H\|/σ_null |
|---|---|---|---|---|---|---|
| 0.1–0.5 | 1.20 | −0.58 | 2.8 | 1.14 | −0.24 | 2.3 |
| 0.5–1 | 0.40 | −0.34 | 3.1 | 0.49 | −0.62 | 3.4 |
| 1–2 | 0.43 | −0.95 | 3.2 | 0.45 | −1.02 | 3.2 |
| 2–4 | 0.39 | −0.41 | 3.1 | 0.27 | −0.32 | 1.7 |
| 4–8.3 | 0.53 | +0.66 | 1.8 | 0.14 | +0.34 | 0.4 |

## Combined, 0.1–4 mHz, both cameras

| Statistic | Observed | Null | Null shifts reaching it | z |
|---|---|---|---|---|
| Σ\|H\|²/σ² (phase-agnostic) | 67.9 | 16.0 ± 6.2 | 0 of 213 | 8.3 (Gaussian χ²₁₆: p = 2×10⁻⁸, 5.5σ) |
| ΣRe H/σ (in-phase, positive response) | 18.4 | −0.2 ± 3.3 | 0 of 213 | 5.7 |

The TESS background responds to solar variability, and the response is
consistent across cameras 1 and 2 (two independent stamp sets and lines of
sight).

## Comparison with the model

The model is `sensitivity.py`: 80% zodiacal light, and a kernel at 18°
latitude.

| | \|H\| predicted | \|H\| measured | phase predicted | phase measured |
|---|---|---|---|---|
| 0.5–2 mHz | 0.3–0.4 | 0.40–0.49 | −0.6 to −1.0 | −0.34 to −1.0 |
| 2–4 mHz | ~0.1 | 0.27–0.39 | ~−1.3 | −0.3 to −0.4 |

- Below 2 mHz the response matches the model in both amplitude and lag.
- The 2–4 mHz excess is about 2σ above the model, with a smaller lag.
  A zero-delay component could explain it, for example Earthshine or
  something instrumental. That needs checking.

## Not yet done

- Light-time correction to emission at the Sun (~5 s).
- Repeat with VIRGO TSI, an independent instrument.
- Split-half stamp test.
- A lag-domain (deconvolved) view.
