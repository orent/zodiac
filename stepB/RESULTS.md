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

## Causality / onset validation

Time base: TESS spacecraft clock (`TIME − TIMECORR`) and VIRGO's own
timestamps, which removes the 499 s Sun-to-1 AU travel time common to both.
The response must start at τ = 0 and be zero at negative lags.

**Free delay bins (`lagfit.py`).** Combined over both cameras, 0.1–8.3 mHz:

| lag bin [s] | weight | model |
|---|---|---|
| −2400..−1200 | −0.13 ± 0.27 | 0 |
| −1200..−600 | −0.11 ± 0.20 | 0 |
| −600..−240 | +0.13 ± 0.15 | 0 |
| −240..0 | +0.13 ± 0.11 | 0 |
| **0..240** | **+0.67 ± 0.11 (6.0σ)** | 0.37 |
| 240..600 | +0.26 ± 0.14 | 0.23 |
| 600..1200 | +0.39 ± 0.21 | 0.13 |
| 1200..2400 | −0.36 ± 0.28 | 0.06 |

- The negative-lag bins are all within 1.2σ of zero. Their sum is
  +0.001 ± 0.62.
- Both cameras agree in the 0–240 s bin: 0.65 ± 0.15 and 0.68 ± 0.17.
- 60-s bins are not resolvable with one sector. Their errors are ±0.4–0.6
  and adjacent bins trade off against each other.

**Onset scan (`onset.py`).** The model kernel shape (step plus decay) is
shifted to a trial onset τ₀ and only its amplitude is fitted. Both cameras
are combined.

| Quantity | Value |
|---|---|
| Peak | τ₀ = −60 s, S/N 9.0 |
| 1σ range | −150 .. +60 s |
| Camera 1 alone | best −60 s, 5.6σ |
| Camera 2 alone | best −60 s, 7.0σ |

- The onset is consistent with zero, at about ±100 s resolution.
- The S/N falls to zero by τ₀ ≈ −600 s and +480 s. Beyond that, the
  negative sidelobes come from the autocorrelation of the high-passed
  solar signal.
- The fitted amplitude is 1.35, against about 0.9 expected (80% zodiacal
  light × ~1.15 for TESS band vs 862 nm). So the response is stronger
  and/or more compact than the model. Possible causes: Earthshine at
  zero lag, a higher zodiacal fraction, or near-Earth dust such as the
  resonant ring. This needs follow-up.
- VIRGO's timestamp convention (start, mid or end of the minute) is a
  ±30 s systematic on the onset. It still has to be pinned down before
  claiming better than ~1 minute.

## Visual response curve (`curve.py`, `response_s27.png`)

The curve is the inverse transform of H = S_yx/S_xx over 0.1–8.3 mHz, with
cameras 1 and 2 summed. A Wiener-like weight rolls it off above about
3 mHz, where the solar probe signal is weak. The model kernel is passed
through the same filter so the two can be compared directly.

| Quantity | Value |
|---|---|
| Measured peak at τ = 0 | 0.168 per minute |
| Null 1σ at τ = 0 | 0.036, so the peak is ~4.7σ in a single point |
| Decay | over ~5 min, matching the filtered model (fitted amplitude ×1.32) |

- The rise starts about 0.5–1 min earlier than the filtered model. That is
  the same −60 s seen in the onset scan, and it lies within the VIRGO
  timestamp-convention uncertainty.
- Acausal side: 10% of points fall outside the null 95% band, against
  about 5% expected. The excursions are a few isolated wiggles of
  null-like amplitude, with no structure.

## Earthshine split test (`earthshine.py`)

Method:
- Work in absolute units (e⁻/s/px), so a constant zodiacal level gives a
  constant response.
- Split the 6-h windows into thirds by background level above the sector
  floor (5th percentile). That excess is the scattered-light proxy.
- Fit the model kernel (amplitude A) plus a zero-delay term (amplitude E)
  to each third.
- Errors from null shifts.

| cam | level range | excess | A (ZL kernel) | E (zero delay) | E / excess |
|---|---|---|---|---|---|
| 1 | 138–158 | 8 | 144 ± 45 | −4 ± 22 | |
| 1 | 158–231 | 53 | 186 ± 59 | +31 ± 25 | 0.6 ± 0.5 |
| 1 | 231–325 | 134 | 276 ± 178 | **+146 ± 52** | 1.09 ± 0.39 |
| 2 | 93–109 | 7 | 97 ± 42 | +7 ± 22 | |
| 2 | 109–147 | 34 | 206 ± 48 | −10 ± 22 | |
| 2 | 147–240 | 89 | 213 ± 77 | **+93 ± 33** | 1.04 ± 0.37 |

- **A zero-delay solar-modulated component appears only in the brightest
  third.** Both cameras show it, at about 3.9σ combined.
- **It scales about 1:1 with the excess level** (E/excess ≈ 1.0 in both
  cameras). In other words, the scattered-light excess is fully modulated
  sunlight arriving with no extra delay. That is the Earthshine/moonshine
  signature.
- The −240..0 s lag bin also rises in the bright third. That is the
  zero-delay term smeared by the band limit, not acausal dust.
- **In the quiet third**, E is consistent with 0. A matches the
  zodiacal-only expectation if the background floor is essentially all
  zodiacal light: 138 × ~1.15 ≈ 160 expected vs 144 ± 45 measured
  (camera 1); 94 × 1.15 ≈ 108 vs 97 ± 42 (camera 2).
- So the earlier fractional amplitude excess (×1.32) is mostly
  Earthshine. The assumed 80% zodiacal fraction was also too low in quiet
  windows.

Remedy for the depth analysis: either drop windows more than ~30 e⁻/s/px
above the floor, or fit a zero-delay term ∝ (level − floor) jointly with
the kernel. The `QUALITY` scattered-light flag alone is not enough: the
flagged cadences were already excluded here.

## Update after the time-base fix (see `TIMEBASE.md`)

- The 1σ onset ranges quoted above (−150..+60 s) used the wrong
  definition (S/N drop of 1). The corrected interval (S/N² drop of 1),
  with the emission-time base, is −84..−33 s for the model template.
- That offset is degenerate with the response shape. It is explained
  mainly by the compact (zero-delay) Earthshine term.
- In quiet windows, |H(f)| follows the smooth-cloud model, and the onset
  is −100 [−182, −32] s, i.e. not compelling.
- Timing goes into the joint fit as a free Δt.
