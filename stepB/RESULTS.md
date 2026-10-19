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

## Joint estimator, sector 27 (`joint.py`)

The model is fitted per window in absolute units, with VIRGO red as the
reference:

    Y = X · [A · H_α · e^(−2πifΔt) + E · (level − floor)]

- Δt is shared by both cameras.
- Errors: block bootstrap over windows (300 resamples) and profile
  likelihood.
- A is tested against nulls (~200 VIRGO shifts).

| | camera 1 (lat 18°) | camera 2 (lat 42°) |
|---|---|---|
| windows | 146 | 144 |
| **Earthshine E** [per e⁻/s/px of excess] | **0.84 [0.64, 0.97]** | **0.74 [0.57, 0.85]** |
| ZL amplitude A (best α), significance vs nulls | 78, **4.6σ** | 60, **4.2σ** |
| α (density index), 68% Δχ² = 1 | 1.8 … 4.0 (best 3.4) | 1.0 … 4.0 (best 3.0) |
| model α = 1.34–1.4: Δχ², A | Δχ² 1.4, A = 102 | Δχ² 0.5, A = 69 |
| expected A from quiet floor × 1.15 | ~159 | ~108 |
| non-negative bins: total area | 144 [110, 185] | 124 [98, 181] |

**Shared Δt = +50 s.** Profile 68%: +20 … +70 s. Bootstrap 68%:
−10 … +70 s. Consistent with 0 at about 1–2σ, and within the VIRGO
stamp-convention uncertainty.

Non-negative log-binned kernel (e⁻/s/px per unit solar fraction, 68%
interval in brackets):

| bin [s] | 0–60 | 60–180 | 180–420 | 420–900 | 900–2000 | 2000–4000 |
|---|---|---|---|---|---|---|
| cam 1 | 30 [19, 42] | 23 [9, 38] | 14 [3, 27] | 38 [24, 49] | 0 [0, 0] | 39 [10, 71] |
| cam 2 | 16 [7, 26] | 27 [18, 39] | 11 [0, 21] | 20 [11, 33] | 6 [0, 26] | 44 [16, 87] |

Reading:
- **The zodiacal response is detected separately from Earthshine** in
  each camera, about 6σ combined. Earthshine is fully solar-modulated
  light at zero delay, with E ≈ 0.75–0.85 per unit of excess level.
- **The timing offset is consistent with zero** once Earthshine is
  modelled. The earlier negative onset was template mismatch.
- **One sector does not constrain the shape.** The χ² over α is shallow,
  and the smooth-cloud α = 1.34 is within about 1σ.
- **The total area of the non-negative bins agrees with the
  quiet-floor zodiacal calibration** (144 vs 159; 124 vs 108).
  - The parametric fit with a compact α underestimates A, because it
    cannot put weight in the far bins.
  - Those far bins (≥ 900 s) map to f < 0.3 mHz, where the 3-h high-pass
    and the spacecraft common mode act. The 900–2000 / 2000–4000 split
    is not trustworthy yet: one bin is pinned at 0 and the other takes
    everything.

Next:
- stack sectors 28–38;
- for the tail: 12–24 h windows and a gentler high-pass, plus
  common-mode regression (`ROADMAP.md`).

## Interim: joint fit, sectors 27–30, all four cameras

(20-s data, VIRGO red; raw output below.)

```
camera 1: 502 windows from 4 sector products (27,28,29,30)
camera 2: 502 windows from 4 sector products (27,28,29,30)
camera 3: 500 windows from 4 sector products (27,28,29,30)
camera 4: 504 windows from 4 sector products (27,28,29,30)

shared timing offset dt = +10 s  (profile 68%: +0 .. +10 s; bootstrap 68%: -10 .. +30 s)

per camera at the shared dt (A, E per unit quiet floor; expected A ~1.15 x ZL fraction):
  cam  lat  alpha best [boot 68%] [dchi2=1]   A [boot 68%]         E [boot 68%]        A/null_sigma   A at alpha=1.4
  1     18  1.0 [0.4, 1.6] [0.6, 1.4]     0.79 [0.66, 0.92]    +0.59 [+0.46, +0.72]      9.0 (183 nulls)    0.72 (dchi2 0.8)
  2     42  1.6 [0.8, 2.6] [0.8, 2.4]     0.67 [0.59, 0.78]    +0.60 [+0.46, +0.74]      8.1 (182 nulls)    0.69 (dchi2 0.0)
  3     66  2.2 [0.4, 4.0] [0.8, 4.0]     0.65 [0.56, 0.77]    +0.76 [+0.62, +0.91]      7.0 (182 nulls)    0.68 (dchi2 0.2)
  4     90  1.6 [0.4, 4.0] [0.4, 4.0]     0.44 [0.38, 0.52]    +0.80 [+0.69, +0.91]      6.8 (181 nulls)    0.45 (dchi2 0.0)

profile chi^2 vs alpha at the shared dt, relative to best:
  cam 1: 0.6:0.5 1.0:0.0 1.4:0.8 1.8:2.5 2.2:4.8 2.6:7.6 3.0:10.7 3.6:15.7
  cam 2: 0.6:1.6 1.0:0.5 1.4:0.0 1.8:0.1 2.2:0.6 2.6:1.4 3.0:2.5 3.6:4.5
  cam 3: 0.6:1.1 1.0:0.6 1.4:0.2 1.8:0.1 2.2:0.0 2.6:0.1 3.0:0.2 3.6:0.6
  cam 4: 0.6:0.1 1.0:0.0 1.4:0.0 1.8:0.0 2.2:0.0 2.6:0.1 3.0:0.2 3.6:0.4

non-negative log-binned kernel at dt = +10 s (per unit floor, per bin; bootstrap 68%):
  bin [s]                    0-60            60-180           180-420           420-900          900-2000         2000-4000   Earthshine E
  cam 1 fit    0.130 [0.07,0.18]  0.126 [0.09,0.18]  0.231 [0.19,0.29]  0.184 [0.14,0.25]  0.032 [0.00,0.12]  0.188 [0.14,0.31]   0.59 [0.43,0.76]
  cam 1 model  0.171             0.257             0.300             0.242             0.132             0.038              (alpha 1.34, A 1.15)
  cam 1 total area 0.89 [0.80, 1.13]  (expected ~1.15 x ZL fraction)
  cam 2 fit    0.111 [0.07,0.17]  0.239 [0.17,0.30]  0.183 [0.12,0.24]  0.117 [0.06,0.18]  0.000 [0.00,0.00]  0.062 [0.00,0.17]   0.65 [0.56,0.78]
  cam 2 model  0.237             0.335             0.317             0.176             0.068             0.015              (alpha 1.34, A 1.15)
  cam 2 total area 0.71 [0.60, 0.90]  (expected ~1.15 x ZL fraction)
  cam 3 fit    0.199 [0.10,0.30]  0.233 [0.16,0.32]  0.197 [0.11,0.25]  0.000 [0.00,0.04]  0.140 [0.04,0.29]  0.000 [0.00,0.14]   0.77 [0.64,0.93]
  cam 3 model  0.314             0.397             0.278             0.117             0.035             0.007              (alpha 1.34, A 1.15)
  cam 3 total area 0.77 [0.65, 1.05]  (expected ~1.15 x ZL fraction)
  cam 4 fit    0.138 [0.09,0.21]  0.209 [0.15,0.26]  0.045 [0.00,0.11]  0.075 [0.02,0.13]  0.000 [0.00,0.00]  0.000 [0.00,0.13]   0.83 [0.72,0.96]
  cam 4 model  0.405             0.423             0.224             0.074             0.019             0.004              (alpha 1.34, A 1.15)
  cam 4 total area 0.47 [0.43, 0.67]  (expected ~1.15 x ZL fraction)

```

## Full 20-s stack: joint fit, sectors 27–38, all four cameras

(VIRGO red; raw output below; interpretation in the next section.)

```
camera 1: 1521 windows from 12 sector products (27,28,29,30,31,32,33,34,35,36,37,38)
camera 2: 1528 windows from 12 sector products (27,28,29,30,31,32,33,34,35,36,37,38)
camera 3: 1528 windows from 12 sector products (27,28,29,30,31,32,33,34,35,36,37,38)
camera 4: 1533 windows from 12 sector products (27,28,29,30,31,32,33,34,35,36,37,38)

shared timing offset dt = -10 s  (profile 68%: -10 .. +0 s; bootstrap 68%: -10 .. +0 s)

per camera at the shared dt (A, E per unit quiet floor; expected A ~1.15 x ZL fraction):
  cam  lat  alpha best [boot 68%] [dchi2=1]   A [boot 68%]         E [boot 68%]        A/null_sigma   A at alpha=1.4
  1     18  1.0 [0.6, 1.6] [0.8, 1.2]     0.78 [0.68, 0.84]    +0.35 [+0.26, +0.47]     15.1 (128 nulls)    0.72 (dchi2 2.3)
  2     42  1.0 [0.4, 1.8] [0.6, 1.4]     0.58 [0.52, 0.65]    +0.40 [+0.29, +0.54]     12.6 (127 nulls)    0.56 (dchi2 0.7)
  3     66  0.4 [0.4, 1.4] [0.4, 0.8]     0.50 [0.43, 0.55]    +0.69 [+0.55, +0.86]      9.3 (126 nulls)    0.46 (dchi2 2.0)
  4     90  0.4 [0.4, 1.2] [0.4, 0.8]     0.49 [0.45, 0.52]    +0.47 [+0.38, +0.61]     13.9 (127 nulls)    0.47 (dchi2 1.8)

profile chi^2 vs alpha at the shared dt, relative to best:
  cam 1: 0.6:1.9 1.0:0.0 1.4:2.3 1.8:7.6 2.2:15.2 2.6:24.3 3.0:34.4 3.6:50.7
  cam 2: 0.6:0.6 1.0:0.0 1.4:0.7 1.8:2.5 2.2:5.0 2.6:8.1 3.0:11.6 3.6:17.3
  cam 3: 0.6:0.3 1.0:1.0 1.4:2.0 1.8:3.1 2.2:4.4 2.6:5.8 3.0:7.2 3.6:9.6
  cam 4: 0.6:0.3 1.0:1.1 1.4:1.8 1.8:2.6 2.2:3.5 2.6:4.3 3.0:5.2 3.6:6.5

non-negative log-binned kernel at dt = -10 s (per unit floor, per bin; bootstrap 68%):
  bin [s]                    0-60            60-180           180-420           420-900          900-2000         2000-4000   Earthshine E
  cam 1 fit    0.095 [0.07,0.12]  0.166 [0.14,0.19]  0.210 [0.19,0.23]  0.166 [0.13,0.19]  0.087 [0.04,0.13]  0.102 [0.01,0.15]   0.36 [0.27,0.48]
  cam 1 model  0.171             0.257             0.300             0.242             0.132             0.038              (alpha 1.34, A 1.15)
  cam 1 total area 0.83 [0.69, 0.93]  (expected ~1.15 x ZL fraction)
  cam 2 fit    0.099 [0.05,0.13]  0.172 [0.15,0.21]  0.170 [0.13,0.20]  0.098 [0.06,0.13]  0.000 [0.00,0.00]  0.039 [0.00,0.11]   0.42 [0.29,0.55]
  cam 2 model  0.237             0.335             0.317             0.176             0.068             0.015              (alpha 1.34, A 1.15)
  cam 2 total area 0.58 [0.51, 0.70]  (expected ~1.15 x ZL fraction)
  cam 3 fit    0.071 [0.03,0.11]  0.210 [0.16,0.24]  0.156 [0.12,0.21]  0.025 [0.00,0.07]  0.108 [0.03,0.17]  0.149 [0.04,0.24]   0.75 [0.62,0.98]
  cam 3 model  0.314             0.397             0.278             0.117             0.035             0.007              (alpha 1.34, A 1.15)
  cam 3 total area 0.72 [0.55, 0.89]  (expected ~1.15 x ZL fraction)
  cam 4 fit    0.180 [0.15,0.20]  0.164 [0.13,0.20]  0.104 [0.09,0.14]  0.057 [0.02,0.08]  0.045 [0.01,0.11]  0.016 [0.00,0.10]   0.48 [0.38,0.59]
  cam 4 model  0.405             0.423             0.224             0.074             0.019             0.004              (alpha 1.34, A 1.15)
  cam 4 total area 0.57 [0.53, 0.71]  (expected ~1.15 x ZL fraction)

```

### Reading

**Detection, per camera vs ~127 null shifts:** 15.1σ, 12.6σ, 9.3σ, 13.9σ;
about 25σ combined.

**Timing.** Δt = −10 s, 68% −10 … 0 s (profile and bootstrap agree,
on a 10-s grid).
- The VIRGO mid-point stamp convention is confirmed: start or end
  stamps (±30 s) are excluded.
- So the absolute time base is good to about 10 s.

**Shape, camera 1 (18°).**
- α = 1.0, 68% 0.8–1.2.
- The Kelsall smooth-cloud α = 1.34 is at Δχ² = 2.3 (~1.5σ).
- Compact responses (α ≥ 1.8) are excluded at Δχ² > 7.
- Normalised to unit area, the non-negative bins (0–60 … 900–2000 s)
  are 0.11, 0.20, 0.25, 0.20, 0.10, against model 0.15, 0.22, 0.26,
  0.21, 0.11. The **first depth profile matches the smooth-cloud shape
  within errors out to ~2 AU.**
- The 2000–4000 s bin stays unreliable (low-frequency common mode).

**Cameras 3 and 4 (66°, 90°).**
- α runs to the grid edge (0.4): the model wants **more extended
  response at high latitude** than the Kelsall vertical profile allows.
- Camera 4 bin fractions beyond 420 s: 0.10, 0.08, 0.03, against model
  0.06, 0.02, 0.003.
- Either the dust extends higher above the plane than in Kelsall, or the
  far bins of the high-latitude cameras are contaminated by low-frequency
  systematics. The next step is a model with a free vertical-scale
  parameter (instead of misusing α), plus injection–recovery.

**Amplitude per unit floor:** 0.78, 0.58, 0.50, 0.49. That implies
zodiacal fractions of the floor of about 0.68, 0.50, 0.43, 0.43 (÷1.15),
falling with latitude. Candidates: the ZL share of the background, and
viewing-angle coherence. Check against Kelsall-predicted brightness.

**Earthshine E:** 0.35–0.75, lower than S27 alone (sector-to-sector
scattered-light geometry varies; one shared E per camera is a
simplification).

## Fine timing (`timing.py`), sectors 27–38

2-s grid; χ² minimised over α, A and E.

| | Δt [s] | Δχ² = 1 interval [s] |
|---|---|---|
| camera 1 | +3.6 | −6.5 … +12.6 |
| camera 2 | −8.4 | −23.5 … +6.3 |
| camera 3 | +24.0 | +5.5 … +41.6 |
| camera 4 | −16.2 | −24.0 … −8.8 |
| **combined** | **−5.9** | **−10.5 … −0.1** |

Per sector (cameras combined):
- The median is +0.7 s, but the χ² scatter is 57 for 11 degrees of
  freedom.
- Sector 34 gives +116 s with the red channel, but only +18 s with TSI.
- Per sector and per camera, single estimates scatter by ±200 s, and red
  and TSI disagree for the same sector.

**Reading:**
- Per-sector and per-camera Δχ² = 1 intervals are too narrow. Timing and
  shape trade off at ~4σ per sector-camera, and α runs to the grid edges.
- There is no evidence of a VIRGO red clock jump: TSI does not confirm
  sector 34.
- The camera 3/4 split (+24 vs −16 s) is the shape–timing degeneracy
  again, for the poorly modelled high-latitude cameras.
- The combined Δt ≈ −6 s stands, but its ±5 s is probably
  underestimated. Realistic errors come from injection–recovery below.

## Injection–recovery v1 (`injection.py`, per-window |Y|² weights)

Sectors 27–38, 4 cameras; ~10 usable trials per case; A_inj = 0.7, Δt_inj = 0.
Raw output:

```
injection-recovery on 27-38, cameras (1, 2, 3, 4), 16 null shifts per case, A_inj = 0.7, dt_inj = 0

== model (alpha 1.34, bump 0.0): 10 trials
  dt: median +5.0 s, scatter (16-84%) +0 .. +15 s
  cam 1: alpha 2.2 [1.5,2.6] (true 1.34); A 0.47 [0.41,0.50] (true 0.7); E -0.04 (true 0)
     bins true 0.104 0.156 0.183 0.148 0.081 0.023
     bins rec  0.068 0.127 0.140 0.089 0.039 0.041
     bins 68%  0.027 0.026 0.028 0.025 0.026 0.052
  cam 2: alpha 1.8 [1.5,2.5] (true 1.34); A 0.50 [0.46,0.55] (true 0.7); E +0.05 (true 0)
     bins true 0.144 0.204 0.193 0.107 0.041 0.009
     bins rec  0.088 0.190 0.145 0.049 0.025 0.038
     bins 68%  0.025 0.033 0.033 0.034 0.035 0.090
  cam 3: alpha 2.7 [1.6,4.0] (true 1.34); A 0.49 [0.46,0.54] (true 0.7); E -0.01 (true 0)
     bins true 0.191 0.242 0.170 0.071 0.022 0.004
     bins rec  0.185 0.185 0.125 0.035 0.044 0.079
     bins 68%  0.042 0.029 0.034 0.030 0.033 0.081
  cam 4: alpha 3.5 [1.8,4.0] (true 1.34); A 0.50 [0.48,0.51] (true 0.7); E -0.01 (true 0)
     bins true 0.247 0.258 0.136 0.045 0.012 0.002
     bins rec  0.207 0.205 0.091 0.005 0.004 0.126
     bins 68%  0.011 0.013 0.022 0.024 0.037 0.085

== extended (alpha 0.6, bump 0.0): 10 trials
  dt: median +5.0 s, scatter (16-84%) +0 .. +15 s
  cam 1: alpha 1.3 [0.7,1.8] (true 0.6); A 0.45 [0.38,0.50] (true 0.7); E -0.04 (true 0)
     bins true 0.080 0.127 0.165 0.157 0.111 0.046
     bins rec  0.048 0.103 0.126 0.099 0.060 0.047
     bins 68%  0.026 0.025 0.029 0.025 0.025 0.055
  cam 2: alpha 1.0 [0.9,1.7] (true 0.6); A 0.48 [0.44,0.54] (true 0.7); E +0.04 (true 0)
     bins true 0.122 0.181 0.188 0.123 0.061 0.020
     bins rec  0.070 0.174 0.143 0.061 0.037 0.034
     bins 68%  0.024 0.032 0.034 0.034 0.036 0.090
  cam 3: alpha 1.9 [0.8,3.2] (true 0.6); A 0.48 [0.45,0.54] (true 0.7); E -0.02 (true 0)
     bins true 0.173 0.227 0.172 0.084 0.033 0.009
     bins rec  0.167 0.173 0.129 0.046 0.050 0.076
     bins 68%  0.040 0.030 0.035 0.033 0.037 0.081
  cam 4: alpha 2.7 [1.2,4.0] (true 0.6); A 0.49 [0.46,0.50] (true 0.7); E -0.01 (true 0)
     bins true 0.235 0.249 0.139 0.052 0.018 0.005
     bins rec  0.196 0.194 0.094 0.010 0.006 0.121
     bins 68%  0.013 0.013 0.022 0.027 0.038 0.084

== bump (alpha 1.34, bump 0.05): 10 trials
  dt: median +5.0 s, scatter (16-84%) +0 .. +10 s
  cam 1: alpha 2.0 [1.3,2.6] (true 1.34); A 0.48 [0.41,0.51] (true 0.7); E -0.04 (true 0)
     bins true 0.104 0.156 0.183 0.148 0.116 0.023
     bins rec  0.068 0.127 0.141 0.084 0.069 0.036
     bins 68%  0.026 0.026 0.028 0.025 0.026 0.049
  cam 2: alpha 1.8 [1.4,2.6] (true 1.34); A 0.49 [0.45,0.55] (true 0.7); E +0.05 (true 0)
     bins true 0.144 0.204 0.193 0.107 0.076 0.009
     bins rec  0.084 0.191 0.145 0.044 0.054 0.025
     bins 68%  0.023 0.032 0.034 0.034 0.034 0.088
  cam 3: alpha 2.8 [1.8,3.9] (true 1.34); A 0.49 [0.46,0.54] (true 0.7); E -0.01 (true 0)
     bins true 0.191 0.242 0.170 0.071 0.057 0.004
     bins rec  0.175 0.185 0.124 0.029 0.072 0.061
     bins 68%  0.044 0.032 0.036 0.027 0.048 0.078
  cam 4: alpha 3.7 [2.0,4.0] (true 1.34); A 0.50 [0.48,0.51] (true 0.7); E -0.02 (true 0)
     bins true 0.247 0.258 0.136 0.045 0.047 0.002
     bins rec  0.205 0.207 0.088 0.001 0.033 0.116
     bins 68%  0.019 0.015 0.021 0.021 0.050 0.076

```

**Findings (estimator v1):**
- **A is biased low by ~30%** in every camera and every shape
  (0.47–0.50 vs 0.7). Cause: the noise weights are taken from each
  window's own |Y|², which contains the echo and so correlates with that
  window's |X|².
- **α is biased toward compact.** True 1.34 is recovered as 1.8–3.5; true
  0.6 as 1.0–2.7. The 420–2000 s bins are recovered at ~60%.
- **The 2000–4000 s bin is spurious:** +0.04 to +0.13 recovered when the
  truth is ≤ 0.02. The camera 3 far-bin excess in the real data is
  consistent with this artifact. The camera 4 real 420–2000 s content
  exceeds any recovered injection, so it is not explained.
- **Δt:** +5 s bias, 68% scatter 0 … +15 s, so the realistic error on
  the full-stack timing is ±7 s.
- **E is unbiased.**
- A 0.05 bump at 1500 s is ~1σ per camera: not detectable.

**Fix (v2):** noise weights from the chunk-average spectrum × one scale
factor per window (`noise_weights` in `joint.py`). Re-tested below.

## Injection–recovery v2 (chunk-level noise weights)

```
injection-recovery on 27-38, cameras (1, 2, 3, 4), 8 null shifts per case, A_inj = 0.7, dt_inj = 0

== model (alpha 1.34, bump 0.0): 5 trials
  dt: median +5.0 s, scatter (16-84%) +0 .. +9 s
  cam 1: alpha 1.8 [1.2,1.9] (true 1.34); A 0.56 [0.52,0.69] (true 0.7); E +0.00 (true 0)
     bins true 0.104 0.156 0.183 0.148 0.081 0.023
     bins rec  0.084 0.133 0.152 0.133 0.059 0.028
     bins 68%  0.021 0.024 0.028 0.054 0.043 0.034
  cam 2: alpha 0.8 [0.7,1.3] (true 1.34); A 0.68 [0.65,0.72] (true 0.7); E -0.03 (true 0)
     bins true 0.144 0.204 0.193 0.107 0.041 0.009
     bins rec  0.133 0.217 0.185 0.118 0.163 0.233
     bins 68%  0.018 0.036 0.024 0.022 0.054 0.091
  cam 3: alpha 0.4 [0.4,1.8] (true 1.34); A 0.70 [0.62,0.74] (true 0.7); E -0.00 (true 0)
     bins true 0.191 0.242 0.170 0.071 0.022 0.004
     bins rec  0.177 0.217 0.217 0.028 0.106 0.121
     bins 68%  0.053 0.056 0.040 0.050 0.073 0.080
  cam 4: alpha 2.0 [0.5,4.0] (true 1.34); A 0.67 [0.65,0.71] (true 0.7); E +0.01 (true 0)
     bins true 0.247 0.258 0.136 0.045 0.012 0.002
     bins rec  0.261 0.280 0.096 0.043 0.072 0.052
     bins 68%  0.023 0.022 0.033 0.029 0.055 0.046

== extended (alpha 0.6, bump 0.0): 5 trials
  dt: median +5.0 s, scatter (16-84%) +5 .. +14 s
  cam 1: alpha 1.0 [0.5,1.1] (true 0.6); A 0.54 [0.48,0.63] (true 0.7); E +0.01 (true 0)
     bins true 0.080 0.127 0.165 0.157 0.111 0.046
     bins rec  0.065 0.098 0.136 0.138 0.085 0.054
     bins 68%  0.019 0.025 0.028 0.052 0.042 0.044
  cam 2: alpha 0.4 [0.4,0.5] (true 0.6); A 0.68 [0.62,0.70] (true 0.7); E -0.03 (true 0)
     bins true 0.122 0.181 0.188 0.123 0.061 0.020
     bins rec  0.120 0.190 0.184 0.132 0.180 0.244
     bins 68%  0.019 0.038 0.023 0.023 0.055 0.092
  cam 3: alpha 0.4 [0.4,1.3] (true 0.6); A 0.67 [0.59,0.69] (true 0.7); E +0.00 (true 0)
     bins true 0.173 0.227 0.172 0.084 0.033 0.009
     bins rec  0.167 0.204 0.215 0.041 0.107 0.122
     bins 68%  0.058 0.061 0.041 0.052 0.075 0.083
  cam 4: alpha 2.2 [0.5,4.0] (true 0.6); A 0.65 [0.62,0.68] (true 0.7); E +0.02 (true 0)
     bins true 0.235 0.249 0.139 0.052 0.018 0.005
     bins rec  0.263 0.255 0.092 0.048 0.071 0.048
     bins 68%  0.019 0.028 0.032 0.033 0.057 0.045

== bump (alpha 1.34, bump 0.05): 5 trials
  dt: median +5.0 s, scatter (16-84%) +0 .. +9 s
  cam 1: alpha 1.8 [1.3,1.9] (true 1.34); A 0.56 [0.51,0.68] (true 0.7); E +0.00 (true 0)
     bins true 0.104 0.156 0.183 0.148 0.116 0.023
     bins rec  0.084 0.133 0.152 0.131 0.097 0.029
     bins 68%  0.021 0.025 0.028 0.053 0.044 0.033
  cam 2: alpha 0.8 [0.7,1.3] (true 1.34); A 0.67 [0.64,0.72] (true 0.7); E -0.03 (true 0)
     bins true 0.144 0.204 0.193 0.107 0.076 0.009
     bins rec  0.133 0.217 0.186 0.115 0.201 0.229
     bins 68%  0.018 0.036 0.024 0.022 0.054 0.090
  cam 3: alpha 0.4 [0.4,1.9] (true 1.34); A 0.70 [0.61,0.73] (true 0.7); E -0.00 (true 0)
     bins true 0.191 0.242 0.170 0.071 0.057 0.004
     bins rec  0.178 0.217 0.218 0.024 0.148 0.118
     bins 68%  0.052 0.057 0.039 0.047 0.081 0.073
  cam 4: alpha 2.2 [0.7,4.0] (true 1.34); A 0.67 [0.65,0.70] (true 0.7); E +0.01 (true 0)
     bins true 0.247 0.258 0.136 0.045 0.047 0.002
     bins rec  0.260 0.280 0.096 0.037 0.113 0.052
     bins 68%  0.023 0.022 0.032 0.028 0.057 0.046

```

**Findings (v2):**
- **A is unbiased for cameras 2–4** (0.65–0.70 vs 0.7). Camera 1 is
  0.56 [0.52, 0.69]; only 5 trials, so it may still be biased.
- **The 0–900 s bins are recovered within ~1σ in all cameras.**
- **Bins ≥ 900 s are unreliable** (spurious +0.1–0.2 for cameras 2–3).
  They need low-frequency work.
- **Per-camera α scatters widely** (camera 3, true 1.34: 0.4–1.8;
  camera 4: 0.5–4.0). The real high-latitude "extended response"
  (α at the grid edge) is therefore **not significant**, and the real
  camera 3 far-bin pattern matches the injected artifacts.
- **Δt:** +5 s bias (possibly the 5-s grid), ±5 s scatter.

## Full 20-s stack with the corrected estimator (v2)

| cam | lat | A per unit floor | Earthshine E | A / null σ |
|---|---|---|---|---|
| 1 | 18° | **1.18** [1.02, 1.27] | 0.72 | 12.4 |
| 2 | 42° | 0.92 [0.81, 0.96] | 0.77 | 14.6 |
| 3 | 66° | 0.67 [0.59, 0.74] | 0.86 | 9.9 |
| 4 | 90° | 0.58 [0.52, 0.62] | 0.79 | 13.1 |

**Δt = 0 s** (bootstrap ±10 s).

**Camera 1 in absolute units** (per unit quiet floor), against the
Kelsall smooth-cloud model with A = 1.15 (i.e. the whole quiet floor is
zodiacal light):

| bin [s] | 0–60 | 60–180 | 180–420 | 420–900 | 900–2000 |
|---|---|---|---|---|---|
| measured | 0.13 ± 0.03 | 0.21 ± 0.04 | 0.28 ± 0.03 | 0.26 ± 0.05 | 0.14 ± 0.09 |
| Kelsall model | 0.17 | 0.26 | 0.30 | 0.24 | 0.13 |

- **The near-ecliptic depth profile matches the standard smooth-cloud
  model in absolute terms**, bin by bin, out to ~2 AU.
- Total area: 1.03 [0.92, 1.23] vs 1.15. Near the ecliptic,
  essentially all of the quiet background is solar-modulated zodiacal
  light.
- **Do not trust the parametric α** (camera 1: 0.6 [0.4, 0.8]). It is
  scattered and biased in injections; the bins are the reliable product.

**Amplitude vs latitude:** 1.18 → 0.92 → 0.67 → 0.58. The zodiacal share
of the quiet background falls toward the pole, as expected: ZL is
fainter there, and unresolved starlight and diffuse galactic light are a
larger share. Viewing-angle coherence loss may contribute.

**Earthshine is now consistent across cameras: E ≈ 0.72–0.92**, i.e. the
scattered-light excess is nearly fully solar-modulated.

## Joint 3-D fit: radial index α and vertical scale v (`vertical.py`)

- Fit: sectors 27–38, all four cameras, shared (α, v), Δt = 0, per-camera
  A and E.
- v scales the height of the Kelsall fan profile (v = 1: COBE model;
  v > 1: dust extends higher above the plane).

| | result |
|---|---|
| best | α = 0.6, v = 1.00 |
| α, 68% (profile and bootstrap agree) | 0.4 … 1.2 |
| **v, 68%** | **1.00 … 1.25** |
| v ≤ 0.75 | excluded, Δχ² > 11 |
| v ≥ 1.75 | excluded, Δχ² > 10 |
| Kelsall point (α 1.4, v 1.0) | Δχ² = 8.2 |
| α 1.4 at v = 1.25 | Δχ² = 2.4 |

- **α and v are degenerate along a valley** from (0.6, 1.0) to
  (1.0–1.4, 1.25). The data accept either the Kelsall radial index with a
  vertical extent ~25% larger, or the Kelsall vertical profile with a
  flatter radial falloff.
- **The vertical extent is measured to ~±15% and is consistent with the
  COBE/Kelsall model.** There is no evidence for a strongly extended
  high-latitude cloud. That agrees with injection–recovery v2, which
  showed the single-camera "extension" was not significant.
- Per-camera amplitudes at the joint best: 1.18, 0.88, 0.69, 0.57.
- Caveats:
  - (α, v) recovery has not yet been injection-tested. Single-camera α
    was biased and scattered.
  - The 2-min sectors (including the northern hemisphere) will tighten
    the valley.

## Full archive with per-sector geometry (`geometry.py`, Hong phase function)

Data: 20-s sectors 27–38 plus 2-min sectors 1–26, all four cameras
(152 sector-camera products, ~14,600 windows). Each sector-camera now
uses its actual sightline: TESS year-2 sectors 14–16 and 24–26 were
pointed further north.

**Joint fit (all data):**
- **Detection per camera:** 21.4σ, 21.7σ, 17.3σ, 22.0σ (~150 nulls
  each); **~41σ combined**.
- **Δt = −10 s** (profile and bootstrap both −10 on the 10-s grid).
  Consistent with the ±5–7 s realistic error and +5 s estimator bias
  from injection–recovery.
- **Camera 1: α = 1.4** [1.4, 1.6]. Bins per unit floor: 0.115, 0.217,
  0.245, 0.196, 0.147, against the Kelsall model × 0.8 (0.128, 0.195,
  0.235, 0.200, 0.118). Shape matches out to ~2 AU; zodiacal fraction of
  the quiet floor ≈ 0.8.
- Amplitudes per unit floor: 0.90, 0.85, 0.80, 0.67.

**Shared radial index α and vertical scale v:**

| data | α (68%) | v (68%) | Kelsall (1.4, 1.0) Δχ² |
|---|---|---|---|
| **all** | **1.4 [1.4–1.6]**, bootstrap 1.0–1.6 | **1.00**, bootstrap 0.75–1.00 | **0.0** |
| south (1–13 2-min + 27–38) | 1.6, bootstrap 1.4–2.0 | 1.25, bootstrap 1.25–1.50 | 9.2 |
| north, standard pointing (17–23) | 1.4, bootstrap 0.6–2.0 | 1.0, bootstrap 0.75–1.25 | 0.0 |
| north incl. far-north pointings (14–26) | 1.6 | 0.5 (grid edge) | 60 |

Reading:
1. **The full TESS archive reproduces the COBE/Kelsall smooth cloud:**
   radial index ≈ 1.4 and the Kelsall vertical profile, measured by
   light-travel delay rather than brightness modelling.
2. **The far-north pointings still misfit.** In those sectors cameras 3–4
   look past the pole at 60–85° from the Sun. The likely cause is
   viewing-angle coherence: dust on those oblique sightlines sees the Sun
   from directions far from VIRGO's, so its echo is partly incoherent.
   That suppresses the far response and mimics a thinner cloud. The
   model has no coherence factor yet; the scattering phase function is
   the other candidate.
3. **Possible north/south difference** (south v ≈ 1.25–1.5, north
   standard ≈ 0.75–1.25). Only ~1–2σ. It is the kind of signature the
   ~2° tilt of the cloud's symmetry plane would produce. It needs a tilt
   model and an injection test before it means anything.
