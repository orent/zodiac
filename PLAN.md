# Proof-of-concept plan: zodiacal-light reflectometry

Goal: show that the TESS sky background carries a **delayed, smeared copy**
of solar intensity variations (VIRGO reference), and extract one
uncalibrated delay profile K(τ) for one line of sight.

Numbers below come from `sensitivity.py`, a Fisher-matrix model with
literature-scale inputs (solar granulation and p-mode spectra from Kallinger
2014 scalings, the Kelsall 1998 dust cloud, and TESS photon and read noise).
Treat them as order-of-magnitude.

## 1. Geometry: what the probe sees

Excess light-travel path for dust at distance s along the line of sight,
with the observer at R = 1 AU:

    τ(s) = (|r_obs + s·û| + s − R) / c

- Anti-solar line of sight: τ = 2s/c, or about 1000 s per AU of depth.
- Camera 1 points at anti-solar longitude, ecliptic latitude ±18° (range
  6–30°). This is the best case:
  - The dust along the line of sight sees nearly the same solar hemisphere
    as VIRGO, so coherence ≈ 1 for granulation and for l ≥ 1 modes.
  - The scattering angle stays at 160–180°, so the phase function is nearly
    constant.
  - Gegenschein backscatter boosts the signal.
- Model kernel at 18° latitude:

  | Quantity                | Value  |
  |-------------------------|--------|
  | Median delay            | 276 s  |
  | 90% of the light within | 1210 s |
  | Mean delay              | 506 s  |

  The density r^-1.34 combined with r^-2 illumination puts most of the light
  within ~0.3 AU of TESS.
- The transfer function H(f) is a low-pass with a large phase lag:

  | f       | \|H\| | arg H     |
  |---------|-------|-----------|
  | 0.3 mHz | 0.73  | −0.62 rad |
  | 1 mHz   | 0.38  | −0.98 rad |
  | 3 mHz   | 0.14  | ≈ −1.4 rad |

  The response falls roughly as 1/f because the kernel has a cusp at τ = 0.
  So frequencies above ~1 mHz mostly measure the nearest shell, and the
  lower frequencies carry the deeper structure.

**PoC signature:** a TESS-vs-VIRGO phase lag of ~0.6–1 rad at 0.3–1 mHz,
growing with frequency. An instantaneous contaminant gives zero lag.

## 2. How much data to sum

In the photon and read-noise limit, SNR² scales with
(number of independent background pixels) × (time). So the requirements
are given in **pixel-days**, at 120 s cadence, sky 120 e⁻/s/px, and 80% of
the background from zodiacal light:

| Target | Pixel-days | Allowed common-mode noise floor, one sector |
|---|---|---|
| 5σ detection, known kernel shape | 2.8 × 10⁴ | ~45 ppm²/µHz (~430 ppm per 2-min cadence) |
| 2-parameter kernel (amplitude + delay scale ±20%), i.e. a parametric depth profile | 1.6 × 10⁵ | ~7 ppm²/µHz (~180 ppm per 2-min cadence) |
| Non-parametric 3 bins (0–300, 300–1000, 1000–4000 s), each at 3σ | 3 × 10⁶ | ~0.4 ppm²/µHz |
| Non-parametric 4 bins | ~10⁷ | not realistic |

Supply of independent pixels:
- SPOC `SAP_BKG` is a local estimate: a trimmed mean of dim pixels near the
  target (Jenkins 2021; the algorithm changed in SPOC R5.0). Assume roughly
  20–40 independent pixels per stamp.
- Camera 1 has on the order of 10³ 2-min stamps per sector, so one
  camera-sector gives roughly 10⁶ pixel-days.

**Conclusion.** Photon noise is not the bottleneck. A single camera-sector
covers detection and the parametric profile with margin. Common-mode
systematics are the real limit: jitter, scattered light, and the background
algorithm all hit every stamp in a camera together and do not average down
across stamps.

Proposed scope:

- **Step A.** One sector, camera 1, all 2-min light curves. Read only a few
  columns (section 4); this is a few GB to download and only MB to keep.
  Measure the combined background PSD in 0.1–2 mHz and compare it with the
  floors above. This tells you how many sectors you need.
- **Step B.** Stack sectors that overlap VIRGO. The kernel is fixed by
  geometry, so camera-1 sectors stack directly.
  - Start with 4–8 sectors. That is enough for the parametric profile with
    room for jackknife and null tests.
  - Use all overlapping sectors for the 3-bin attempt.
  - Keep the two ecliptic hemispheres separate. They are different lines of
    sight, and the north/south comparison is itself a result.

Data source: **20-s "fast" light curves** (`*_fast-lc.fits`, sector 27
onward), resampled to the VIRGO 1-min grid. Notes:
- There are far fewer 20-s targets than 2-min targets, roughly a few hundred
  per camera per sector (check against MAST). At ~30 background pixels each,
  one camera-sector gives ~2 × 10⁵ pixel-days. That is about the
  parametric-profile requirement, so plan to stack several sectors from the
  start.
- 20-s data has no onboard cosmic-ray mitigation, so clip cosmic rays on the
  ground before averaging.
- FFIs are out: their time resolution is too coarse for the delay window.

Data window: VIRGO/SPM is reported to continue to the present. That needs
confirming once the data hosts are reachable; this sandbox currently blocks
them. If it holds, every 20-s sector (S27 → now) overlaps VIRGO.

## 3. Granulation vs. p-modes

Share of the detection SNR², by frequency band:

| Band       | Share of SNR² |
|------------|---------------|
| < 0.5 mHz  | ~70% |
| 0.5–2 mHz  | ~28% |
| 2–4.5 mHz  | ~2%  |
| p-modes alone | ~1% |

**Granulation (and supergranulation and activity at low f) is the probe.**
- It acts like a pseudo-random TDR excitation that you measure instead of
  generate. Since VIRGO records it, its randomness is signal, not noise.
- Raw cross-correlation gives K(τ) ⊗ R_xx(τ). The solar autocorrelation
  R_xx is ~200–400 s wide, comparable to K itself.
- So you must **deconvolve, i.e. prewhiten**: estimate H(f) = S_xy / S_xx
  and fit K(τ) to it. Do not read depth off the raw cross-correlation.
- The red solar spectrum means low frequencies dominate the SNR but carry
  little depth resolution. The high-pass cut f_lo trades systematics for
  SNR: raising it from 0.1 to 0.3 mHz roughly doubles the time needed for
  detection and the delay scale, and makes the 4-bin profile ~9× more
  expensive.

**p-modes are almost useless as a probe, and harmless if handled right.**
- Their total power is about 10 ppm rms, against roughly 35 ppm for
  granulation. They sit where |H| ≈ 0.14, which is ~50× down in power.
- Since H(f) is smooth on 0.5 mHz scales, the 135 µHz comb samples it more
  than densely enough. They add no new information.
- They do damage a time-domain approach:
  - They put 5-minute ringing on the raw cross-correlation, inside the
    delay window.
  - They make adjacent lags collinear in a finite-impulse-response (FIR)
    regression, i.e. an ill-conditioned problem.
  - They produce echoes near 1/Δν ≈ 2 h and 2/Δν ≈ 4 h. Those fall outside
    the 0–2000 s window, so they do not alias into it.
- A frequency-domain estimator weights each bin by S_xx / S_n and handles
  the comb automatically. No notch is needed.
- Their real role is a **timing requirement**: the TESS and VIRGO
  timestamps must agree to ≪ 1/(2π·3 mHz) ≈ 50 s. The low-f phase-lag
  measurement needs better than ~10 s.

## 4. Data reduction and filtering

Columns needed from TESS light-curve files:
- `TIME`, `TIMECORR`, `CADENCENO`
- `SAP_BKG`, `SAP_BKG_ERR`
- `QUALITY`
- `POS_CORR1/2` (or `MOM_CENTR1/2`)
- `SAP_FLUX` (for the leakage veto)
- Header: `CAMERA`, `CCD`, `SECTOR`, `TESSMAG`, `RA_OBJ`/`DEC_OBJ`

Steps:

1. **Time base. This is critical.** TESS `TIME` is barycentric-corrected
   toward each target. That shifts it by up to ±500 s, by a different
   amount for every target.
   - Subtract `TIMECORR` to get spacecraft time.
   - Then refer both TESS and SOHO to emission time at the Sun,
     t − |r_sc − r_☉|/c, using JPL Horizons ephemerides (TESS = −95,
     SOHO = −21). This takes out the ~5 s L1 offset and TESS's ±1 s orbit.
   - Watch for UTC vs. TDB in the VIRGO files.
2. **Quality mask.**
   - Drop cadences with these `QUALITY` bits set: attitude tweak, safe
     mode, coarse point, earth point, Argabrightening, desaturation, manual
     exclude, impulsive outlier, straylight/scattered light. Bit values are
     in lightkurve's `TessQualityFlags`; use the "hard" mask as a start.
   - Also drop ±1 h around momentum dumps and the first ~0.5 d after each
     downlink gap.
3. **Stamp vetting.** Reject any stamp where:
   - its `SAP_BKG` is coherent with its own `SAP_FLUX`, meaning target
     light leaks into the "background" pixels;
   - the in-band PSD is far above the camera median (variable neighbours);
   - the target is bright (Tmag ≲ 8).
   Then regress out `POS_CORR` jitter terms, separately in each segment.
4. **Normalisation.** Divide each stamp's background by its segment median
   to get the fractional variation. Combine stamps on the same camera with
   inverse-variance weights.
5. **Resampling to the VIRGO 1-min grid.**
   - After the time-base correction, the 20-s cadences no longer line up
     with VIRGO's minute boundaries.
   - So for each VIRGO minute, average the TESS 20-s samples weighted by
     their exposure overlap with that minute. Each TESS sample is a 20-s box.
   - This is decimation with a matched box response, not point
     interpolation. Point interpolation would leave TESS with a narrower
     integration window than VIRGO and add a small phase error that grows
     with frequency.
   - Drop any minute that has less than ~2/3 coverage after quality masking.
   - Use the VIRGO red channel (862 nm), which is closest to the TESS band
     (600–1000 nm).
6. **Band limiting.** Use Welch segments of ~4–6 h, with linear detrend and
   a Hann taper. This sets f_lo ≈ 0.1 mHz implicitly. Any extra filter must
   be applied identically to both series.
7. **Solar-correlated contaminants. These are the bias risks; ordinary
   noise only costs time.**
   - **Earthshine and moonshine.** Scattered Earth and Moon light carries
     the solar signal at τ ≈ 0–2 s. It is a "near-end reflection" that is
     indistinguishable from the first dust bin.
     - Mask high-scattered-light periods, or
     - fit a separate τ = 0 term whose amplitude follows the Earth and Moon
       angles.
   - **Solar energetic particle events.** They raise the cosmic-ray rate in
     step with solar activity, so exclude SEP intervals (GOES proton flux).

## 5. Estimator and PoC deliverables

- **Accumulate.** Sum S_xy(f) and S_xx(f) over all segments, stamps and
  sectors. This is "the summing". Then Ĥ = S_xy / S_xx, with error
  ∝ √(S_n / S_xx / N_seg).
- **Fit.** Model K(τ) ≥ 0 with a power-law dust density (amplitude,
  exponent) plus a τ = 0 term. Fit it to Ĥ by weighted least squares in
  frequency.
  - The time-domain equivalent: a FIR regression on lagged VIRGO, summing
    the normal equations, with a positivity constraint and a few log-spaced
    delay bins.
- **Null tests.** Each must give coherence ≈ 0:
  - VIRGO shifted by ±N days;
  - VIRGO from the wrong year;
  - TESS from the wrong camera or hemisphere.
  Also check that the delay scale is consistent between halves of the data
  (jackknife).
- **Success criterion.**
  - Significant coherence in 0.1–2 mHz.
  - A phase lag that fits a dust kernel and rejects τ = 0.
  - A 2-parameter "depth profile" with error bars.

## 6. Open items to verify

- VIRGO/SPM 1-min availability after 2021-05, and its time scale.
- SPOC background algorithm details per sector. How many pixels it uses
  sets N_eff per stamp; the R5.0 change may split the data into two
  separate sets.
- The actual 2-min target count per camera per sector, from
  `all_targets.sqlite`, which already has Camera/CCD.
- The real TESS background noise in 0.1–2 mHz. Step A measures it, and it
  replaces every "excess=1" assumption in `sensitivity.py`.
