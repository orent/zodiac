"""Back-of-envelope sensitivity model for zodiacal-light reflectometry.

Model: TESS background y(t) = Z * (h * x)(t) + n(t), where x is the solar
relative intensity variation (VIRGO reference), h is the zodiacal
impulse response (delay kernel) and n is TESS background noise.

All spectra are one-sided, in fractional units (1/Hz). Every number here
is a rough literature-scale guess; the point is the scaling, not 10%.
"""

import numpy as np

AU_LIGHT_S = 499.005

# ---------------------------------------------------------------- solar PSD

BAND = 0.85      # TESS band vs Kepler band amplitude ratio (redder -> smaller)
NUMAX = 3090e-6
DNU = 135.1e-6


def harvey(f, a, b):
    """Kallinger-style super-Lorentzian; integrates to a**2."""
    xi = 2 * np.sqrt(2) / np.pi
    return xi * a**2 / b / (1 + (f / b) ** 4)


def lorentz(f, f0, width, power):
    return power * (width / 2 / np.pi) / ((f - f0) ** 2 + (width / 2) ** 2)


def solar_psd(f, granulation=True, pmodes=True, activity=True):
    s = np.zeros_like(f)
    if activity:        # supergranulation + active regions, hours timescale
        s += harvey(f, 60e-6 * BAND, 30e-6)
    if granulation:     # Kallinger et al. 2014 scaling relations for numax_sun
        s += harvey(f, 25e-6 * BAND, 0.77e-3)
        s += harvey(f, 25e-6 * BAND, 2.76e-3)
    if pmodes:
        amax = 2.5e-6 * BAND            # radial mode rms amplitude, Kepler band
        env_sigma = 900e-6 / 2.355
        vis = {0: 1.0, 1: 1.54, 2: 0.58, 3: 0.07}
        shift = {0: 0.0, 1: DNU / 2 - 6e-6, 2: -9e-6, 3: DNU / 2 - 15e-6}
        for n in range(12, 32):
            for l, v in vis.items():
                f0 = DNU * (n + 1.4) + shift[l]
                env = np.exp(-0.5 * ((f0 - NUMAX) / env_sigma) ** 2)
                s += lorentz(f, f0, 1.2e-6, amax**2 * v * env)
    return s


# ------------------------------------------------------------ delay kernel

def kelsall_density(x, y, z):
    """Kelsall et al. 1998 smooth cloud, symmetry plane = ecliptic."""
    r = np.sqrt(x * x + y * y + z * z)
    zeta = np.abs(z / r)
    mu = 0.189
    g = np.where(zeta < mu, zeta**2 / (2 * mu), zeta - mu / 2)
    return r**-1.34 * np.exp(-4.14 * g**0.942)


def kernel(lat_deg=18.0, smax=6.0, ns=20000):
    """Line of sight anti-solar in longitude, at ecliptic latitude lat_deg.

    Returns path samples s [AU], delay tau(s) [s], normalized weight w(s) ds.
    Phase function taken as constant (scattering angle stays 160-180 deg).
    """
    b = np.radians(lat_deg)
    s = np.linspace(0, smax, ns + 1)[1:] - smax / ns / 2
    x, z = 1 + s * np.cos(b), s * np.sin(b)
    r = np.hypot(x, z)
    tau = (r + s - 1) * AU_LIGHT_S
    w = kelsall_density(x, 0 * x, z) / r**2
    w /= w.sum()
    return s, tau, w


def transfer(f, tau, w):
    """H(f) is smooth on ~1/(kernel length) scales: evaluate on a coarse
    grid and interpolate onto the fine grid the p-mode lines need."""
    fc = np.linspace(f.min(), f.max(), min(len(f), 800))
    hc = np.exp(-2j * np.pi * np.outer(fc, tau)) @ w
    return np.interp(f, fc, hc.real) + 1j * np.interp(f, fc, hc.imag)


# ------------------------------------------------------------------- noise

def noise_psd(cadence, npix, sky=120.0, zl_frac=0.8, read_noise=10.0, excess=1.0):
    """White fractional noise PSD of a background estimate built from npix
    independent pixels. sky in e-/s/px; TESS 2 s frames, onboard cosmic-ray
    mitigation keeps 8 of every 10 frames. excess multiplies the variance."""
    texp = 0.8 * cadence
    var_e = sky * texp + (texp / 2) * read_noise**2
    frac_var = var_e / (zl_frac * sky * texp) ** 2 / npix * excess
    return 2 * frac_var * cadence


# ------------------------------------------------------------------ fisher

def fisher(f, basis, sx, sn):
    """F_jk = 2T * int Re(B_j B_k^*) Sx/Sn df, returned per second of data."""
    df = np.gradient(f)
    wgt = 2 * sx / sn * df
    return np.real(basis @ (basis.conj() * wgt).T)


def days_for(snr, fisher_per_s):
    return snr**2 / fisher_per_s / 86400


def report(cadence=120.0, npix=1.0, f_lo=0.1e-3, lat=18.0, excess=1.0):
    fn = 0.5 / cadence
    f = np.arange(f_lo, fn, 0.2e-6)
    s, tau, w = kernel(lat)
    h = transfer(f, tau, w)
    sx = solar_psd(f)
    sn = noise_psd(cadence, npix, excess=excess)

    # 1. Detection: amplitude of a known-shape response.
    f_amp = fisher(f, h[None], sx, sn)[0, 0]
    # Share of detection SNR^2 per frequency band.
    contrib = 2 * np.abs(h) ** 2 * sx / sn * np.gradient(f)
    bands = [(0, 0.5e-3), (0.5e-3, 2e-3), (2e-3, 4.5e-3), (4.5e-3, 1)]
    share = [contrib[(f >= a) & (f < b)].sum() / contrib.sum() for a, b in bands]
    sx_nop = solar_psd(f, pmodes=False)
    pmode_share = 1 - (2 * np.abs(h) ** 2 * sx_nop / sn * np.gradient(f)).sum() / contrib.sum()

    # 2. Delay scale: H(f; k) = sum w exp(-2 pi i f k tau). Is k != 0?
    #    dH/dk at k=1 -> sigma(k); 5 sigma on k=1 means "the response is
    #    delayed, not instantaneous" to ~20%.
    dh = -2j * np.pi * f * transfer(f, tau, w * tau)
    fk = fisher(f, np.vstack([h, dh]), sx, sn)              # amplitude + scale, jointly
    cov_k = np.linalg.inv(fk)[1, 1]

    # 3. Coarse nonparametric profile: 4 delay bins, boxcar responses.
    edges = [0, 2 * cadence, 6 * cadence, 14 * cadence, max(3000, 20 * cadence)]
    basis, true = [], []
    for a, b in zip(edges[:-1], edges[1:]):
        m = (tau >= a) & (tau < b)
        true.append(w[m].sum())
        basis.append(np.where(
            np.abs(f) > 0,
            (np.exp(-2j * np.pi * f * a) - np.exp(-2j * np.pi * f * b)) /
            (2j * np.pi * f * (b - a)), 1))
    fb = fisher(f, np.array(basis), sx, sn)
    cov_b = np.linalg.inv(fb)

    print(f"cadence {cadence:.0f}s  pixel-equivalents {npix:g}  f_lo {f_lo*1e3:.2f} mHz  "
          f"excess x{excess:g}  lat {lat:g} deg")
    print(f"  kernel: median delay {np.interp(0.5, np.cumsum(w), tau):.0f}s, "
          f"90% {np.interp(0.9, np.cumsum(w), tau):.0f}s, mean {np.sum(w * tau):.0f}s; "
          f"|H| at 0.3/1/3 mHz = " +
          "/".join(f"{abs(transfer(np.array([x]), tau, w)[0]):.2f}" for x in (0.3e-3, 1e-3, 3e-3)))
    print("  detection SNR^2 share by band  <0.5 / 0.5-2 / 2-4.5 / >4.5 mHz: " +
          " / ".join(f"{x:.0%}" for x in share) + f"   (p-modes alone: {pmode_share:.0%})")
    print(f"  days for 5 sigma detection (known shape): {days_for(5, f_amp):10.3g}")
    print(f"  days for delay scale to 20% (5 sigma):    {25 * cov_k * 1 / 86400:10.3g}")
    worst = max(np.sqrt(cov_b[i, i]) / true[i] for i in range(4))
    print("  4-bin profile, edges " + ",".join(f"{e:g}" for e in edges) + " s; "
          "true weights " + ", ".join(f"{t:.2f}" for t in true))
    print(f"  days for every bin at 3 sigma:            {9 * worst**2 / 86400:10.3g}")
    print()


if __name__ == "__main__":
    # Normalized to ONE independent pixel; results scale as 1/npix while
    # photon/read-noise limited, so read the outputs as pixel-days.
    report(cadence=120, npix=1)
    report(cadence=120, npix=1, f_lo=0.3e-3)
    report(cadence=200, npix=1)
    report(cadence=60, npix=1, lat=45)
