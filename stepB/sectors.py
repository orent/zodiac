"""Sector groups by pointing (from geometry.csv), shared by the analysis
scripts.

STANDARD   anti-sunward strip at ecliptic latitude ~ +-18/42/66/90 deg
ECLIPTIC   all four cameras along the ecliptic
FAR_NORTH  camera 1 at ~+50 deg, cameras 3-4 past the pole (excluded from
           shape fits: the model does not describe them)
"""

FAR_NORTH = {14, 15, 16, 24, 25, 26, 40, 41, 52, 53, 74, 75, 76, 77, 78, 79}
ECLIPTIC = {42, 43, 44, 45, 46, 70, 71, 72}
STANDARD = set(range(1, 91)) - FAR_NORTH - ECLIPTIC
SOUTH = set(range(1, 14)) | set(range(27, 40)) | set(range(61, 70)) | set(range(87, 91))
NORTH = STANDARD - SOUTH
