#!/bin/bash
# Download, reduce and compact TESS 20-s sectors, one at a time, committing
# each sector's compact product to the data branch and deleting the stamps.
# Resumable: a sector whose compact files already exist for every camera
# is skipped.
#
# usage: run_sectors.sh targets.csv workdir databranch_checkout sector...
set -u
TARGETS=$1 WORK=$2 DATA=$3
shift 3
HERE=$(cd "$(dirname "$0")/.." && pwd)
PY="$HERE/.venv/bin/python"
CAMS="1 2 3 4"

for S in "$@"; do
    SD=$(printf "%s/s%04d" "$DATA" "$S")
    done_all=1
    for C in $CAMS; do [ -f "$SD/cam$C.npz" ] || done_all=0; done
    if [ $done_all = 1 ] && [ -f "$SD/ephem.npz" ]; then
        echo "sector $S: already done"; continue
    fi
    echo "sector $S: start $(date +%H:%M:%S)"
    RED="$WORK/s$S"
    for C in $CAMS; do
        "$PY" "$HERE/stepA/fetch_reduce.py" "$TARGETS" "$S" "$C" "$RED" 16 > "$WORK/fetch_s${S}_c$C.log" 2>&1
        echo "  cam $C: $(grep -c ' ok\| cached' "$WORK/fetch_s${S}_c$C.log") files, $(grep -c failed "$WORK/fetch_s${S}_c$C.log") failed"
    done
    echo "  reduced $(du -sh "$RED" | cut -f1), $(date +%H:%M:%S)"
    "$PY" "$HERE/stepA/noise.py" "$RED" "$WORK/noise_s$S.npz" > "$WORK/noise_s$S.log" 2>&1 || { echo "  noise failed"; continue; }
    "$PY" "$HERE/stepA/compact.py" sector "$S" "$RED" "$WORK/noise_s$S.npz" "$DATA" > "$WORK/compact_s$S.log" 2>&1 || { echo "  compact failed"; continue; }
    "$PY" "$HERE/stepB/timebase.py" ephem "$DATA" "$S" > /dev/null 2>&1 || echo "  ephem failed"
    (cd "$DATA" && git add -A && git commit -qm "Compact data: sector $S, cameras 1-4

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01SSmC1MH5t31radZp4oBcbF" && git push -q origin data) || echo "  git failed"
    rm -rf "$RED"
    echo "sector $S: done $(date +%H:%M:%S), compact $(du -sh "$SD" | cut -f1)"
done
echo "all done $(date +%H:%M:%S)"
