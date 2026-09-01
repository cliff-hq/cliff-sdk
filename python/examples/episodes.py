"""Episodic signals: events, not streams.

Not everything is a 4 Hz feed. A weld happens, a grasp is attempted, a test run finishes — and
between episodes the signal is silent because the world is. The protocol needs nothing special
for this: an episode is just rows, stamped with the time the thing actually happened, sharing
whatever field ties them together. No session objects, no start/stop calls, no schema.

Two patterns, one signal each:

  welds        one row per completed weld — the episode IS the row, emitted at completion,
               `time` = when the weld finished
  grasps       one episode spans several rows — approach, grip samples, outcome — tied
               together by an `episode` field the rows simply share

A note on health: Cliff derives liveness from cadence, so a signal that is quiet between
episodes can read as `stalled` during long idle stretches. That is the honest reading of an
empty line. If idle-is-normal for your station, put many stations' episodes on one signal, or
treat stalled as idle when nothing is scheduled.

    export CLIFF_TOKEN=ck_…
    export CLIFF_ENDPOINT=https://sense.cliff.ai
    python episodes.py --hours 4      # four hours of history, then one live episode of each
"""

import argparse
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import cliff_sdk  # noqa: E402


def weld_row(finished: datetime) -> dict:
    """One completed weld: the whole episode in a single row."""
    duration_ms = int(random.gauss(840, 60))
    peak_a = round(random.gauss(9200, 300), 1)
    porous = random.random() < 0.04  # the defect an evaluator would go looking for
    return {
        "weld": uuid.uuid4().hex[:8],
        "duration_ms": duration_ms,
        "peak_current_a": peak_a,
        "energy_j": round(peak_a * duration_ms * 0.023, 1),
        "result": "porous" if porous else "ok",
        "electrode_welds_since_dress": random.randint(0, 400),
    }, finished


def grasp_rows(started: datetime) -> list:
    """One grasp attempt as an episode of rows: approach, grip samples, outcome.

    The `episode` field is nothing but a value the rows share — grouping by it happens at query
    time, which is exactly where it belongs.
    """
    ep = uuid.uuid4().hex[:8]
    ok = random.random() < 0.85
    t = started
    rows = [({"episode": ep, "phase": "approach", "target": {"x": round(random.uniform(-0.4, 0.4), 3),
                                                            "y": round(random.uniform(0.2, 0.6), 3)}}, t)]
    for _ in range(random.randint(3, 6)):
        t += timedelta(milliseconds=random.randint(120, 300))
        rows.append(({"episode": ep, "phase": "grip",
                      "force_n": round(random.gauss(14 if ok else 6, 1.5), 2),
                      "slip": (not ok) and random.random() < 0.5}, t))
    t += timedelta(milliseconds=random.randint(200, 500))
    rows.append(({"episode": ep, "phase": "outcome", "success": ok,
                  "duration_ms": int((t - started).total_seconds() * 1000)}, t))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=4, help="hours of episode history to emit")
    ap.add_argument("--welds-signal", default="demo-weld-station")
    ap.add_argument("--grasps-signal", default="demo-grasp-attempts")
    args = ap.parse_args()

    c = cliff_sdk.connect()
    welds = c.signal(args.welds_signal)
    grasps = c.signal(args.grasps_signal)

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=args.hours)

    # History: welds arrive in work bursts with idle gaps between them — the shape that makes a
    # signal episodic in the first place.
    n_welds = 0
    t = start
    while t < now:
        for _ in range(random.randint(8, 20)):  # a burst of parts
            t += timedelta(seconds=random.randint(20, 45))
            if t >= now:
                break
            row, at = weld_row(t)
            welds.emit(row, time=at)
            n_welds += 1
        t += timedelta(minutes=random.randint(10, 35))  # idle: a changeover, a break

    n_grasps = 0
    t = start
    while t < now:
        for row, at in grasp_rows(t):
            grasps.emit(row, time=at)
        n_grasps += 1
        t += timedelta(seconds=random.randint(45, 240))

    c.close()
    print(f"emitted {n_welds} welds and {n_grasps} grasp episodes over {args.hours}h")

    # One live episode of each, timestamped now — what a station actually does at the moment
    # the thing happens.
    c = cliff_sdk.connect()
    row, at = weld_row(datetime.now(timezone.utc))
    c.signal(args.welds_signal).emit(row, time=at)
    for row, at in grasp_rows(datetime.now(timezone.utc)):
        c.signal(args.grasps_signal).emit(row, time=at)
        time.sleep(0.05)
    c.close()
    print("plus one live weld and one live grasp — done")


if __name__ == "__main__":
    main()
