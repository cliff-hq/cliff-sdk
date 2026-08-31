"""A demo robotic arm, streaming into Cliff.

Synthesizes a plausible 6-joint pick-and-place cycle: sinusoidal joint positions, torque that
tracks acceleration with occasional friction spikes, a gripper that opens and closes once per
cycle, and motor temperature that drifts up under load. One signal, nested rows, no schema
declared anywhere: the rows teach Cliff the shape.

    export CLIFF_TOKEN=ck_…
    export CLIFF_ENDPOINT=https://sense.cliff.ai
    python robot_arm.py --backfill-minutes 60      # an hour of history, then live at 4 Hz
"""

import argparse
import math
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import cliff_sdk  # noqa: E402


def sample(t: float, spike: float) -> dict:
    """One observation of the arm at cycle-time t (seconds). Deterministic apart from noise."""
    cycle = 8.0  # one pick-and-place takes 8s
    phase = (t % cycle) / cycle * 2 * math.pi
    joints = {}
    for j in range(1, 7):
        pos = round(math.sin(phase + j * 0.9) * (1.8 - j * 0.15), 4)
        vel = round(math.cos(phase + j * 0.9) * (1.4 - j * 0.1), 4)
        torque = round(abs(vel) * (0.6 + j * 0.05) + random.gauss(0, 0.02) + spike, 4)
        joints[f"j{j}"] = {"pos": pos, "vel": vel, "torque": torque}
    gripper = round((math.sin(phase * 2) + 1) / 2, 3)  # open→closed→open each cycle
    temp = round(38 + 6 * math.sin(t / 600) + random.gauss(0, 0.3), 2)
    return {
        "joints": joints,
        "gripper": gripper,
        "motor_temp_c": temp,
        "cycle": int(t // cycle),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--signal", default="demo-arm-1")
    ap.add_argument("--hz", type=float, default=4.0, help="live sample rate")
    ap.add_argument("--backfill-minutes", type=int, default=0)
    ap.add_argument("--live-seconds", type=int, default=0, help="0 = stream until interrupted")
    args = ap.parse_args()

    c = cliff_sdk.connect()
    sig = c.signal(args.signal)
    period = 1.0 / args.hz

    if args.backfill_minutes:
        print(f"backfilling {args.backfill_minutes} minutes of {args.signal} …")
        start = datetime.now(timezone.utc) - timedelta(minutes=args.backfill_minutes)
        n = int(args.backfill_minutes * 60 * args.hz)
        for i in range(n):
            t = i * period
            spike = 0.9 if random.random() < 0.001 else 0.0  # a rare friction bind
            sig.emit(sample(t, spike), time=start + timedelta(seconds=t))
        c.close()  # flush the backfill fully before going live
        c = cliff_sdk.connect()
        sig = c.signal(args.signal)
        print(f"backfilled {n} rows")

    print(f"streaming {args.signal} at {args.hz} Hz — ctrl-c to stop")
    t0 = time.monotonic()
    try:
        while True:
            t = time.monotonic() - t0
            if args.live_seconds and t > args.live_seconds:
                break
            spike = 0.9 if random.random() < 0.001 else 0.0
            sig.emit(sample(time.time() % 86400, spike))
            time.sleep(period)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        print("flushed and closed")


if __name__ == "__main__":
    main()
