"""
lsl_hr_bridge.py
================
Publishes this project's live HR/HRV output onto LSL, so external tools
(e.g. a separate BeamNG driving-simulation project) can consume it via
standard LSL stream discovery — with zero changes to `stream.py` or any
other file in this project.

Why this exists
----------------
`stream.py` listens on UDP, buffers PPG/EDA samples, and every
`stride_sec` computes a window of features (HR derived from PPG IBI, HRV
LF/HF, IBI mean/stdev, ...) — but only writes them to
`processed_data_live.csv`. It has no way to hand that data to another,
unrelated project in real time.

This script is a drop-in alternative entry point: it reuses (imports, does
not modify) this project's own `load_config`, `parse_packet`, `append_csv`,
`buffer_to_df` (from `stream.py`) and `extract_window_features` (from
`signal_utils.py`), runs the exact same UDP-ingestion + windowed-feature
loop, and — in addition to still writing `processed_data_live.csv` exactly
as `stream.py` does — pushes each computed sample onto two LSL outlets:

    "HR-Bridge"          (1 channel:  bpm)
    "Variability-Bridge" (3 channels: ibi_mean_ms, ibi_stdev_ms, lf/hf)

Any LSL consumer that resolves streams and routes by name containing "HR"
(a common convention) will pick up "HR-Bridge" automatically — no shared
filesystem path, no knowledge of this project's folder layout, and no
coupling to what machine/process is consuming it. LSL discovery works over
the local network, so the consumer can be a completely separate,
standalone project.

NOTE on naming — two collisions to avoid with lsl_receiver.py's
_metric_route(), which routes by substring match on the (uppercased)
stream name:
  1. NOT "EmotiBit-HR": the word "EmotiBit" itself contains the substring
     "IBI" (E-M-O-T-"I-B-I"-T), which trips the IBI-stream exclusion rule
     (`if "IBI" in n: return None`) before the "HR" check ever runs,
     silently skipping the stream entirely.
  2. NOT "HRV-...": any name starting with "HRV" also contains the literal
     substring "HR", so it would be routed and consumed as an HR stream —
     its first channel (ibi_mean_ms, e.g. ~800) would get pushed into the
     HR buffer as if it were a BPM value, corrupting real HR readings.
  "HR-Bridge" / "Variability-Bridge" avoid both collisions: the former
  matches "HR" as intended, the latter contains neither "HR", "IBI",
  "EDA", nor "GSR", so _metric_route() correctly skips it (until a future
  consumer explicitly asks for it by name).

Run this INSTEAD OF stream.py / stream_timesync.py — only one process can
bind the UDP port at a time:

    python lsl_hr_bridge.py

Prerequisites:
    pip install pylsl
    (plus requirements.txt: numpy, pandas, scipy, scikit-learn,
     matplotlib, pyyaml)
"""

from __future__ import annotations

import csv
import signal
import socket
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

from run_utils import build_run_folder_name
from signal_utils import FEATURE_COLUMNS, extract_window_features
from stream import append_csv, buffer_to_df, load_config, parse_packet

# ---------------------------------------------------------------------------
# LSL stream names. "HR" appears in the first name so generic HR-routing
# consumers (matching on "HR" in the stream name) pick it up automatically.
# "Variability-Bridge" deliberately does NOT match typical HR/EDA name
# filters, so it's ignored by existing consumers unless a future script
# explicitly asks for it.
#
# Deliberately does NOT contain "EmotiBit" (contains "IBI" as a substring)
# and the HRV name deliberately does NOT start with "HR" (would be
# misrouted as an HR stream). See module docstring for the full
# explanation of both naming collisions.
# ---------------------------------------------------------------------------

HR_STREAM_NAME = "HR-Bridge"
HRV_STREAM_NAME = "Variability-Bridge"


def _make_outlets():
    """Create (hr_outlet, hrv_outlet) pylsl StreamOutlets."""
    try:
        from pylsl import StreamInfo, StreamOutlet
    except ImportError as exc:
        print(
            "Failed to import pylsl. Install it with: pip install pylsl",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    hr_info = StreamInfo(
        name=HR_STREAM_NAME,
        type="HR",
        channel_count=1,
        nominal_srate=0,  # irregular rate — samples pushed on each window
        channel_format="float32",
        source_id="emotibit_lsl_hr_bridge_hr",
    )
    hr_info.desc().append_child_value("unit", "bpm")

    hrv_info = StreamInfo(
        name=HRV_STREAM_NAME,
        type="HRV",
        channel_count=3,
        nominal_srate=0,
        channel_format="float32",
        source_id="emotibit_lsl_hr_bridge_hrv",
    )
    channels = hrv_info.desc().append_child("channels")
    for label in ("ibi_mean_ms", "ibi_stdev_ms", "lf_hf"):
        channels.append_child("channel").append_child_value("label", label)

    return StreamOutlet(hr_info), StreamOutlet(hrv_info)


def main() -> None:
    cfg = load_config()
    stream_cfg = cfg["streaming"]
    proc_cfg = cfg["processing"]

    data_dir = Path(stream_cfg.get("data_dir", "data"))
    unix_ts = int(time.time())
    run_id = build_run_folder_name(unix_ts, stream_cfg.get("run_tag", "") or "")
    run_dir = data_dir / run_id
    raw_dir = run_dir / "raw"
    processed_dir = run_dir / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    print(f"Run folder: {run_dir}")

    skipped = set(stream_cfg["skipped_streams"])
    ppg_stream = stream_cfg["ppg_stream"]
    eda_stream = stream_cfg["eda_stream"]
    save_raw = stream_cfg.get("save_raw", True)
    live_processing = proc_cfg.get("live_processing", True)

    buffers: dict[str, deque] = defaultdict(deque)
    csv_initialized: set[str] = set()
    session_start: float | None = None
    session_start_unix: float | None = None
    last_window_end = -1.0
    window_count = 0

    live_output = processed_dir / "processed_data_live.csv"
    live_output_initialized = False

    window_size = proc_cfg["window_size_sec"]
    stride = proc_cfg["stride_sec"]
    edge_buffer = proc_cfg["edge_buffer_sec"]
    min_reliability = proc_cfg["min_reliability"]
    warmup = edge_buffer + window_size

    print("Creating LSL outlets:", flush=True)
    print(f"  HR : {HR_STREAM_NAME}", flush=True)
    print(f"  HRV: {HRV_STREAM_NAME} (ibi_mean_ms, ibi_stdev_ms, lf/hf)", flush=True)
    hr_outlet, hrv_outlet = _make_outlets()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((stream_cfg["udp_host"], stream_cfg["udp_port"]))
    sock.settimeout(1.0)
    print(f"Listening on {stream_cfg['udp_host']}:{stream_cfg['udp_port']}")
    print("Press Ctrl+C to stop.")

    running = True

    def request_stop(_signum, _frame) -> None:
        nonlocal running
        if not running:
            return
        running = False
        print("\nStopping...", flush=True)
        sock.close()

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, request_stop)

    try:
        while running:
            try:
                binary_data, _ = sock.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break

            sample = parse_packet(binary_data)

            if sample["stream"] in skipped:
                continue

            if sample["stream"] == "UN":
                if save_raw:
                    append_csv(
                        raw_dir / "stream_UN.csv",
                        ["emotibit time", "unix time", "note", "reliability"],
                        {
                            "emotibit time": sample["emotibit_time_ms"],
                            "unix time": sample["unix_time"],
                            "note": sample["note"],
                            "reliability": sample["reliability"],
                        },
                        csv_initialized,
                        "UN",
                    )
                print(f"[UN] {sample['note']}")
                continue

            if sample["value"] is None:
                continue

            if save_raw:
                append_csv(
                    raw_dir / f"stream_{sample['stream']}.csv",
                    ["emotibit time", "unix time", "value", "reliability"],
                    {
                        "emotibit time": sample["emotibit_time_ms"],
                        "unix time": sample["unix_time"],
                        "value": sample["value"],
                        "reliability": sample["reliability"],
                    },
                    csv_initialized,
                    sample["stream"],
                )

            if sample["reliability"] < min_reliability:
                continue
            if sample["stream"] not in (ppg_stream, eda_stream):
                continue

            time_sec = sample["emotibit_time_ms"] / 1000.0
            if session_start is None:
                session_start = time_sec
                session_start_unix = float(sample["unix_time"])
            relative_time = time_sec - session_start
            buffers[sample["stream"]].append((relative_time, sample["value"]))

            keep = window_size + edge_buffer + stride + 5
            for stream_name in list(buffers):
                while buffers[stream_name] and buffers[stream_name][0][0] < relative_time - keep:
                    buffers[stream_name].popleft()

            if not live_processing or relative_time < warmup:
                continue

            while running:
                if last_window_end < 0:
                    window_end = warmup
                else:
                    window_end = last_window_end + stride
                if relative_time < window_end:
                    break

                window_start = window_end - window_size
                ppg_df = buffer_to_df(buffers[ppg_stream], window_start, window_end)
                eda_df = buffer_to_df(buffers[eda_stream], window_start, window_end)

                features = extract_window_features(
                    ppg_df,
                    eda_df,
                    window_start=window_start,
                    window_end=window_end,
                    window_size_sec=window_size,
                    visualize=False,
                )
                if features is not None:
                    features["unix time"] = session_start_unix + features["time"]
                    write_header = not live_output_initialized
                    with live_output.open("a", newline="", encoding="utf-8") as handle:
                        writer = csv.DictWriter(handle, fieldnames=FEATURE_COLUMNS)
                        if write_header:
                            writer.writeheader()
                        writer.writerow(features)
                    live_output_initialized = True

                    hr_bpm = features["hr"]
                    ibi_mean = features["ibi mean"]
                    ibi_stdev = features["ibi stdev"]
                    lf_hf = features["lf/hf"]

                    status = f"[features @ {features['time']:.1f}s] LF/HF={lf_hf:.3f}"
                    if hr_bpm > 0:
                        hr_outlet.push_sample([float(hr_bpm)])
                        status += f"  HR={hr_bpm:.1f} bpm  -> pushed to '{HR_STREAM_NAME}'"
                    else:
                        status += "  HR=n/a (not enough clean beats yet)"
                    print(status)

                    hrv_outlet.push_sample([float(ibi_mean), float(ibi_stdev), float(lf_hf)])

                last_window_end = window_end
                window_count += 1

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        running = False
        try:
            sock.close()
        except OSError:
            pass


if __name__ == "__main__":
    main()
