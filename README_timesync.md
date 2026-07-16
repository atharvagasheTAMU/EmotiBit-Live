# Timesync streaming

`stream_timesync.py` is like `stream.py`, but unix timestamps come from the recording's RD/TL/AK timesync data (same logic as EmotiBitDataParser), not from `time.time()` at packet receive.

## Files

| File | Purpose |
|------|---------|
| `timesync.py` | Parse RD/TL/AK from raw CSV and map EmotiBit ms → unix seconds |
| `stream_timesync.py` | UDP listener that writes corrected unix timestamps |

## Replay workflow (recommended)

Pass the same raw CSV to both terminals so timestamps are correct from the first sensor packet:

```bash
# Terminal 1
python stream_timesync.py --timesync-csv ../recording.csv

# Terminal 2
python replay.py ../recording.csv --speed 50 --max-seconds 60
```

## Live hardware

Omit `--timesync-csv`. The stream builds the map incrementally from incoming RD/TL/AK packets. Early packets may be dropped until the first valid sync is received.

```bash
python stream_timesync.py
```

## Output

Same layout as `stream.py`:

- `data/{experiment_unix_start}/raw/stream_*.csv` — `unix time` column uses experiment time
- `data/{experiment_unix_start}/processed/processed_data_live.csv`

Run folder name uses the experiment start time when a timesync map is available.
