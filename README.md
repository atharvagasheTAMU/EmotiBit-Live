# EmotiBit Live

Minimal scripts for EmotiBit UDP streaming, raw CSV capture, and biosignal feature extraction.

```
emotibit-live/
├── config.yaml
├── signal_utils.py
├── stream.py
├── process_offline.py
├── plot_streams.py      # plot HR/BI from a saved run
├── replay.py
├── requirements.txt
└── data/
    └── {unix_timestamp}/     # one folder per run
        ├── raw/              # stream_*.csv
        └── processed/        # feature CSVs
```

## Setup

```bash
cd emotibit-live
pip install -r requirements.txt
```

## Usage

**Live** (EmotiBit streaming to UDP port 12346):

```bash
python stream.py
```

Each run creates a new folder like `data/1783020361/` and prints the path on startup.

**Offline** (from a saved run):

```bash
python process_offline.py --run-dir data/1783020361
```

If `--run-dir` is omitted, the latest timestamp folder under `data/` is used.

**Plot HR/BI** (from raw CSVs in a run folder):

```bash
python plot_streams.py                        # latest run
python plot_streams.py --run-dir data/1783020361
```

Saves `HR.png` and `BI.png` into the run folder.

**Test without hardware** (replay a raw EmotiBit CSV over UDP):

```bash
# Terminal 1
python stream.py

# Terminal 2
python replay.py ../2026-04-20_23-43-02-431082.csv --speed 50 --max-seconds 60
```

Edit `config.yaml` for ports, stream tags, window size, etc.

## Output

| Script | Raw data | Features |
|--------|----------|----------|
| `stream.py` | `data/{timestamp}/raw/stream_*.csv` | `data/{timestamp}/processed/processed_data_live.csv` |
| `plot_streams.py` | reads `raw/stream_HR.csv`, `raw/stream_BI.csv` | `data/{timestamp}/HR.png`, `BI.png` |
| `process_offline.py` | — | `data/{timestamp}/processed/processed_data.csv` |

Original lab scripts in the parent folder are unchanged.
