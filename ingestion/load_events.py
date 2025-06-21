# Script to simulate and ingest event logs into the lake
import json
import pandas as pd
from pathlib import Path

def load_event_data(json_path: str, output_path: str):
    with open(json_path, 'r') as f:
        events = json.load(f)
    df = pd.DataFrame(events)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} events to {output_path}")

if __name__ == "__main__":
    base_path = Path(__file__).resolve().parent.parent / "data"
    input_file = base_path / "sample_events.json"
    output_file = base_path / "feature_usage_hourly_sample.csv"
    load_event_data(str(input_file), str(output_file))
