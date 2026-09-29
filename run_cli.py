import json, sys
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))
from sensor_alerts.contracts import Calibration, Observation

cal = Calibration("CAL-4", "DEV-8", 1.02)
obs = Observation("DEV-8", 41, datetime.now(timezone.utc), 7.3)
print(json.dumps({"device": obs.device_id, "sequence": obs.sequence, "factor": cal.factor}, ensure_ascii=False))
