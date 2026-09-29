import sys, unittest
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from sensor_alerts.contracts import Calibration, Observation


class SensorContractTests(unittest.TestCase):
    def test_observation_keeps_sequence(self):
        item = Observation("D", 3, datetime.now(timezone.utc), 2.0)
        self.assertEqual(item.sequence, 3)

    def test_factor_must_be_positive(self):
        with self.assertRaises(ValueError):
            Calibration("C", "D", 0)


if __name__ == "__main__": unittest.main()
