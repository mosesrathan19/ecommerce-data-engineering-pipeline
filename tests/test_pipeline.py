from pathlib import Path
from src.pipeline import extract, transform
ROOT = Path(__file__).resolve().parents[1]

def test_quality_and_uniqueness():
    clean, rejected, quality = transform(*extract(ROOT / "data/raw"))
    assert quality["raw_rows"] == 100100
    assert quality["duplicates_removed"] == 100
    assert quality["rejected_rows"] == 625
    assert quality["accepted_rows"] == 99375
    assert clean.order_id.is_unique
    assert not clean.isna().any().any()

def test_completed_only_contributes_revenue():
    clean, _, _ = transform(*extract(ROOT / "data/raw"))
    assert clean.loc[clean.status != "completed", "recognized_revenue"].eq(0).all()
