import os, pyarrow.parquet as pq

def test_parquet_exists():
    path = "data/processed/trips_parquet"
    assert os.path.exists(path), "Parquet output not found"

def test_has_partitions():
    path = "data/processed/trips_parquet"
    # expect year= and month= folders after first run
    parts = [p for p in os.listdir(path) if p.startswith("year=")]
    assert len(parts) > 0, "No year partitions produced"
