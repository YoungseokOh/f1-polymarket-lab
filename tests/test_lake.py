from pathlib import Path

from f1_polymarket_lab.storage.lake import LakeWriter


def test_lake_writer_persists_bronze_and_silver(tmp_path: Path) -> None:
    writer = LakeWriter(tmp_path)

    bronze_path = writer.write_bronze("openf1", "sessions", [{"session_key": 1}])
    silver_path = writer.write_silver(
        "f1_sessions", [{"session_key": 1, "session_name": "Practice 2"}]
    )

    assert bronze_path.exists()
    assert silver_path is not None
    assert silver_path.exists()
    manifest_path = (
        tmp_path
        / "lake"
        / "_manifests"
        / "bronze"
        / "openf1"
        / "sessions"
        / f"{bronze_path.stem}.json"
    )
    assert manifest_path.exists()
