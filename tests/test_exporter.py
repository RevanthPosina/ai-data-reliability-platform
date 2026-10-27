from adrp.obs.exporter import Connector, Slot, render


def test_render_all_metrics() -> None:
    text = render(
        [Slot("dbz_shop", 1, 1234.0, 5678.0)],
        [Connector("pg-shop", "RUNNING", ("RUNNING",)), Connector("bad", "RUNNING", ("RUNNING", "FAILED"))],
    )
    assert 'adrp_replication_slot_lag_bytes{slot="dbz_shop"} 1234' in text
    assert 'adrp_replication_slot_retained_wal_bytes{slot="dbz_shop"} 5678' in text
    assert 'adrp_replication_slot_active{slot="dbz_shop"} 1' in text
    assert 'adrp_connector_up{connector="pg-shop"} 1' in text
    assert 'adrp_connector_up{connector="bad"} 0' in text
    assert 'adrp_connector_tasks{connector="bad",state="FAILED"} 1' in text
    assert 'adrp_exporter_source_up{source="postgres"} 1' in text


def test_connector_without_tasks_is_not_up() -> None:
    assert Connector("c", "RUNNING", ()).up == 0
    assert Connector("c", "PAUSED", ("PAUSED",)).up == 0


def test_render_with_failed_sources_reports_source_down_and_omits_values() -> None:
    text = render(None, None)
    assert 'adrp_exporter_source_up{source="postgres"} 0' in text
    assert 'adrp_exporter_source_up{source="connect"} 0' in text
    assert "adrp_replication_slot_lag_bytes{" not in text
    assert "adrp_connector_up{" not in text
