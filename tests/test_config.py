from __future__ import annotations

from scheduler.config import load_config
from scheduler.models import IngestionSource, ScheduleOverride


def test_load_config_counts():
    config = load_config("data")
    # src_009 is disabled, so 9 enabled sources
    assert len(config.sources) == 9
    assert len(config.accounts) == 4
    assert len(config.bot_types) == 3


def test_load_config_only_enabled():
    config = load_config("data")
    assert all(s.enabled for s in config.sources)


def test_effective_interval_uses_override(sample_config):
    source = sample_config.sources[0]
    source.schedule_override = ScheduleOverride(interval_minutes=15)
    assert sample_config.get_effective_interval(source) == 15


def test_effective_interval_falls_back_to_bot_type(sample_config):
    source = sample_config.sources[0]
    source.schedule_override = None
    assert sample_config.get_effective_interval(source) == 60
