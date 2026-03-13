from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from scheduler.models import Account, BotType, IngestionSource


@dataclass
class Config:
    sources: list[IngestionSource]
    accounts: dict[str, Account]
    bot_types: dict[str, BotType]

    def get_effective_interval(self, source: IngestionSource) -> int:
        """Return interval in minutes: source override takes priority over bot type default."""
        if source.schedule_override:
            return source.schedule_override.interval_minutes
        return self.bot_types[source.bot_type_id].default_schedule.interval_minutes


def load_config(data_dir: str | Path = "data") -> Config:
    data_dir = Path(data_dir)

    accounts_raw = json.loads((data_dir / "accounts.json").read_text())
    bot_types_raw = json.loads((data_dir / "bot_types.json").read_text())
    sources_raw = json.loads((data_dir / "ingestion_sources.json").read_text())

    accounts = {a["id"]: Account.model_validate(a) for a in accounts_raw}
    bot_types = {b["id"]: BotType.model_validate(b) for b in bot_types_raw}
    sources = [
        IngestionSource.model_validate(s)
        for s in sources_raw
        if s.get("enabled", True)
    ]

    return Config(sources=sources, accounts=accounts, bot_types=bot_types)
