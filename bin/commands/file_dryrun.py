from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict

from cleo.helpers import option

from bin.commands.base import BaseAppCommand

SUBTITLE = "File Dry Run Module"


class FileDryRunCommand(BaseAppCommand):
    """Run the process for dry run file ingestion"""

    name = "file-dryrun"
    subtitle = SUBTITLE

    options = [
        option(
            "dryrun-file-path",
            None,
            "Path to dry run file.",
            flag=False,
            value_required=True,
        ),
        option(
            "override-sub-population",
            None,
            flag=False,
            value_required=False,
        ),
    ]

    def handle(self) -> int:
        from app.dryrun import dryrun

        file_name = self.option("dryrun-file-path")
        override_sub_population = self._get_override_sub_population()
        asyncio.run(
            dryrun.process_dryrun(
                file_name, override_sub_population=override_sub_population
            )
        )

    def _get_override_sub_population(self) -> Dict[int, int] | None:
        override_sub_population_str = self.option("override-sub-population")
        if not override_sub_population_str:
            return None
        try:
            raw_dict = json.loads(override_sub_population_str)
            override_sub_population = {int(k): v for k, v in raw_dict.items()}
            return override_sub_population
        except Exception as e:
            example_json = '{"736":99}'
            logging.error(
                f"Please pass in correct override-sub-population, e.g: {example_json}.\r\nError parsing override-sub-population: {e}"
            )
            return None
