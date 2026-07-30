import json
from argparse import ArgumentParser

from django.core.management.base import BaseCommand, CommandError
from django.db import DEFAULT_DB_ALIAS

from cacheops.reaper import reap_conjs


class Command(BaseCommand):
    help = "Removes stale cacheops conjunction members within hard limits."

    def add_arguments(self, parser: ArgumentParser):
        parser.add_argument("--chunk-size", type=int, default=1000)
        parser.add_argument("--min-conj-set-size", type=int, default=1000)
        parser.add_argument("--database-alias", default=DEFAULT_DB_ALIAS)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--max-conj-sets", type=int, default=1)
        parser.add_argument("--max-members", type=int, default=1000)
        parser.add_argument("--time-budget-seconds", type=float, default=1)

    def handle(
        self,
        chunk_size,
        min_conj_set_size,
        database_alias,
        dry_run,
        max_conj_sets,
        max_members,
        time_budget_seconds,
        **kwargs,
    ):
        try:
            result = reap_conjs(
                chunk_size=chunk_size,
                min_conj_set_size=min_conj_set_size,
                using=database_alias,
                dry_run=dry_run,
                max_conj_sets=max_conj_sets,
                max_members=max_members,
                time_budget_seconds=time_budget_seconds,
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        output = json.dumps(result, sort_keys=True)
        if result["errors"]:
            raise CommandError(output)
        self.stdout.write(output)
