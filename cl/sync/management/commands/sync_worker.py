"""Django management command that starts a Prefect worker for warehouse sync.

The worker polls the ``sync-pool`` work pool and executes sync flows
in-process, giving them full access to the Django ORM and the warehouse
database connection.

Usage::

    python manage.py sync_worker
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Start a Prefect worker that syncs warehouse data into CL via Django ORM"

    def handle(self, **options):
        import asyncio

        from cl.sync.worker import SyncWorker

        self.stdout.write("Starting sync worker on pool 'sync-pool'...")

        async def main():
            worker = SyncWorker(work_pool_name="sync-pool")
            await worker.start()

        asyncio.run(main())
