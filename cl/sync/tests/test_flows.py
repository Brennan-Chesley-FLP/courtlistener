"""Tests for cl.sync.flows — the Prefect flow and task logic.

The warehouse database doesn't exist in the test environment, so all
warehouse cursor operations are mocked.  The Django ORM operations use
the real test database.
"""

from datetime import date
from unittest import mock
from unittest.mock import MagicMock, call, patch

from cl.search.docket_sources import DocketSources
from cl.search.factories import CourtFactory, DocketFactory
from cl.search.models import Docket
from cl.sync.flows import (
    CHUNK_SIZE,
    _fetch_chunk,
    _get_pending_count,
    _read_sync_state,
    _write_sync_state,
    _writeback_cl_id,
    sync_table,
)
from cl.sync.mappers import TABLE_KEYS
from cl.tests.cases import TestCase


def _mock_cursor_ctx(mock_conn, rows=None, description=None):
    """Configure a mock warehouse connection to return rows from a cursor."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = rows[0] if rows else None
    mock_cursor.fetchall.return_value = rows or []
    if description is not None:
        col_objs = [MagicMock(name=col) for col in description]
        for obj, name in zip(col_objs, description):
            obj.name = name
        mock_cursor.description = col_objs
    mock_conn.cursor.return_value = mock_cursor
    return mock_cursor


class ReadSyncStateTest(TestCase):
    @patch("cl.sync.flows.connections")
    def test_returns_zero_when_no_state(self, mock_connections):
        mock_conn = MagicMock()
        mock_connections.__getitem__.return_value = mock_conn
        _mock_cursor_ctx(mock_conn, rows=None)

        result = _read_sync_state("dockets")
        self.assertEqual(result, 0)

    @patch("cl.sync.flows.connections")
    def test_returns_stored_value(self, mock_connections):
        mock_conn = MagicMock()
        mock_connections.__getitem__.return_value = mock_conn
        _mock_cursor_ctx(mock_conn, rows=[(42,)])

        result = _read_sync_state("dockets")
        self.assertEqual(result, 42)


class WriteSyncStateTest(TestCase):
    @patch("cl.sync.flows.connections")
    def test_upserts_state(self, mock_connections):
        mock_conn = MagicMock()
        mock_connections.__getitem__.return_value = mock_conn
        mock_cursor = _mock_cursor_ctx(mock_conn)

        _write_sync_state("dockets", 100)

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("INSERT INTO warehouse.sync_state", sql)
        self.assertIn("ON CONFLICT", sql)
        params = mock_cursor.execute.call_args[0][1]
        self.assertEqual(params, ["dockets", 100])


class GetPendingCountTest(TestCase):
    @patch("cl.sync.flows.connections")
    def test_returns_count(self, mock_connections):
        mock_conn = MagicMock()
        mock_connections.__getitem__.return_value = mock_conn
        _mock_cursor_ctx(mock_conn, rows=[(5,)])

        result = _get_pending_count("dockets", 10)
        self.assertEqual(result, 5)


class FetchChunkTest(TestCase):
    @patch("cl.sync.flows.connections")
    def test_returns_dicts(self, mock_connections):
        mock_conn = MagicMock()
        mock_connections.__getitem__.return_value = mock_conn
        _mock_cursor_ctx(
            mock_conn,
            rows=[("ala", "SC-001", 1, "Smith v. Jones")],
            description=["court_id", "docket_number", "version_provenance", "case_name"],
        )

        result = _fetch_chunk("dockets", 0, 500, 0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["court_id"], "ala")
        self.assertEqual(result[0]["docket_number"], "SC-001")
        self.assertEqual(result[0]["version_provenance"], 1)


class WritebackTest(TestCase):
    @patch("cl.sync.flows.connections")
    def test_writeback_docket(self, mock_connections):
        mock_conn = MagicMock()
        mock_connections.__getitem__.return_value = mock_conn
        mock_cursor = _mock_cursor_ctx(mock_conn)

        row = {"court_id": "ala", "docket_number": "SC-001"}
        _writeback_cl_id("dockets", row, 42)

        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("UPDATE courtlistener.staged_dockets", sql)
        self.assertIn("SET courtlistener_id = %s", sql)
        params = mock_cursor.execute.call_args[0][1]
        self.assertEqual(params, [42, "ala", "SC-001"])

    @patch("cl.sync.flows.connections")
    def test_writeback_opinions(self, mock_connections):
        mock_conn = MagicMock()
        mock_connections.__getitem__.return_value = mock_conn
        mock_cursor = _mock_cursor_ctx(mock_conn)

        row = {
            "court_id": "ala",
            "docket_number": "SC-001",
            "cluster_date_filed": "2024-01-15",
            "type": "020lead",
            "author_str": "Judge Smith",
        }
        _writeback_cl_id("opinions", row, 99)

        sql = mock_cursor.execute.call_args[0][0]
        self.assertIn("UPDATE courtlistener.staged_opinions", sql)
        params = mock_cursor.execute.call_args[0][1]
        self.assertEqual(
            params,
            [99, "ala", "SC-001", "2024-01-15", "020lead", "Judge Smith"],
        )

    def test_all_tables_have_keys(self):
        """Every table in MAPPER_REGISTRY has a TABLE_KEYS entry."""
        from cl.sync.mappers import MAPPER_REGISTRY

        for table_name in MAPPER_REGISTRY:
            self.assertIn(
                table_name,
                TABLE_KEYS,
                f"TABLE_KEYS missing entry for '{table_name}'",
            )


class SyncTableTest(TestCase):
    """Integration test for sync_table using a real Django DB and mocked warehouse."""

    @classmethod
    def setUpTestData(cls):
        cls.court = CourtFactory(id="testsyncfl", jurisdiction="SA")

    @patch("cl.sync.flows._write_sync_state")
    @patch("cl.sync.flows._read_sync_state", return_value=0)
    @patch("cl.sync.flows._get_pending_count", return_value=0)
    def test_no_pending_rows(self, mock_count, mock_read, mock_write):
        result = sync_table.fn("dockets")
        self.assertEqual(result["synced"], 0)
        self.assertEqual(result["errors"], 0)
        mock_write.assert_not_called()

    @patch("cl.sync.flows._writeback_cl_id")
    @patch("cl.sync.flows._write_sync_state")
    @patch("cl.sync.flows._fetch_chunk")
    @patch("cl.sync.flows._get_pending_count", return_value=1)
    @patch("cl.sync.flows._read_sync_state", return_value=0)
    def test_syncs_one_docket(
        self, mock_read, mock_count, mock_fetch, mock_write_state, mock_writeback
    ):
        mock_fetch.return_value = [
            {
                "court_id": self.court.id,
                "docket_number": "FLOW-001",
                "case_name": "Flow Test",
                "case_name_short": "",
                "case_name_full": "",
                "date_filed": date(2024, 1, 1),
                "cause": "",
                "nature_of_suit": "",
                "assigned_to_str": "",
                "referred_to_str": "",
                "panel_str": "",
                "appeal_from_str": "",
                "blocked": False,
                "courtlistener_id": None,
                "version_provenance": 5,
            },
        ]

        result = sync_table.fn("dockets")

        self.assertEqual(result["synced"], 1)
        self.assertEqual(result["errors"], 0)

        # Docket was actually created in the DB
        docket = Docket.objects.get(
            court_id=self.court.id, docket_number="FLOW-001"
        )
        self.assertEqual(docket.case_name, "Flow Test")

        # Writeback was called with the new docket's PK
        mock_writeback.assert_called_once()
        args = mock_writeback.call_args[0]
        self.assertEqual(args[0], "dockets")  # table_name
        self.assertEqual(args[2], docket.pk)  # cl_id

        # High-water mark was updated to 5
        mock_write_state.assert_called_once_with("dockets", 5)

    @patch("cl.sync.flows._writeback_cl_id")
    @patch("cl.sync.flows._write_sync_state")
    @patch("cl.sync.flows._fetch_chunk")
    @patch("cl.sync.flows._get_pending_count", return_value=1)
    @patch("cl.sync.flows._read_sync_state", return_value=0)
    def test_handles_mapper_error(
        self, mock_read, mock_count, mock_fetch, mock_write_state, mock_writeback
    ):
        """A row that fails to sync is counted as an error, not a crash."""
        mock_fetch.return_value = [
            {
                "court_id": "nonexistent_court",
                "docket_number": "FAIL-001",
                "case_name": "Bad",
                "case_name_short": "",
                "case_name_full": "",
                "date_filed": None,
                "cause": "",
                "nature_of_suit": "",
                "assigned_to_str": "",
                "referred_to_str": "",
                "panel_str": "",
                "appeal_from_str": "",
                "blocked": False,
                "courtlistener_id": None,
                "version_provenance": 1,
            },
        ]

        result = sync_table.fn("dockets")

        self.assertEqual(result["synced"], 0)
        self.assertEqual(result["errors"], 1)
        mock_writeback.assert_not_called()

    @patch("cl.sync.flows._writeback_cl_id")
    @patch("cl.sync.flows._write_sync_state")
    @patch("cl.sync.flows._fetch_chunk")
    @patch("cl.sync.flows._get_pending_count", return_value=2)
    @patch("cl.sync.flows._read_sync_state", return_value=0)
    def test_tracks_max_provenance(
        self, mock_read, mock_count, mock_fetch, mock_write_state, mock_writeback
    ):
        """High-water mark is set to the max version_provenance seen."""
        mock_fetch.return_value = [
            {
                "court_id": self.court.id,
                "docket_number": "PROV-001",
                "case_name": "First",
                "case_name_short": "",
                "case_name_full": "",
                "date_filed": None,
                "cause": "",
                "nature_of_suit": "",
                "assigned_to_str": "",
                "referred_to_str": "",
                "panel_str": "",
                "appeal_from_str": "",
                "blocked": False,
                "courtlistener_id": None,
                "version_provenance": 3,
            },
            {
                "court_id": self.court.id,
                "docket_number": "PROV-002",
                "case_name": "Second",
                "case_name_short": "",
                "case_name_full": "",
                "date_filed": None,
                "cause": "",
                "nature_of_suit": "",
                "assigned_to_str": "",
                "referred_to_str": "",
                "panel_str": "",
                "appeal_from_str": "",
                "blocked": False,
                "courtlistener_id": None,
                "version_provenance": 7,
            },
        ]

        result = sync_table.fn("dockets")

        self.assertEqual(result["synced"], 2)
        mock_write_state.assert_called_once_with("dockets", 7)
