"""Mapper functions that save staged warehouse rows to CL via Django ORM.

Each function takes a row dict (from ``courtlistener.staged_*``) and
returns the CL primary key (``courtlistener_id``) of the saved object.

The general pattern for each mapper:

1. If ``courtlistener_id`` is already set, look up by PK (update path).
2. Otherwise, look up by natural key (first-sync / find-or-create path).
3. Set fields from the staged row.
4. Save and return the PK.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import PurePosixPath

from django.core.files.base import ContentFile
from django.db import connections, transaction
from django.utils.text import get_valid_filename

from cl.audio.models import Audio
from cl.search.docket_sources import DocketSources
from cl.search.models import (
    Court,
    Docket,
    DocketEntry,
    OpinionCluster,
    Opinion,
    OriginatingCourtInformation,
    SOURCES,
)

logger = logging.getLogger(__name__)


# -- helpers ----------------------------------------------------------------

def _set_fields(obj, row: dict, field_map: dict[str, str]) -> None:
    """Set fields on a Django model instance from a staged row dict.

    ``field_map`` maps staged column names to Django field names.
    Only non-None values are applied (preserving existing data when the
    warehouse has NULL for a field).
    """
    for staged_col, django_field in field_map.items():
        value = row.get(staged_col)
        if value is not None:
            setattr(obj, django_field, value)


def _coerce_date(value) -> date | None:
    """Coerce a value to a date, handling strings and None."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value


_s3_client = None  # lazy-loaded boto3 S3 client for scraper bucket


def _get_scraper_s3_client():
    """Return a boto3 S3 client configured for the scraper bucket.

    Lazy-loaded so the module can be imported without hitting S3
    (e.g. during Django startup or in tests).  Reads endpoint URL
    from the ``SCRAPER_S3_ENDPOINT_URL`` environment variable,
    defaulting to the en_banc LocalStack instance.
    """
    global _s3_client
    if _s3_client is None:
        import os

        import boto3

        endpoint_url = os.environ.get(
            "SCRAPER_S3_ENDPOINT_URL",
            "http://mini.bopp-justice.ts.net:4566",
        )
        _s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
    return _s3_client


def _copy_s3_file_to_opinion(opinion: Opinion, s3_uri: str) -> None:
    """Copy a file from the scraper S3 bucket into CL's media storage.

    Parses the ``s3://bucket/key`` URI, downloads file bytes via boto3,
    and saves into ``opinion.local_path`` using Django's
    ``IncrementingAWSMediaStorage``.  The caller must call
    ``opinion.save()`` afterward.
    """
    # Parse s3://bucket_name/key → bucket_name, key
    without_scheme = s3_uri.replace("s3://", "")
    bucket_name, s3_key = without_scheme.split("/", 1)

    client = _get_scraper_s3_client()
    response = client.get_object(Bucket=bucket_name, Key=s3_key)
    content = response["Body"].read()
    cf = ContentFile(content)

    ext = PurePosixPath(s3_key).suffix or ".pdf"
    case_name = opinion.cluster.case_name or "opinion"
    filename = get_valid_filename(case_name[:75]) + ext
    opinion.local_path.save(filename, cf, save=False)


def _maybe_copy_opinion_file(opinion: Opinion, row: dict) -> None:
    """Copy a scraper S3 file to CL storage if the row has one."""
    s3_uri = row.get("local_path")
    if s3_uri and s3_uri.startswith("s3://"):
        _copy_s3_file_to_opinion(opinion, s3_uri)


def _find_or_create_docket(row: dict) -> Docket:
    """Find or create a parent Docket for a dependent table row.

    Checks the warehouse for the docket's courtlistener_id (written back
    by the prior docket sync step), then falls back to natural key lookup
    in CL's database.  If no docket exists at all, creates a minimal stub
    so that the dependent record is not orphaned.  The stub will be updated
    with full data when the docket is synced later via ``sync_docket``.
    """
    court_id = row["court_id"]
    docket_number = row["docket_number"]

    # Check if the docket was already synced (CL ID written back to warehouse)
    staged_row = None
    try:
        with connections["warehouse"].cursor() as cur:
            cur.execute(
                "SELECT courtlistener_id FROM courtlistener.staged_dockets "
                "WHERE court_id = %s AND docket_number = %s",
                [court_id, docket_number],
            )
            staged_row = cur.fetchone()
    except Exception:
        logger.debug(
            "Could not check warehouse for docket CL ID "
            "(court_id=%s, docket_number=%s)",
            court_id, docket_number,
        )

    if staged_row and staged_row[0]:
        try:
            return Docket.objects.get(pk=staged_row[0])
        except Docket.DoesNotExist:
            logger.warning(
                "Docket CL ID %d from warehouse not found in CL "
                "(court_id=%s, docket_number=%s)",
                staged_row[0], court_id, docket_number,
            )

    # Fall back to natural key lookup in CL
    court = Court.objects.get(id=court_id)
    try:
        return Docket.objects.get(court=court, docket_number=docket_number)
    except Docket.MultipleObjectsReturned:
        return Docket.objects.filter(
            court=court, docket_number=docket_number,
        ).first()
    except Docket.DoesNotExist:
        logger.info(
            "Creating stub docket for court_id=%s, docket_number=%s",
            court_id,
            docket_number,
        )
        docket = Docket(
            court=court,
            docket_number=docket_number,
            source=DocketSources.SCRAPER,
            case_name=row.get("case_name", ""),
        )
        docket.save()
        return docket


# -- dockets ----------------------------------------------------------------

DOCKET_FIELD_MAP = {
    "case_name": "case_name",
    "case_name_short": "case_name_short",
    "case_name_full": "case_name_full",
    "date_filed": "date_filed",
    "cause": "cause",
    "nature_of_suit": "nature_of_suit",
    "assigned_to_str": "assigned_to_str",
    "referred_to_str": "referred_to_str",
    "panel_str": "panel_str",
    "appeal_from_str": "appeal_from_str",
    "blocked": "blocked",
}


def sync_docket(row: dict) -> int:
    """Save a staged docket to CL's Docket model.

    Returns the Docket PK (courtlistener_id).
    """
    court = Court.objects.get(id=row["court_id"])
    cl_id = row.get("courtlistener_id")

    if cl_id:
        # Update existing by PK
        docket = Docket.objects.get(pk=cl_id)
        docket.source = DocketSources.merge_sources(
            docket.source, DocketSources.SCRAPER
        )
        _set_fields(docket, row, DOCKET_FIELD_MAP)
        if row.get("date_filed"):
            docket.date_filed = _coerce_date(row["date_filed"])
        docket.save()
        return docket.pk

    # First sync — find or create by natural key
    with transaction.atomic():
        try:
            docket = Docket.objects.get(
                court=court, docket_number=row["docket_number"]
            )
        except Docket.MultipleObjectsReturned:
            docket = Docket.objects.filter(
                court=court, docket_number=row["docket_number"]
            ).first()
        except Docket.DoesNotExist:
            docket = None

        if docket is not None:
            # Found existing — update
            docket.source = DocketSources.merge_sources(
                docket.source, DocketSources.SCRAPER
            )
            _set_fields(docket, row, DOCKET_FIELD_MAP)
            if row.get("date_filed"):
                docket.date_filed = _coerce_date(row["date_filed"])
            docket.save()
        else:
            docket = Docket(
                court=court,
                docket_number=row["docket_number"],
                source=DocketSources.SCRAPER,
                case_name=row.get("case_name") or "",
                case_name_short=row.get("case_name_short") or "",
                case_name_full=row.get("case_name_full") or "",
                date_filed=_coerce_date(row.get("date_filed")),
                cause=row.get("cause") or "",
                nature_of_suit=row.get("nature_of_suit") or "",
                assigned_to_str=row.get("assigned_to_str") or "",
                referred_to_str=row.get("referred_to_str") or "",
                panel_str=row.get("panel_str") or "",
                appeal_from_str=row.get("appeal_from_str") or "",
                blocked=row.get("blocked", False),
            )
            docket.save()

    return docket.pk


# -- originating court information ------------------------------------------

OCI_FIELD_MAP = {
    "lower_court_docket_number": "docket_number",
    "court_reporter": "court_reporter",
    "assigned_to_str": "assigned_to_str",
    "ordering_judge_str": "ordering_judge_str",
}

OCI_DATE_FIELDS = [
    "date_filed",
    "date_disposed",
    "date_judgment",
    "date_judgment_eod",
    "date_filed_noa",
    "date_received_coa",
]


def sync_originating_court_information(row: dict) -> int:
    """Save staged originating court information.

    Returns the OriginatingCourtInformation PK.
    """
    cl_id = row.get("courtlistener_id")

    if cl_id:
        oci = OriginatingCourtInformation.objects.get(pk=cl_id)
    else:
        docket = _find_or_create_docket(row)
        if hasattr(docket, "originating_court_information") and docket.originating_court_information:
            oci = docket.originating_court_information
        else:
            oci = OriginatingCourtInformation()
            oci.save()
            docket.originating_court_information = oci
            docket.save()

    _set_fields(oci, row, OCI_FIELD_MAP)
    for date_field in OCI_DATE_FIELDS:
        value = row.get(date_field)
        if value is not None:
            setattr(oci, date_field, _coerce_date(value))

    oci.save()
    return oci.pk


# -- opinion clusters -------------------------------------------------------

CLUSTER_FIELD_MAP = {
    "case_name": "case_name",
    "case_name_short": "case_name_short",
    "case_name_full": "case_name_full",
    "judges": "judges",
    "precedential_status": "precedential_status",
    "syllabus": "syllabus",
    "headnotes": "headnotes",
    "summary": "summary",
    "disposition": "disposition",
    "procedural_history": "procedural_history",
    "attorneys": "attorneys",
    "blocked": "blocked",
    "date_filed_is_approximate": "date_filed_is_approximate",
}


def sync_opinion_cluster(row: dict) -> int:
    """Save a staged opinion cluster to CL.

    Returns the OpinionCluster PK.
    """
    cl_id = row.get("courtlistener_id")

    if cl_id:
        cluster = OpinionCluster.objects.get(pk=cl_id)
        _set_fields(cluster, row, CLUSTER_FIELD_MAP)
        if row.get("date_filed"):
            cluster.date_filed = _coerce_date(row["date_filed"])
        cluster.save()
        return cluster.pk

    # First sync — find docket, then find-or-create cluster
    docket = _find_or_create_docket(row)
    date_filed = _coerce_date(row["date_filed"])

    with transaction.atomic():
        try:
            cluster = OpinionCluster.objects.get(
                docket=docket, date_filed=date_filed
            )
            _set_fields(cluster, row, CLUSTER_FIELD_MAP)
            cluster.save()
        except OpinionCluster.DoesNotExist:
            cluster = OpinionCluster(
                docket=docket,
                date_filed=date_filed,
                case_name=row.get("case_name") or "",
                case_name_short=row.get("case_name_short") or "",
                case_name_full=row.get("case_name_full") or "",
                judges=row.get("judges") or "",
                precedential_status=row.get("precedential_status") or "",
                syllabus=row.get("syllabus") or "",
                headnotes=row.get("headnotes") or "",
                summary=row.get("summary") or "",
                disposition=row.get("disposition") or "",
                procedural_history=row.get("procedural_history") or "",
                attorneys=row.get("attorneys") or "",
                blocked=row.get("blocked", False),
                date_filed_is_approximate=row.get("date_filed_is_approximate", False),
                source=SOURCES.COURT_WEBSITE,
            )
            cluster.save()

    return cluster.pk


# -- opinions ---------------------------------------------------------------

OPINION_FIELD_MAP = {
    "author_str": "author_str",
    "per_curiam": "per_curiam",
    "joined_by_str": "joined_by_str",
    "sha1": "sha1",
    "page_count": "page_count",
    "download_url": "download_url",
    "plain_text": "plain_text",
    "html": "html",
    "extracted_by_ocr": "extracted_by_ocr",
}


def sync_opinion(row: dict) -> int:
    """Save a staged opinion to CL.

    Returns the Opinion PK.
    """
    cl_id = row.get("courtlistener_id")

    if cl_id:
        opinion = Opinion.objects.get(pk=cl_id)
        _set_fields(opinion, row, OPINION_FIELD_MAP)
        _maybe_copy_opinion_file(opinion, row)
        opinion.save()
        return opinion.pk

    # First sync — find cluster, then find-or-create opinion
    docket = _find_or_create_docket(row)
    cluster_date = _coerce_date(row["cluster_date_filed"])
    cluster = OpinionCluster.objects.get(docket=docket, date_filed=cluster_date)

    op_type = row["type"]
    author_str = row.get("author_str") or ""

    with transaction.atomic():
        try:
            opinion = Opinion.objects.get(
                cluster=cluster, type=op_type, author_str=author_str
            )
            _set_fields(opinion, row, OPINION_FIELD_MAP)
            _maybe_copy_opinion_file(opinion, row)
            opinion.save()
        except Opinion.DoesNotExist:
            opinion = Opinion(
                cluster=cluster,
                type=op_type,
                author_str=author_str,
                per_curiam=row.get("per_curiam", False),
                joined_by_str=row.get("joined_by_str") or "",
                sha1=row.get("sha1") or "",
                page_count=row.get("page_count"),
                download_url=row.get("download_url") or "",
                plain_text=row.get("plain_text") or "",
                html=row.get("html") or "",
                extracted_by_ocr=row.get("extracted_by_ocr", False),
            )
            _maybe_copy_opinion_file(opinion, row)
            opinion.save()
        except Opinion.MultipleObjectsReturned:
            # If multiple opinions match, update the first one
            opinion = Opinion.objects.filter(
                cluster=cluster, type=op_type, author_str=author_str
            ).first()
            _set_fields(opinion, row, OPINION_FIELD_MAP)
            _maybe_copy_opinion_file(opinion, row)
            opinion.save()

    return opinion.pk


# -- docket entries ---------------------------------------------------------

DOCKET_ENTRY_FIELD_MAP = {
    "description": "description",
}


def sync_docket_entry(row: dict) -> int:
    """Save a staged docket entry to CL.

    Returns the DocketEntry PK.
    """
    cl_id = row.get("courtlistener_id")

    if cl_id:
        de = DocketEntry.objects.get(pk=cl_id)
        _set_fields(de, row, DOCKET_ENTRY_FIELD_MAP)
        if row.get("date_filed"):
            de.date_filed = _coerce_date(row["date_filed"])
        if row.get("time_filed") is not None:
            de.time_filed = row["time_filed"]
        if row.get("entry_number") is not None:
            de.entry_number = row["entry_number"]
        de.save()
        return de.pk

    # First sync
    docket = _find_or_create_docket(row)
    entry_number = row.get("entry_number")

    with transaction.atomic():
        if entry_number is not None:
            try:
                de = DocketEntry.objects.get(
                    docket=docket, entry_number=entry_number
                )
                _set_fields(de, row, DOCKET_ENTRY_FIELD_MAP)
                if row.get("date_filed"):
                    de.date_filed = _coerce_date(row["date_filed"])
                if row.get("time_filed") is not None:
                    de.time_filed = row["time_filed"]
                de.save()
                return de.pk
            except DocketEntry.DoesNotExist:
                pass
            except DocketEntry.MultipleObjectsReturned:
                de = DocketEntry.objects.filter(
                    docket=docket, entry_number=entry_number
                ).first()
                _set_fields(de, row, DOCKET_ENTRY_FIELD_MAP)
                if row.get("date_filed"):
                    de.date_filed = _coerce_date(row["date_filed"])
                de.save()
                return de.pk

        # Create new
        de = DocketEntry(
            docket=docket,
            date_filed=_coerce_date(row.get("date_filed")),
            time_filed=row.get("time_filed"),
            entry_number=entry_number,
            description=row.get("description") or "",
        )
        de.save()

    return de.pk


# -- audio ------------------------------------------------------------------

AUDIO_FIELD_MAP = {
    "case_name": "case_name",
    "case_name_short": "case_name_short",
    "case_name_full": "case_name_full",
    "judges": "judges",
    "download_url": "download_url",
    "duration": "duration",
    "stt_status": "stt_status",
    "blocked": "blocked",
}


def sync_audio(row: dict) -> int:
    """Save a staged audio record to CL.

    Returns the Audio PK.
    """
    cl_id = row.get("courtlistener_id")

    if cl_id:
        audio = Audio.objects.get(pk=cl_id)
        _set_fields(audio, row, AUDIO_FIELD_MAP)
        audio.save()
        return audio.pk

    # First sync
    docket = _find_or_create_docket(row)

    with transaction.atomic():
        # Try to find by docket + download_url (most unique identifier for audio)
        download_url = row.get("download_url")
        if download_url:
            try:
                audio = Audio.objects.get(
                    docket=docket, download_url=download_url
                )
                _set_fields(audio, row, AUDIO_FIELD_MAP)
                audio.save()
                return audio.pk
            except Audio.DoesNotExist:
                pass
            except Audio.MultipleObjectsReturned:
                audio = Audio.objects.filter(
                    docket=docket, download_url=download_url
                ).first()
                _set_fields(audio, row, AUDIO_FIELD_MAP)
                audio.save()
                return audio.pk

        # Create new
        audio = Audio(
            docket=docket,
            case_name=row.get("case_name") or "",
            case_name_short=row.get("case_name_short") or "",
            case_name_full=row.get("case_name_full") or "",
            judges=row.get("judges") or "",
            source=SOURCES.COURT_WEBSITE,
            download_url=download_url or "",
            duration=row.get("duration"),
            stt_status=row.get("stt_status", Audio.STT_NEEDED),
            blocked=row.get("blocked", False),
            sha1="",
        )
        audio.save()

    return audio.pk


# -- dispatch ---------------------------------------------------------------

MAPPER_REGISTRY: dict[str, callable] = {
    "dockets": sync_docket,
    "originating_court_information": sync_originating_court_information,
    "opinion_clusters": sync_opinion_cluster,
    "opinions": sync_opinion,
    "docket_entries": sync_docket_entry,
    "audio": sync_audio,
}

# Natural key columns for each staged table, used for writeback queries.
TABLE_KEYS: dict[str, list[str]] = {
    "dockets": ["court_id", "docket_number"],
    "originating_court_information": ["court_id", "docket_number"],
    "opinion_clusters": ["court_id", "docket_number", "date_filed"],
    "opinions": ["court_id", "docket_number", "cluster_date_filed", "type", "author_str"],
    "docket_entries": ["court_id", "docket_number", "document_uuid"],
    "audio": ["court_id", "docket_number", "date_argued"],
}
