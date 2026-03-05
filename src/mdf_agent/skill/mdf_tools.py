from __future__ import annotations

from mdf_agent.skill import handlers

# Flat tool registry for agentic/MCP integration.
TOOLS = {
    "mdf_health_check": handlers.health_check,
    "mdf_scan_folder": handlers.scan_folder,
    "mdf_create_manifest": handlers.create_manifest,
    "mdf_suggest_mappings": handlers.suggest_mappings,
    "mdf_validate_and_preview": handlers.validate_and_preview,
    "mdf_publish": handlers.publish,
    "mdf_stream_create": handlers.stream_create,
    "mdf_stream_append": handlers.stream_append,
    "mdf_stream_status": handlers.stream_status,
    "mdf_stream_close": handlers.stream_close,
    "mdf_stream_snapshot": handlers.stream_snapshot,
    "mdf_check_status": handlers.check_status,
    "mdf_list_submissions": handlers.list_submissions,
    "mdf_search_datasets": handlers.search_datasets,
    "mdf_get_citation": handlers.get_citation,
    "mdf_get_card": handlers.get_card,
    "mdf_dataset_preview": handlers.dataset_preview,
    "mdf_dataset_sample": handlers.dataset_sample,
    "mdf_curation_list_pending": handlers.curation_list_pending,
    "mdf_curation_review": handlers.curation_review,
    "mdf_curation_approve": handlers.curation_approve,
    "mdf_curation_reject": handlers.curation_reject,
    "mdf_edit_metadata": handlers.edit_metadata,
    "mdf_withdraw": handlers.withdraw,
    "mdf_resubmit": handlers.resubmit,
    "mdf_version_diff": handlers.version_diff,
    "mdf_delete_submission": handlers.delete_submission,
    "mdf_admin_stats": handlers.admin_stats,
    "mdf_dataset_stats": handlers.dataset_stats,
}

__all__ = ["TOOLS"]
