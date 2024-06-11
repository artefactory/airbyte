from typing import List, Optional

def build_query(
        select: Optional[List[str]] = ["*"],
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        query_end_char: Optional[str] = None,
        use_legacy_sql: Optional[bool] = False,
        timeout_ms: Optional[int] = None,
        max_results: Optional[int] = None,
        dry_run: Optional[bool] = None,
        uppercase: Optional[bool] = False,
):
    _from = '.'.join(filter(None, [project_id, dataset_id, table_id]))
    _query = " ".join(filter(None, (
        f"{'SELECT' if uppercase else 'select'} {', '.join(select)} {'FROM' if uppercase else 'from'} `{_from}`",
        f"{'WHERE' if uppercase else 'where'} {where}" if where else None,
        f"{'LIMIT' if uppercase else 'limit'} {limit}" if limit else None,
    ))) + (query_end_char or "")
    return {} | (
        {"kind": "bigquery#queryRequest"}
    ) | (
        {"query": _query}
    ) | (
        {"useLegacySql": use_legacy_sql}
    ) | (
        {"timeoutMs": timeout_ms} if timeout_ms else {}
    ) | (
        {"maxResults": max_results} if max_results else {}
    ) | (
        {"dryRun": dry_run} if dry_run else {}
    )