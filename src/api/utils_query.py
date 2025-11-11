from sqlalchemy import text

def run_mv_query(db, mv_name, where_clauses, params, order_clause, limit_offset=True):
    """
    Execute MV query with safe parameter binding and optional pagination.
    """
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}

    # total count
    total = db.execute(
        text(f"SELECT COUNT(*) FROM {mv_name} {where_sql}")
        .bindparams(**count_params)
    ).scalar()

    # data rows
    sql = f"SELECT * FROM {mv_name} {where_sql} {order_clause}"
    if limit_offset:
        sql += " LIMIT :limit OFFSET :offset"

    rows = db.execute(text(sql).bindparams(**params)).mappings().all()
    return total, [dict(r) for r in rows]
