def registerTmpTable(ctx, sql, alias, url):
    queries = sql.split(';')
    import connectorx as cx
    tmp = cx.read_sql(url, sql if len(queries) < 2 else queries, return_type="polars2") 
    ctx.register(alias, tmp)

def run_polars(plan, ctx, accio):
    for v in plan.temp_views:
        registerTmpTable(ctx, v.sql, v.alias, accio.get_url(v.db))
    return ctx.execute(plan.local.sql)

def clean_polars(plan, ctx):
    for v in plan.temp_views:
        ctx.unregister(v.alias)

