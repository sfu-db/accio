def registerTmpTable(ctx, sql, alias, url):
    queries = sql.split(';')
    import connectorx as cx
    tmp = cx.read_sql(url, sql if len(queries) < 2 else queries, return_type="arrow2") 
    ctx.from_arrow_table(tmp, name=alias)

def run_datafusion(plan, ctx, accio):
    for v in plan.temp_views:
        registerTmpTable(ctx, v.sql, v.alias, accio.get_url(v.db))
    return ctx.sql(plan.local.sql)

def clean_datafusion(plan, ctx):
    for v in plan.temp_views:
        ctx.deregister_table(v.alias)

