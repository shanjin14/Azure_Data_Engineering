import logging
import csv
import psycopg2
import logging

DEBUG = True

async def upsert(cursor, table_name, csvinput,separator):
    logging.info('upsert starts')
    cur = cursor
    #Get table selector fields (primary keys)
    QueryString = "select constraint_name, string_agg(column_name,',')  as selector_fields \
    FROM information_schema.key_column_usage \
    WHERE table_name = '"+table_name+"' \
    group by constraint_name"
    cur.execute(QueryString)
    rows = cursor.fetchall()
    selector_rowcount = 0
    for row in rows:
        selector_string = str(row[1])
        selector_rowcount = selector_rowcount + 1
    if selector_rowcount == 0:
        return "Error: No primary key being created for "+table_name+". Please create in psql"
    logging.info(QueryString)
    #Get table setter fields (non-primary keys)
    QueryString = "select A.table_name, string_agg(A.column_name,',')  As setter_fields \
    from information_schema.columns A \
    left join information_schema.key_column_usage B on  A.column_name=B.column_name \
    where A.table_name = '"+table_name+"' and B.constraint_name is null \
    group by A.table_name;"
    cur.execute(QueryString)
    rows = cursor.fetchall()
    for row in rows:
        setter_string = str(row[1])

    logging.info(QueryString)

    selector_fields = selector_string.split(",")
    setter_fields = setter_string.split(",")

    #Original reference code to ingest data from csv (Pipe delimited)
    csv_data = csvinput

    sql_template = """
        WITH updates AS (
            UPDATE %(target)s t
                SET %(set)s        
            FROM source s
            WHERE %(where_t_pk_eq_s_pk)s 
            RETURNING %(s_pk)s
        )
        INSERT INTO %(target)s (%(columns)s)
            SELECT %(source_columns)s 
            FROM source s LEFT JOIN updates t USING(%(pk)s)
            WHERE %(where_t_pk_is_null)s
            GROUP BY %(s_pk)s
    """
    statement = sql_template % dict(
        target = table_name,
        set = ',\n'.join(["%s = s.%s" % (x,x) for x in setter_fields]),
        where_t_pk_eq_s_pk = ' AND '.join(["t.%s = s.%s" % (x,x) for x in selector_fields]),
        s_pk = ','.join(["s.%s" % x for x in selector_fields]),
        columns = ','.join([x for x in selector_fields+setter_fields]),
        source_columns = ','.join(['s.%s' % x for x in selector_fields+setter_fields]), 
        pk = ','.join(selector_fields),
        where_t_pk_is_null = ' AND '.join(["t.%s IS NULL" % x for x in selector_fields]),
        t_pk = ','.join(["t.%s" % x for x in selector_fields]))

    if DEBUG: 
        logging.info(statement)

    # with cursor as cur:

    cur.execute('CREATE TEMP TABLE source(LIKE %s INCLUDING ALL) ON COMMIT DROP;' % table_name)
    cur.copy_from(csv_data, 'source',sep=separator, columns=selector_fields+setter_fields)
    cur.execute(statement)
    cur.execute('DROP TABLE source')
    return "success upsert"