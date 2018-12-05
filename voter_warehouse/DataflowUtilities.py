"""Utilities for Dataflow pipelines.
"""
import apache_beam as beam

from apache_beam.io.gcp.internal.clients import bigquery

def gen_kind_schema(tup):
    kind_schema = bigquery.TableFieldSchema()
    kind_schema.name = tup[0]
    kind_schema.type = tup[1]
    kind_schema.mode = tup[2]
    if kind_schema.type == 'record':
        for nest_tup in tup[3]:
            nest_schema = gen_kind_schema(nest_tup)
            kind_schema.fields.append(nest_schema)
    return kind_schema

def gen_schema(fields):
    table_schema = bigquery.TableSchema()
    for tup in fields:
        kind_schema = gen_kind_schema(tup)
        table_schema.fields.append(kind_schema)
    return table_schema

def singleton(cls):
    instances = {}
    def get(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return get

def flatten_format(row):
    # Map and type-convert
    output = {}
    for name, typ, mode in SCHEMA_FIELDS:
        if typ in ['string', 'timestamp', 'date']:
            if name in row:
                try:
                    output[name] = row[name].encode('utf-8', errors='ignore').strip()
                except Exception:
                    output[name] = None
            else:
                output[name] = None
        elif typ == 'integer':
            try:
                output[name] = int(row[name].strip())
            except Exception:
                output[name] = None

    return output

class BatchRunner(beam.DoFn):

    def process(self, batch, fn):
        threads = deque()
        results = []
        for row in batch:
            if len(threads) > 30:
                t = threads.popleft()
                t.join()

            t = threading.Thread(target=fn,
                    args=(row, results))
            t.start()
            threads.append(t)

        while threads:
            t = threads.popleft()
            t.join()

        for r in results:
            yield r
