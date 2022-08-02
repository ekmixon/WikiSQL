import records
import re
from babel.numbers import parse_decimal, NumberFormatError
from lib.query import Query


schema_re = re.compile(r'\((.+)\)')
num_re = re.compile(r'[-+]?\d*\.\d+|\d+')


class DBEngine:

    def __init__(self, fdb):
        self.db = records.Database(f'sqlite:///{fdb}')
        self.conn = self.db.get_connection()

    def execute_query(self, table_id, query, *args, **kwargs):
        return self.execute(table_id, query.sel_index, query.agg_index, query.conditions, *args, **kwargs)

    def execute(self, table_id, select_index, aggregation_index, conditions, lower=True):
        if not table_id.startswith('table'):
            table_id = f"table_{table_id.replace('-', '_')}"
        table_info = self.conn.query('SELECT sql from sqlite_master WHERE tbl_name = :name', name=table_id).all()[0].sql
        schema_str = schema_re.findall(table_info)[0]
        schema = {}
        for tup in schema_str.split(', '):
            c, t = tup.split()
            schema[c] = t
        select = f'col{select_index}'
        if agg := Query.agg_ops[aggregation_index]:
            select = f'{agg}({select})'
        where_clause = []
        where_map = {}
        for col_index, op, val in conditions:
            if lower and isinstance(val, str):
                val = val.lower()
            if schema[f'col{col_index}'] == 'real' and not isinstance(
                val, (int, float)
            ):
                try:
                    val = float(parse_decimal(val))
                except NumberFormatError as e:
                    val = float(num_re.findall(val)[0])
            where_clause.append(f'col{col_index} {Query.cond_ops[op]} :col{col_index}')
            where_map[f'col{col_index}'] = val
        where_str = 'WHERE ' + ' AND '.join(where_clause) if where_clause else ''
        query = f'SELECT {select} AS result FROM {table_id} {where_str}'
        out = self.conn.query(query, **where_map)
        return [o.result for o in out]
