import re
from tabulate import tabulate
from lib.query import Query
import random


class Table:

    schema_re = re.compile('\((.+)\)')

    def __init__(self, table_id, header, types, rows, caption=None):
        self.table_id = table_id
        self.header = header
        self.types = types
        self.rows = rows
        self.caption = caption

    def __repr__(self):
        return 'Table: {id}\nCaption: {caption}\n{tabulate}'.format(
                id=self.table_id,
                caption=self.caption,
                tabulate=tabulate(self.rows, headers=self.header)
                )

    @classmethod
    def get_schema(cls, db, table_id):
        if table_infos := db.query(
            'SELECT sql from sqlite_master WHERE tbl_name = :name',
            name=cls.get_id(table_id),
        ).all():
            return table_infos[0]
        else:
            return None

    @classmethod
    def get_id(cls, table_id):
        return f"table_{table_id.replace('-', '_')}"

    @classmethod
    def from_db(cls, db, table_id):
        if table_info := cls.get_schema(db, table_id):
            schema_str = cls.schema_re.findall(table_info)[0] = [0].sql
            header, types = [], []
            for tup in schema_str.split(', '):
                c, t = tup.split()
                header.append(c)
                types.append(t)
            rows = [
                [getattr(r, h) for h in header]
                for r in db.query(f'SELECT * from {cls.get_id(table_id)}')
            ]

            return cls(table_id, header, types, rows)
        else:
            return None

    @property
    def name(self):
        return self.get_id(self.table_id)

    def create_table(self, db, replace_existing=False, lower=True):
        if exists := self.get_schema(db, self.table_id):
            if replace_existing:
                db.query(f'DROP TABLE {self.name}')
            else:
                return
        type_str = ', '.join([f'col{i} {t}' for i, t in enumerate(self.types)])
        db.query('CREATE TABLE {name} ({types})'.format(name=self.name, types=type_str))
        for row in self.rows:
            value_str = ', '.join([f':val{j}' for j, c in enumerate(row)])
            value_dict = {f'val{j}': c for j, c in enumerate(row)}
            if lower:
                value_dict = {k: v.lower() if isinstance(v, str) else v for k, v in value_dict.items()}
            db.query('INSERT INTO {name} VALUES ({values})'.format(name=self.name, values=value_str), **value_dict)

    def execute_query(self, db, query, lower=True):
        sel_str = f'col{query.sel_index}' if query.sel_index >= 0 else '*'
        agg_str = sel_str
        if agg_op := Query.agg_ops[query.agg_index]:
            agg_str = f'{agg_op}({sel_str})'
        where_str = ' AND '.join(
            [f'col{i} {Query.cond_ops[o]} :col{i}' for i, o, v in query.conditions]
        )

        where_map = {f'col{i}': v for i, o, v in query.conditions}
        if lower:
            where_map = {k: v.lower() if isinstance(v, str) else v for k, v in where_map.items()}
        if where_map:
            where_str = f'WHERE {where_str}'

        if query.sel_index >= 0:
            query_str = 'SELECT {agg_str} AS result FROM {name} {where_str}'.format(agg_str=agg_str, name=self.name, where_str=where_str)
            return [r.result for r in db.query(query_str, **where_map)]
        else:
            query_str = 'SELECT {agg_str} FROM {name} {where_str}'.format(agg_str=agg_str, name=self.name, where_str=where_str)
            return [
                [getattr(r, f'col{i}') for i in range(len(self.header))]
                for r in db.query(query_str, **where_map)
            ]

    def query_str(self, query):
        agg_str = self.header[query.sel_index]
        if agg_op := Query.agg_ops[query.agg_index]:
            agg_str = f'{agg_op}({agg_str})'
        where_str = ' AND '.join(
            [
                f'{self.header[i]} {Query.cond_ops[o]} {v}'
                for i, o, v in query.conditions
            ]
        )

        return f'SELECT {agg_str} FROM {self.name} WHERE {where_str}'

    def generate_query(self, db, max_cond=4):
        max_cond = min(len(self.header), max_cond)
        # sample a select column
        sel_index = random.choice(list(range(len(self.header))))
        # sample where conditions
        query = Query(-1, Query.agg_ops.index(''))
        results = self.execute_query(db, query)
        condition_options = list(range(len(self.header)))
        condition_options.remove(sel_index)
        for _ in range(max_cond):
            if not results:
                break
            cond_index = random.choice(condition_options)
            cond_op = (
                Query.cond_ops.index('=')
                if self.types[cond_index] == 'text'
                else random.choice(list(range(len(Query.cond_ops))))
            )

            cond_val = random.choice([r[cond_index] for r in results])
            query.conditions.append((cond_index, cond_op, cond_val))
            new_results = self.execute_query(db, query)
            if [r[sel_index] for r in new_results] != [r[sel_index] for r in results]:
                condition_options.remove(cond_index)
                results = new_results
            else:
                query.conditions.pop()
        # sample an aggregation operation
        if self.types[sel_index] == 'text':
            query.agg_index = Query.agg_ops.index('')
        else:
            query.agg_index = random.choice(list(range(len(Query.agg_ops))))
        query.sel_index = sel_index
        results = self.execute_query(db, query)
        return query, results

    def generate_queries(self, db, n=1, max_tries=5, lower=True):
        qs = []
        for _ in range(n):
            n_tries = 0
            r = None
            while r is None and n_tries < max_tries:
                q, r = self.generate_query(db, max_cond=4)
                n_tries += 1
            if r:
                qs.append((q, r))
        return qs
