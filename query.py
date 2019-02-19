import pandas as pd
import argparse
import re

from parsing import query

DB1 = pd.DataFrame([
    {
        "uuid": "qwer",
        "tid": "123",
        "maturity": "false",
    },{
        "uuid": "asdf",
        "tid": "234",
        "maturity": "true",
    },{
        "uuid": "zxcv",
        "tid": "456",
        "maturity": "false",
    },{
        "uuid": "lkj",
        "tid": "567",
        "maturity": "true",
    },{
        "uuid": "poiu",
        "tid": "678",
        "maturity": "false",
    },{
        "uuid": "not in db2",
        "tid": "61612",
        "maturity": "true",
    },
])

DB2 = pd.DataFrame([
    {
        "tid": "123",
        "name": "gooba",
    },{
        "tid": "234",
        "name": "snard",
    },{
        "tid": "456",
        "name": "hi",
    },{
        "tid": "567",
        "name": "booga",
    },{
        "tid": "678",
        "name": "ooofffaa",
    },
])

operators = {}

def add_operator(name, f):
    operators[name] = f

def simple_query(db):
    pass

class Var(object):
    def __init__(self, value):
        self._value = str(value)

    def is_var(self):
        return True

    def value(self):
        return self._value

class Const(object):
    def __init__(self, value):
        self._value = str(value)

    def is_var(self):
        return False

    def value(self):
        return self._value

def join_op(pseudo_df):
    other = pd.DataFrame(pseudo_df)
    other = other.assign(key=0)
    def op(neg, df, *args, **kwargs):
        for i, arg in enumerate(args):
            kwargs[str(i)] = arg
        # Existing keys
        existing_keys = [m for m in kwargs.items() if m[1].is_var() and m[1].value() in df.keys()]

        # Add consts
        consts = {m[0]: m[1].value() for m in kwargs.items() if not m[1].is_var()}
        df = df.assign(**consts)

        left_keys = [m[1].value() for m in existing_keys if m[1].is_var()] + ["key"] + list(consts.keys())
        right_keys = [m[0] for m in existing_keys] + ["key"] + list(consts.keys())

        if neg:
            # Anti join
            output = df.merge(other, how="left", left_on=left_keys, right_on=right_keys, indicator=True)    
            output = output[output._merge == "left_only"]
        else:
            output = df.merge(other, left_on=left_keys, right_on=right_keys)

            # Rename new keys
            key_mapping = {
                m[0]: m[1].value() 
                    for m in kwargs.items()
                    if m[1].is_var() and m[1].value() not in df.keys()
            }
            output = output.rename(columns=key_mapping)

        # Filter out unused vars
        var_names = [m.value() for m in kwargs.values() if m.is_var()]
        cols = set(list(df.keys()) + var_names)
        output = output[cols]
        return output
    return op

def _get_value(panda, atom):
    if atom.is_var():
        return panda[atom.value()]
    else:
        return atom.value()

def _atom_bound(panda, atom):
    if atom.is_var():
        return atom.value() in panda
    else:
        return True

# Given a function that returns a boolean,
# Runs it against all the current rows, and filters any for which f returns false.
def filter_op(f):
    def op(neg, df, *args, **kwargs):
        take = []
        for row in df.iterrows():
            row = row[1]
            i_args = [_get_value(row, arg) for arg in args]
            i_kwargs = {k: _get_value(row, v) for k,v in kwargs.items()}
            r = f(*i_args, **i_kwargs)
            if neg:
                r = not r
            take.append(r)
        return df[take]
    return op

# Given a function that takes 
def map_op(f):
    def op(neg, df, *args, **kwargs):
        def inner(row):
            inner_args = [_get_value(row, a) if _atom_bound(row, a) else None for a in args]
            inner_kwargs = {k: _get_value(v, a) if _atom_bound(row, v) else None for v in kwargs}
            # Call f with only the relevant values.
            r_args, r_kwargs = f(*inner_args, **inner_kwargs)
        inner_args = [_get_value]
        df.apply(f, axis=1, result_type='expand')
    return op

add_operator("db1", join_op(DB1))
add_operator("db2", join_op(DB2))
add_operator("eq", filter_op(lambda x, y: x == y))
add_operator("neq", filter_op(lambda x, y: x == y))
add_operator("gt", filter_op(lambda x, y: float(x) > float(y)))
add_operator("add", map_op(lambda x, y, z: float(x) > float(y)))

def main(query_string, input_file=None):
    q = query.parseString(query_string)

    INPUT = pd.DataFrame(dtype=str)
    if input_file is not None:
        with open(input_file, 'r') as f:
            lines = [f.strip().split(",") for f in f.readlines()]
            columns = [str(x) for x in range(len(lines[0]))]
            INPUT = pd.DataFrame(lines, columns=columns, dtype=str)

    add_operator("input", join_op(INPUT))

    df = pd.DataFrame([{"key": 0}])

    def get_atom(atom):
        if atom.var:
            return Var(atom.var)
        if atom.const:
            return Const(atom.const)

    for part in q.parts:
        pargs = [get_atom(a.parg.atom) for a in part.mappings if a.parg]
        kwargs = {a.kwarg.name: get_atom(a.kwarg.value) for a in part.mappings if a.kwarg}
        negated = bool(part.negated)

        operator = operators[part.identifier]
        if not operator:
            raise Exception("Can't find operator named '%s'" % part.identifier)
        df = operator(negated, df, *pargs, **kwargs)


    print(df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse some thangs")
    parser.add_argument("query", type=str, help="Query string.")
    parser.add_argument("--input", type=str, help="Input file. Format as CSV and it'll be available as input()s")

    args = parser.parse_args()
    main(args.query, args.input)
