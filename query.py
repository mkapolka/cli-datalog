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
        "username": "mkapolka",
    },{
        "tid": "234",
        "name": "snard",
        "username": "superemily",
    },{
        "tid": "456",
        "name": "hi",
        "username": "iateyourpie",
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

    def __str__(self):
        return "Var(%s)" % self.value()

    def __repr__(self):
        return str(self)

class Const(object):
    def __init__(self, value):
        self._value = str(value)

    def is_var(self):
        return False

    def value(self):
        return self._value

    def __str__(self):
        return "Const(%s)" % self.value()

    def __repr__(self):
        return str(self)


def arg_bound(df, arg):
    if not arg:
        return False
    if arg.is_var():
        return arg.value() in df
    else:
        return True

def join(df1, df2, kwargs, anti=False):
    # outside keys
    df1 = df1.assign(key=0)
    df2 = df2.assign(key=0)

    # Existing keys
    existing_keys = [m for m in kwargs.items() if m[1].is_var() and m[1].value() in df1.keys()]

    # Add consts
    consts = {m[0]: m[1].value() for m in kwargs.items() if not m[1].is_var()}
    df1 = df1.assign(**consts)

    left_keys = [m[1].value() for m in existing_keys if m[1].is_var()] + ["key"] + list(consts.keys())
    right_keys = [m[0] for m in existing_keys] + ["key"] + list(consts.keys())

    if anti:
        # Anti join
        output = df1.merge(df2, how="left", left_on=left_keys, right_on=right_keys, indicator=True)    
        output = output[output._merge == "left_only"]
    else:
        output = df1.merge(df2, left_on=left_keys, right_on=right_keys)

        # Rename new keys
        key_mapping = {
            m[0]: m[1].value() 
                for m in kwargs.items()
                if m[1].is_var() and m[1].value() not in df1.keys()
        }
        output = output.rename(columns=key_mapping)

    # Filter out unused vars
    var_names = [m.value() for m in kwargs.values() if m.is_var()]
    cols = set(list(df1.keys()) + var_names)
    output = output[cols]
    return output


def queryable(query_keys=None, available_keys=None, query=None, args=None):
    static_args = args
    def op(neg, df, *args, **kwargs):
        # Check for positionals
        if args:
            raise Exception("Positional args are not supported.")
        # Check for unknown keys and raise.
        unknown_args = {k for k in kwargs.keys() if k not in query_keys and k not in available_keys}
        if unknown_args:
            raise Exception("Unknown args: %s" % ', '.join(unknown_args))

        # Check for queryable keys
        available = {k: v for k, v in kwargs.items() if arg_bound(df, v) and k in query_keys}
        # Rename variable names
        qdf = df.rename(columns={arg.value(): k for k, arg in available.items() if arg.is_var()})
        # Add constant values
        qdf = qdf.assign(**{k: arg.value() for k, arg in available.items() if not arg.is_var()})
        # Filter out unnecessary info
        qdf = qdf[available.keys()]

        result = query(qdf)

        return join(df, result, kwargs, anti=neg)
    return op

def join_op(pseudo_df):
    other = pd.DataFrame(pseudo_df)
    other = other.assign(key=0)
    def op(neg, df, *args, **kwargs):
        for i, arg in enumerate(args):
            kwargs[str(i)] = arg
        return join(df, other, kwargs, anti=neg)
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

add_operator("db1", join_op(DB1))
add_operator("db2", join_op(DB2))
add_operator("eq", filter_op(lambda x, y: x == y))
add_operator("neq", filter_op(lambda x, y: x == y))
add_operator("gt", filter_op(lambda x, y: float(x) > float(y)))

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

        operator = operators.get(part.identifier, None)
        if not operator:
            raise Exception("Can't find operator named '%s'" % part.identifier)
        df = operator(negated, df, *pargs, **kwargs)

    print(df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse some thangs")
    parser.add_argument("query", type=str, help="Query string.")
    parser.add_argument("--input", type=str, help="Input file. Format as CSV and it'll be available as input()s")

    # Mixin the twitch operators
    from twitch import get_operators
    for n, f in get_operators().items():
        add_operator(n, f)
    args = parser.parse_args()
    main(args.query, args.input)
