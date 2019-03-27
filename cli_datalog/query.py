import pandas as pd
import re

from .parsing import query

operators = {}

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

def resolve_args(df, args):
    unbound = [arg.value() for arg in args if arg.value() not in df]
    if unbound:
        raise Exception("Unbound variables: %s" % ', '.join(unbound))
    return df.rename(columns={arg.value(): i for i, arg in enumerate(args)})[range(len(args))]

def resolve_kwargs(df, kwargs):
    # Check for queryable keys
    available = {k: v for k, v in kwargs.items() if arg_bound(df, v)}
    # Rename variable names
    output = df.rename(columns={arg.value(): k for k, arg in available.items() if arg.is_var()})
    # Add constant values
    output = output.assign(**{k: arg.value() for k, arg in available.items() if not arg.is_var()})
    # Filter out unnecessary info
    output = output[available.keys()]
    return output

def join(df1, df2, kwargs, anti=False):
    # outside keys
    df1 = df1.assign(key=0)
    df2 = df2.assign(key=0)

    old_keys = df1.keys()

    # Existing keys
    existing_keys = [m for m in kwargs.items() if m[1].is_var() and m[1].value() in df1.keys()]

    # Add consts
    consts = {m[0]: m[1].value() for m in kwargs.items() if not m[1].is_var()}
    df1 = df1.assign(**consts)
    available_consts = [c for c in consts.keys() if c in df1 and c in df2]

    left_keys = [m[1].value() for m in existing_keys if m[1].is_var()] + ["key"] + list(available_consts)
    right_keys = [m[0] for m in existing_keys] + ["key"] + list(available_consts)

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
    cols = set(list(old_keys) + var_names)
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

        qdf = resolve_kwargs(df, kwargs)
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

def csv_op(neg, df, *args, **kwargs):
    print(resolve_args(df, args).to_csv(header=False, index=False))
    return df

def print_op(neg, df, *args, **kwargs):
    print(df)
    return df

def csv_input_op(neg, df, filename, *args, **kwargs):
    if neg:
        raise Exception("Negation not supported for csv.read")
    filename = filename.value()

    # constant values
    const_df = pd.DataFrame([{i: a.value() for i, a in enumerate(args) if not a.is_var()}])
    const_df = const_df.assign(key=0)

    file_df = pd.read_csv(filename, header=None, dtype=str)
    file_df = file_df.assign(key=0)

    file_df = file_df.merge(const_df, how="inner")
    file_df = file_df.rename(columns={i: a.value() for i, a in enumerate(args) if a.is_var()})
    return df.merge(file_df, how="inner")

DEFAULT_OPERATORS = {
    "print": print_op,
    "eq": filter_op(lambda x, y: x == y),
    "neq": filter_op(lambda x, y: x == y),
    "gt": filter_op(lambda x, y: float(x) > float(y)),
    "csv": csv_op,
    "csv.read": csv_input_op,
}
