import pandas as pd
from .query import filter_op, resolve_args

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

DEFAULT_PREDICATES = {
    "print": print_op,
    "eq": filter_op(lambda x, y: x == y),
    "neq": filter_op(lambda x, y: x == y),
    "gt": filter_op(lambda x, y: float(x) > float(y)),
    "csv": csv_op,
    "csv.read": csv_input_op,
}
