# Entry point helpers
import argparse
import pandas as pd

from .predicates import DEFAULT_PREDICATES
from .query import join_op, Var, Const
from .parsing import query

class Interpreter(object):
    def __init__(self):
        # Add builtin predicates
        self.predicates = dict(DEFAULT_PREDICATES)

    def perform_query(self, query_string):
        q = query.parseString(query_string)

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

            operator = self.predicates.get(part.identifier, None)
            if not operator:
                raise Exception("Can't find operator named '%s'" % part.identifier)
            try:
                df = operator(negated, df, *pargs, **kwargs)
            except Exception as e:
                print("Error in predicate %s: %s" % (part.identifier, e))
                return

        print(df.drop('key', axis=1))

    # Adds a predicate to this environment.
    def add_predicate(self, name, f):
        if name not in self.predicates:
            self.predicates[name] = f
        else:
            raise Exception("Predicate %s already exists." % name)
