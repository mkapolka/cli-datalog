# Entry point helpers
import argparse

from .query import DEFAULT_OPERATORS

class Main(object):
    def __init__(self):
        # Add builtin predicates
        self.predicates = dict(DEFAULT_OPERATORS)

        # Setup the default argument parser
        self.parser = argparse.ArgumentParser(description="Parse some thangs")
        self.parser.add_argument("query", type=str, help="Query string.")
        self.parser.add_argument("--input", type=str, help="Input file. Format as CSV and it'll be available as input()s")

    def get_argparser(self):
        return self.parser

    def main(self):
        args = self.parser.parse_args()
        input_file = args.input
        query_string = args.query

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

    # Adds a predicate to this environment.
    def add_predicate(self, name, f):
        if name not in self.predicates:
            self.predicates[name] = f
        else:
            raise Exception("Predicate %s already exists." % name)
