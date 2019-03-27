from pyparsing import (
    Word, alphanums, Literal, delimitedList, Group, Optional, alphas,
    sglQuotedString, dblQuotedString, removeQuotes,
    pyparsing_common
)

char = alphanums + "_.-!?"
identifier = Word(char)
var = Word(alphas.upper(), alphas.lower())
unquotedString = Word(alphas.lower(), alphanums)
string_const = (sglQuotedString | dblQuotedString).addParseAction(removeQuotes) | unquotedString
number_const = (pyparsing_common.number | pyparsing_common.fnumber)
const = string_const | number_const
atom = Group(const("const") | var("var"))
kwarg = Group(identifier("name") + Literal("=") + atom("value"))
parg = Group(atom("atom"))
arg = Group(kwarg("kwarg") | parg("parg"))
part = Group(
    Optional(Literal("not"))("negated") 
    + identifier("identifier") 
    + Literal("(") + Optional(delimitedList(arg))("mappings") + Literal(")")
)
query = delimitedList(part)("parts")
