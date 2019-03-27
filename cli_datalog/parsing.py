from pyparsing import (
    Word, alphanums, Literal, delimitedList, Group, Optional,
    sglQuotedString, dblQuotedString, removeQuotes,
    pyparsing_common
)

char = alphanums + "_.-!?"
identifier = Word(char)
string_const = (sglQuotedString | dblQuotedString).addParseAction(removeQuotes)
number_const = (pyparsing_common.number | pyparsing_common.fnumber)
const = string_const | number_const
atom = Group(const("const") | identifier("var"))
kwarg = Group(identifier("name") + Literal("=") + atom("value"))
parg = Group(atom("atom"))
arg = Group(kwarg("kwarg") | parg("parg"))
part = Group(
    Optional(Literal("not"))("negated") 
    + identifier("identifier") 
    + Literal("(") + Optional(delimitedList(arg))("mappings") + Literal(")")
)
query = delimitedList(part)("parts")
