# based on https://github.com/pyparsing/pyparsing/blob/master/examples/select_parser.py
from pyparsing import *


def replace_lpar(tokens):
    return "["


def replace_rpar(tokens):
    return "["


ParserElement.enablePackrat()

LPAR, RPAR, COMMA = map(Suppress, "(),")
DOT, STAR = map(Literal, ".*")

# keywords
keywords = {k: CaselessKeyword(k) for k in 'AND NOT OR NULL IS BETWEEN CASE WHEN THEN IN LIKE'.split()}
vars().update(keywords)

any_keyword = MatchFirst(keywords.values())

quoted_identifier = QuotedString('"', escQuote='""')
identifier = (~any_keyword + Word(alphas, alphanums + "_")).setParseAction(pyparsing_common.upcaseTokens) | \
             quoted_identifier
identifier = identifier.setResultsName("col")
# expression
expr = Forward().setName("expression")

numeric_literal = pyparsing_common.number
string_literal = QuotedString("'", escQuote="''")
literal_value = (numeric_literal | string_literal | NULL)

in_list = LPAR.setParseAction(replace_lpar) + Group(delimitedList(expr)).setResultsName("values_list") + RPAR.setParseAction(replace_rpar)

expr_term = (
        in_list
        | literal_value
        | Group(identifier)
)



NOT_NULL = Group(NOT + NULL)
NOT_BETWEEN = Group(NOT + BETWEEN)
NOT_IN = Group(NOT + IN)
NOT_LIKE = Group(NOT + LIKE)

UNARY, BINARY, TERNARY = 1, 2, 3
expr << infixNotation(
    expr_term,
    [
        (oneOf("- +") | NOT, UNARY, opAssoc.RIGHT),
        (NOT_NULL, UNARY, opAssoc.LEFT),
        ("||", BINARY, opAssoc.LEFT),
        (oneOf("* / %"), BINARY, opAssoc.LEFT),
        (oneOf("+ -"), BINARY, opAssoc.LEFT),
        (oneOf("< <= > >="), BINARY, opAssoc.LEFT),
        (
            oneOf("= != <>")
            | IS
            | IN
            | LIKE
            | NOT_IN
            | NOT_LIKE,
            BINARY,
            opAssoc.LEFT,
        ),
        ((BETWEEN | NOT_BETWEEN, AND), TERNARY, opAssoc.LEFT),
        (
            (IN | NOT_IN) + in_list,
            UNARY,
            opAssoc.LEFT,
        ),
        (AND, BINARY, opAssoc.LEFT),
        (OR, BINARY, opAssoc.LEFT),
    ]
)


def extract_identifiers(parsed_expression):
    identifiers = []
    if isinstance(parsed_expression, ParseResults):
        for item in parsed_expression:
            if isinstance(item, ParseResults) and item.col:
                identifiers.append(item.col[0].upper())
            elif isinstance(item, ParseResults):
                identifiers.extend(extract_identifiers(item))
    return identifiers


def parse(expression):
    return expr.parseString(expression)[0]


def main():
    tests = """\
        z > 100
        1=1 and b='yes'
        (1=1 or 2=3) and b='yes'
        (1.0 + bonus)
        bar BETWEEN +180 AND +10E9
        b In ('4')
        C >= CURRENT_Time
        dave != "Dave" 
        dave is not null
        pete is null or peter is not null
        a >= 10 * (2 + 3)
        frank = 'is ''scary'''
        space IS NOT null
        ff NOT IN (1,2,4,5)
        ff not between 3 and 9
        ff not like 'bob%'
    """

    success, _ = expr.runTests(tests)
    print("\n{}".format("OK" if success else "FAIL"))
    return 0 if success else 1


if __name__ == "__main__":
#    main()
    p = expr.parseString("ff NOT IN (1,2,4,5) and dd in ('a','b')")
    print(p[0])
