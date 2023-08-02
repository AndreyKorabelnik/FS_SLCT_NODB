# based on https://github.com/pyparsing/pyparsing/blob/master/examples/select_parser.py
from pyparsing import *


def transform_lpar(tokens):
    return "(["


def transform_rpar(tokens):
    return "])"


def transform_keyword(tokens):
    return f' {tokens[0]} '


def transform_and(tokens):
    return f' & '


def transform_or(tokens):
    return f' | '


def transform_identifier(tokens):
    return f"df['{tokens[0]}']"


def transform_like(tokens):
    return '.str.contains'


def transform_not_like(tokens):
    return '.str.~contains'


def transform_list(tokens):
    return ','.join(f"'{l}'" if isinstance(l, str) else str(l) for l in tokens[0])


def transform_not(tokens):
    return '~'


def transform_in(tokens):
    return '.isin'


def transform_not_in(tokens):
    return '.~isin'


def transform_like_string(tokens):
    return f"('{tokens[0]}')"


ParserElement.enablePackrat()

LPAR, RPAR, COMMA = map(Suppress, "(),")

# keywords
AND = CaselessKeyword('AND').setResultsName("keyword").setParseAction(transform_and)
OR = CaselessKeyword('OR').setResultsName("keyword").setParseAction(transform_or)
CASE = CaselessKeyword('CASE').setResultsName("keyword").setParseAction(transform_keyword)
WHEN = CaselessKeyword('WHEN').setResultsName("keyword").setParseAction(transform_keyword)
THEN = CaselessKeyword('THEN').setResultsName("keyword").setParseAction(transform_keyword)
IS = CaselessKeyword('IS').setResultsName("keyword").setParseAction(transform_keyword)
NULL = CaselessKeyword('NULL').setResultsName("keyword").setParseAction(transform_keyword)
NOT = CaselessKeyword('NOT').setResultsName("keyword").setParseAction(transform_not)
BETWEEN = CaselessKeyword('BETWEEN').setResultsName("keyword").setParseAction(transform_keyword)
IN = CaselessKeyword('IN').setResultsName("keyword").setParseAction(transform_in)
LIKE = CaselessKeyword('LIKE').setResultsName("keyword").setParseAction(transform_like)
NOT_NULL = Group(NOT + NULL).setResultsName("keyword").setParseAction(transform_keyword)
NOT_BETWEEN = Group(NOT + BETWEEN).setResultsName("keyword").setParseAction(transform_keyword)
NOT_IN = Group(NOT + IN).setResultsName("keyword").setParseAction(transform_not_in)
NOT_LIKE = Group(NOT + LIKE).setResultsName("keyword").setParseAction(transform_not_like)

keywords = [AND, OR, LIKE, IN]  # todo add others
any_keyword = MatchFirst(keywords)

quoted_identifier = QuotedString('"', escQuote='""')
identifier = (~any_keyword + Word(alphas, alphanums + "_")).setParseAction(pyparsing_common.upcaseTokens) | \
             quoted_identifier
identifier = identifier.setResultsName("col").setParseAction(transform_identifier)

like_string = QuotedString("'%", end_quote_char="%'").setResultsName("like_string").setParseAction(
    transform_like_string)

expr = Forward().setName("expression")

numeric_literal = pyparsing_common.number
string_literal = QuotedString("'", escQuote="''")
literal_value = (numeric_literal | string_literal | NULL)

in_list = LPAR.setParseAction(transform_lpar) + \
          Group(delimitedList(expr)).setResultsName("values_list").setParseAction(transform_list) + \
          RPAR.setParseAction(transform_rpar)

expr_term = (
        in_list
        | literal_value
        | Group(identifier)
)

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
            | NOT_IN,
            BINARY,
            opAssoc.LEFT,
        ),
        ((BETWEEN | NOT_BETWEEN, AND), TERNARY, opAssoc.LEFT),
        (
            (IN | NOT_IN) + in_list,
            UNARY,
            opAssoc.LEFT,
        ),
        (
            (LIKE | NOT_LIKE) + like_string,
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
                identifiers.append(item.col[4:-2])  # todo: ugly
            elif isinstance(item, ParseResults):
                identifiers.extend(extract_identifiers(item))
    return identifiers


def parse(expression):
    return expr.parseString(expression)[0]


def transform_to_pandas(expression):
    return expr.transformString(expression)


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
    # ~df['ff'].isin([1, 2, 4, 5]) & df['dd'].str.contains('#ASDF#')
    # df['FF'].isin([1, 2, 4, 5]) & df['DD'].str.contains('##ASDF##')
    # p = expr.parseString("ff IN (1,2,4,5) and dd like '%##ASDF##%'")
    p = expr.transformString("ff IN (1,2,4,5) and dd like '%##ASDF##%'")
    print(p)
