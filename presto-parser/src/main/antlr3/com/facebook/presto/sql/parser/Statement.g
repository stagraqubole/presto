/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

grammar Statement;

options {
    output = AST;
    ASTLabelType = CommonTree;
    memoize=true;
}

tokens {
    LEXER_ERROR;
    TERMINATOR;
    STATEMENT_LIST;
    GROUP_BY;
    ORDER_BY;
    SORT_ITEM;
    QUERY;
    WITH_LIST;
    WITH_QUERY;
    ALL_COLUMNS;
    SELECT_LIST;
    SELECT_ITEM;
    ALIASED_COLUMNS;
    TABLE_SUBQUERY;
    TABLE_VALUE;
    ROW_VALUE;
    EXPLAIN_OPTIONS;
    EXPLAIN_FORMAT;
    EXPLAIN_TYPE;
    TABLE;
    JOINED_TABLE;
    QUALIFIED_JOIN;
    CROSS_JOIN;
    INNER_JOIN;
    LEFT_JOIN;
    RIGHT_JOIN;
    FULL_JOIN;
    COMPARE;
    IS_NULL;
    IS_NOT_NULL;
    IS_DISTINCT_FROM;
    IN_LIST;
    SIMPLE_CASE;
    SEARCHED_CASE;
    FUNCTION_CALL;
    LITERAL;
    TIME_ZONE_CONVERSION;
    WINDOW;
    PARTITION_BY;
    UNBOUNDED_PRECEDING;
    UNBOUNDED_FOLLOWING;
    CURRENT_ROW;
    NEGATIVE;
    QNAME;
    SHOW_TABLES;
    SHOW_SCHEMAS;
    SHOW_CATALOGS;
    SHOW_COLUMNS;
    SHOW_PARTITIONS;
    SHOW_FUNCTIONS;
    USE_CATALOG;
    USE_SCHEMA;
    CREATE_TABLE;
    DROP_TABLE;
    CREATE_VIEW;
    DROP_VIEW;
    OR_REPLACE;
    TABLE_ELEMENT_LIST;
    COLUMN_DEF;
    NOT_NULL;
    ALIASED_RELATION;
    SAMPLED_RELATION;
    QUERY_SPEC;
    STRATIFY_ON;
}

@header {
    package com.facebook.presto.sql.parser;
}

@lexer::header {
    package com.facebook.presto.sql.parser;

    import java.util.EnumSet;
}

@members {
    @Override
    protected Object recoverFromMismatchedToken(IntStream input, int tokenType, BitSet follow)
            throws RecognitionException
    {
        throw new MismatchedTokenException(tokenType, input);
    }

    @Override
    public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
            throws RecognitionException
    {
        throw e;
    }

    @Override
    public String getErrorMessage(RecognitionException e, String[] tokenNames)
    {
        if (e.token.getType() == BACKQUOTED_IDENT) {
            return "backquoted identifiers are not supported; use double quotes to quote identifiers";
        }
        if (e.token.getType() == DIGIT_IDENT) {
            return "identifiers must not start with a digit; surround the identifier with double quotes";
        }
        return super.getErrorMessage(e, tokenNames);
    }
}

@lexer::members {
    private EnumSet<IdentifierSymbol> allowedIdentifierSymbols = EnumSet.noneOf(IdentifierSymbol.class);

    public void setAllowedIdentifierSymbols(EnumSet<IdentifierSymbol> allowedIdentifierSymbols)
    {
        this.allowedIdentifierSymbols = EnumSet.copyOf(allowedIdentifierSymbols);
    }

    @Override
    public void reportError(RecognitionException e)
    {
        throw new ParsingException(getErrorMessage(e, getTokenNames()), e);
    }
}

@rulecatch {
    catch (RecognitionException re) {
        throw new ParsingException(getErrorMessage(re, getTokenNames()), re);
    }
}


singleStatement
    : statement EOF -> statement
    ;

singleExpression
    : expr EOF -> expr
    ;

statement
    : query
    | explainStmt
    | showTablesStmt
    | showSchemasStmt
    | showCatalogsStmt
    | showColumnsStmt
    | showPartitionsStmt
    | showFunctionsStmt
    | useCollectionStmt
    | createTableStmt
    | dropTableStmt
    | createViewStmt
    | dropViewStmt
    ;

query
    : queryExpr -> ^(QUERY queryExpr)
    ;

queryExpr
    : withClause?
      ( (orderOrLimitQuerySpec) => orderOrLimitQuerySpec
      | queryExprBody orderClause? limitClause?
      )
      approximateClause?
    ;

orderOrLimitQuerySpec
    : simpleQuery (orderClause limitClause? | limitClause) -> ^(QUERY_SPEC simpleQuery orderClause? limitClause?)
    ;

queryExprBody
    : ( queryTerm -> queryTerm )
      ( UNION setQuant? queryTerm       -> ^(UNION $queryExprBody queryTerm setQuant?)
      | EXCEPT setQuant? queryTerm      -> ^(EXCEPT $queryExprBody queryTerm setQuant?)
      )*
    ;

queryTerm
    : ( queryPrimary -> queryPrimary )
      ( INTERSECT setQuant? queryPrimary -> ^(INTERSECT $queryTerm queryPrimary setQuant?) )*
    ;

queryPrimary
    : simpleQuery -> ^(QUERY_SPEC simpleQuery)
    | tableSubquery
    | explicitTable
    | tableValue
    ;

explicitTable
    : TABLE table -> table
    ;

tableValue
    : VALUES rowValue (',' rowValue)*  -> ^(TABLE_VALUE rowValue+)
    ;

rowValue
    : '(' expr (',' expr)* ')' -> ^(ROW_VALUE expr+)
    ;

simpleQuery
    : selectClause
      fromClause?
      whereClause?
      groupClause?
      havingClause?
    ;

restrictedSelectStmt
    : selectClause
      fromClause
    ;

approximateClause
    : APPROXIMATE AT number CONFIDENCE -> ^(APPROXIMATE number)
    ;

withClause
    : WITH r=RECURSIVE? withList -> ^(WITH $r? withList)
    ;

selectClause
    : SELECT selectExpr -> ^(SELECT selectExpr)
    ;

fromClause
    : FROM tableRef (',' tableRef)* -> ^(FROM tableRef+)
    ;

whereClause
    : WHERE expr -> ^(WHERE expr)
    ;

groupClause
    : GROUP BY expr (',' expr)* -> ^(GROUP_BY expr+)
    ;

havingClause
    : HAVING expr -> ^(HAVING expr)
    ;

orderClause
    : ORDER BY sortItem (',' sortItem)* -> ^(ORDER_BY sortItem+)
    ;

limitClause
    : LIMIT integer -> ^(LIMIT integer)
    ;

withList
    : withQuery (',' withQuery)* -> ^(WITH_LIST withQuery+)
    ;

withQuery
    : ident aliasedColumns? AS subquery -> ^(WITH_QUERY ident subquery aliasedColumns?)
    ;

selectExpr
    : setQuant? selectList
    ;

setQuant
    : DISTINCT
    | ALL
    ;

selectList
    : selectSublist (',' selectSublist)* -> ^(SELECT_LIST selectSublist+)
    ;

selectSublist
    : expr (AS? ident)? -> ^(SELECT_ITEM expr ident?)
    | qname '.' '*'     -> ^(ALL_COLUMNS qname)
    | '*'               -> ALL_COLUMNS
    ;

tableRef
    : ( tableFactor -> tableFactor )
      ( CROSS JOIN tableFactor                 -> ^(CROSS_JOIN $tableRef tableFactor)
      | joinType JOIN tableFactor joinCriteria -> ^(QUALIFIED_JOIN joinType joinCriteria $tableRef tableFactor)
      | NATURAL joinType JOIN tableFactor      -> ^(QUALIFIED_JOIN joinType NATURAL $tableRef tableFactor)
      )*
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    | POISSONIZED
    ;

stratifyOn
    : STRATIFY ON '(' expr (',' expr)* ')' -> ^(STRATIFY_ON expr+)
    ;

tableFactor
    : ( tablePrimary -> tablePrimary )
      ( TABLESAMPLE sampleType '(' expr ')' RESCALED? stratifyOn? -> ^(SAMPLED_RELATION $tableFactor sampleType expr RESCALED? stratifyOn?) )?
    ;

tablePrimary
    : ( relation -> relation )
      ( AS? ident aliasedColumns? -> ^(ALIASED_RELATION $tablePrimary ident aliasedColumns?) )?
    ;

relation
    : table
    | ('(' tableRef ')') => joinedTable
    | tableSubquery
    ;

table
    : qname -> ^(TABLE qname)
    ;

tableSubquery
    : '(' query ')' -> ^(TABLE_SUBQUERY query)
    ;

joinedTable
    : '(' tableRef ')' -> ^(JOINED_TABLE tableRef)
    ;

joinType
    : INNER?       -> INNER_JOIN
    | LEFT OUTER?  -> LEFT_JOIN
    | RIGHT OUTER? -> RIGHT_JOIN
    | FULL OUTER?  -> FULL_JOIN
    ;

joinCriteria
    : ON expr                          -> ^(ON expr)
    | USING '(' ident (',' ident)* ')' -> ^(USING ident+)
    ;

aliasedColumns
    : '(' ident (',' ident)* ')' -> ^(ALIASED_COLUMNS ident+)
    ;

expr
    : orExpression
    ;

orExpression
    : andExpression (OR^ andExpression)*
    ;

andExpression
    : notExpression (AND^ notExpression)*
    ;

notExpression
    : (NOT^)* booleanTest
    ;

booleanTest
    : booleanPrimary
    ;

booleanPrimary
    : predicate
    | EXISTS subquery -> ^(EXISTS subquery)
    ;

predicate
    : (predicatePrimary -> predicatePrimary)
      ( cmpOp e=predicatePrimary                                  -> ^(cmpOp $predicate $e)
      | IS DISTINCT FROM e=predicatePrimary                       -> ^(IS_DISTINCT_FROM $predicate $e)
      | IS NOT DISTINCT FROM e=predicatePrimary                   -> ^(NOT ^(IS_DISTINCT_FROM $predicate $e))
      | BETWEEN min=predicatePrimary AND max=predicatePrimary     -> ^(BETWEEN $predicate $min $max)
      | NOT BETWEEN min=predicatePrimary AND max=predicatePrimary -> ^(NOT ^(BETWEEN $predicate $min $max))
      | LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?      -> ^(LIKE $predicate $e $x?)
      | NOT LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?  -> ^(NOT ^(LIKE $predicate $e $x?))
      | IS NULL                                                   -> ^(IS_NULL $predicate)
      | IS NOT NULL                                               -> ^(IS_NOT_NULL $predicate)
      | IN inList                                                 -> ^(IN $predicate inList)
      | NOT IN inList                                             -> ^(NOT ^(IN $predicate inList))
      )*
    ;

predicatePrimary
    : (numericExpr -> numericExpr)
      ( '||' e=numericExpr -> ^(FUNCTION_CALL ^(QNAME IDENT["concat"]) $predicatePrimary $e) )*
    ;

numericExpr
    : numericTerm (('+' | '-')^ numericTerm)*
    ;

numericTerm
    : numericFactor (('*' | '/' | '%')^ numericFactor)*
    ;

numericFactor
    : exprWithTimeZone
    | '+' numericFactor -> numericFactor
    | '-' numericFactor -> ^(NEGATIVE numericFactor)
    ;

exprWithTimeZone
    : (exprPrimary -> exprPrimary)
      (
        // todo this should have a full tree node to preserve the syntax
        AT TIME ZONE STRING           -> ^(FUNCTION_CALL ^(QNAME IDENT["at_time_zone"]) $exprWithTimeZone STRING)
      | AT TIME ZONE intervalLiteral    -> ^(FUNCTION_CALL ^(QNAME IDENT["at_time_zone"]) $exprWithTimeZone intervalLiteral)
      )?
    ;

exprPrimary
    : NULL
    | (literal) => literal
    | qnameOrFunction
    | specialFunction
    | number
    | bool
    | STRING
    | caseExpression
    | ('(' expr ')') => ('(' expr ')' -> expr)
    | subquery
    ;

qnameOrFunction
    : (qname -> qname)
      ( ('(' '*' ')' over?                          -> ^(FUNCTION_CALL $qnameOrFunction over?))
      | ('(' setQuant? expr? (',' expr)* ')' over?  -> ^(FUNCTION_CALL $qnameOrFunction over? setQuant? expr*))
      )?
    ;

inList
    : ('(' expr) => ('(' expr (',' expr)* ')' -> ^(IN_LIST expr+))
    | subquery
    ;

sortItem
    : expr ordering nullOrdering? -> ^(SORT_ITEM expr ordering nullOrdering?)
    ;

ordering
    : -> ASC
    | ASC
    | DESC
    ;

nullOrdering
    : NULLS FIRST -> FIRST
    | NULLS LAST  -> LAST
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

subquery
    : '(' query ')' -> query
    ;

literal
    : (VARCHAR) => VARCHAR STRING     -> ^(LITERAL IDENT["VARCHAR"] STRING)
    | (BIGINT) => BIGINT STRING       -> ^(LITERAL IDENT["BIGINT"] STRING)
    | (DOUBLE) => DOUBLE STRING       -> ^(LITERAL IDENT["DOUBLE"] STRING)
    | (BOOLEAN) => BOOLEAN STRING     -> ^(LITERAL IDENT["BOOLEAN"] STRING)
    | (DATE) => DATE STRING           -> ^(LITERAL IDENT["DATE"] STRING)
    | (TIME) => TIME STRING           -> ^(TIME STRING)
    | (TIMESTAMP) => TIMESTAMP STRING -> ^(TIMESTAMP STRING)
    | (INTERVAL) => intervalLiteral
    | ident STRING                    -> ^(LITERAL ident STRING)
    ;

intervalLiteral
    : INTERVAL intervalSign? STRING s=intervalField ( TO e=intervalField )? -> ^(INTERVAL STRING intervalSign? $s $e?)
    ;

intervalSign
    : '+' ->
    | '-' -> NEGATIVE
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

specialFunction
    : CURRENT_DATE
    | CURRENT_TIME ('(' integer ')')?              -> ^(CURRENT_TIME integer?)
    | CURRENT_TIMESTAMP ('(' integer ')')?         -> ^(CURRENT_TIMESTAMP integer?)
    | LOCALTIME ('(' integer ')')?                 -> ^(LOCALTIME integer?)
    | LOCALTIMESTAMP ('(' integer ')')?            -> ^(LOCALTIMESTAMP integer?)
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')' -> ^(FUNCTION_CALL ^(QNAME IDENT["substr"]) expr expr expr?)
    | EXTRACT '(' ident FROM expr ')'              -> ^(EXTRACT ident expr)
    | CAST '(' expr AS type ')'                    -> ^(CAST expr type)
    ;

// TODO: this should be 'dataType', which supports arbitrary type specifications. For now we constrain to simple types
type
    : VARCHAR                    -> IDENT["VARCHAR"]
    | BIGINT                     -> IDENT["BIGINT"]
    | DOUBLE                     -> IDENT["DOUBLE"]
    | BOOLEAN                    -> IDENT["BOOLEAN"]
    | TIME WITH TIME ZONE        -> IDENT["TIME WITH TIME ZONE"]
    | TIMESTAMP WITH TIME ZONE   -> IDENT["TIMESTAMP WITH TIME ZONE"]
    | ident
    ;

caseExpression
    : NULLIF '(' expr ',' expr ')'          -> ^(NULLIF expr expr)
    | COALESCE '(' expr (',' expr)* ')'     -> ^(COALESCE expr+)
    | CASE expr whenClause+ elseClause? END -> ^(SIMPLE_CASE expr whenClause+ elseClause?)
    | CASE whenClause+ elseClause? END      -> ^(SEARCHED_CASE whenClause+ elseClause?)
    | IF '(' expr ',' expr (',' expr)? ')'  -> ^(IF expr expr expr?)
    ;

whenClause
    : WHEN expr THEN expr -> ^(WHEN expr expr)
    ;

elseClause
    : ELSE expr -> expr
    ;

over
    : OVER '(' window ')' -> window
    ;

window
    : p=windowPartition? o=orderClause? f=windowFrame? -> ^(WINDOW $p? $o ?$f?)
    ;

windowPartition
    : PARTITION BY expr (',' expr)* -> ^(PARTITION_BY expr+)
    ;

windowFrame
    : RANGE frameBound                        -> ^(RANGE frameBound)
    | ROWS frameBound                         -> ^(ROWS frameBound)
    | RANGE BETWEEN frameBound AND frameBound -> ^(RANGE frameBound frameBound)
    | ROWS BETWEEN frameBound AND frameBound  -> ^(ROWS frameBound frameBound)
    ;

frameBound
    : UNBOUNDED PRECEDING -> UNBOUNDED_PRECEDING
    | UNBOUNDED FOLLOWING -> UNBOUNDED_FOLLOWING
    | CURRENT ROW         -> CURRENT_ROW
    | expr
      ( PRECEDING -> ^(PRECEDING expr)
      | FOLLOWING -> ^(FOLLOWING expr)
      )
    ;

useCollectionStmt
    : USE CATALOG ident -> ^(USE_CATALOG ident)
    | USE SCHEMA ident -> ^(USE_SCHEMA ident)
    ;

explainStmt
    : EXPLAIN explainOptions? statement -> ^(EXPLAIN explainOptions? statement)
    ;

explainOptions
    : '(' explainOption (',' explainOption)* ')' -> ^(EXPLAIN_OPTIONS explainOption+)
    ;

explainOption
    : FORMAT TEXT      -> ^(EXPLAIN_FORMAT TEXT)
    | FORMAT GRAPHVIZ  -> ^(EXPLAIN_FORMAT GRAPHVIZ)
    | FORMAT JSON      -> ^(EXPLAIN_FORMAT JSON)
    | TYPE LOGICAL     -> ^(EXPLAIN_TYPE LOGICAL)
    | TYPE DISTRIBUTED -> ^(EXPLAIN_TYPE DISTRIBUTED)
    ;

showTablesStmt
    : SHOW TABLES from=showTablesFrom? like=showTablesLike? -> ^(SHOW_TABLES $from? $like?)
    ;

showTablesFrom
    : (FROM | IN) qname -> ^(FROM qname)
    ;

showTablesLike
    : LIKE s=STRING -> ^(LIKE $s)
    ;

showSchemasStmt
    : SHOW SCHEMAS from=showSchemasFrom? -> ^(SHOW_SCHEMAS $from?)
    ;

showSchemasFrom
    : (FROM | IN) ident -> ^(FROM ident)
    ;

showCatalogsStmt
    : SHOW CATALOGS -> SHOW_CATALOGS
    ;

showColumnsStmt
    : SHOW COLUMNS (FROM | IN) qname -> ^(SHOW_COLUMNS qname)
    | DESCRIBE qname                 -> ^(SHOW_COLUMNS qname)
    | DESC qname                     -> ^(SHOW_COLUMNS qname)
    ;

showPartitionsStmt
    : SHOW PARTITIONS (FROM | IN) qname w=whereClause? o=orderClause? l=limitClause? -> ^(SHOW_PARTITIONS qname $w? $o? $l?)
    ;

showFunctionsStmt
    : SHOW FUNCTIONS -> SHOW_FUNCTIONS
    ;

dropTableStmt
    : DROP TABLE qname -> ^(DROP_TABLE qname)
    ;

createTableStmt
    : CREATE TABLE qname s=tableContentsSource -> ^(CREATE_TABLE qname $s)
    ;

createViewStmt
    : CREATE r=orReplace? VIEW qname s=tableContentsSource -> ^(CREATE_VIEW qname $s $r?)
    ;

dropViewStmt
    : DROP VIEW qname -> ^(DROP_VIEW qname)
    ;

orReplace
    : OR REPLACE -> OR_REPLACE
    ;

tableContentsSource
    : AS query -> query
    ;

tableElementList
    : '(' tableElement (',' tableElement)* ')' -> ^(TABLE_ELEMENT_LIST tableElement+)
    ;

tableElement
    : ident dataType columnConstDef* -> ^(COLUMN_DEF ident dataType columnConstDef*)
    ;

dataType
    : charType
    | exactNumType
    | dateType
    ;

charType
    : CHAR charlen?              -> ^(CHAR charlen?)
    | CHARACTER charlen?         -> ^(CHAR charlen?)
    | VARCHAR charlen?           -> ^(VARCHAR charlen?)
    | CHAR VARYING charlen?      -> ^(VARCHAR charlen?)
    | CHARACTER VARYING charlen? -> ^(VARCHAR charlen?)
    ;

charlen
    : '(' integer ')' -> integer
    ;

exactNumType
    : NUMERIC numlen? -> ^(NUMERIC numlen?)
    | DECIMAL numlen? -> ^(NUMERIC numlen?)
    | DEC numlen?     -> ^(NUMERIC numlen?)
    | INTEGER         -> ^(INTEGER)
    | INT             -> ^(INTEGER)
    ;

numlen
    : '(' p=integer (',' s=integer)? ')' -> $p $s?
    ;

dateType
    : DATE -> ^(DATE)
    ;

columnConstDef
    : columnConst -> ^(CONSTRAINT columnConst)
    ;

columnConst
    : NOT NULL -> NOT_NULL
    ;

qname
    : ident ('.' ident)* -> ^(QNAME ident+)
    ;

ident
    : IDENT
    | QUOTED_IDENT
    | nonReserved  -> IDENT[$nonReserved.text]
    ;

number
    : DECIMAL_VALUE
    | INTEGER_VALUE
    ;

bool
    : TRUE
    | FALSE
    ;

integer
    : INTEGER_VALUE
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW
    | DATE | TIME | TIMESTAMP | INTERVAL
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED
    | TABLESAMPLE | SYSTEM | BERNOULLI | POISSONIZED | USE | SCHEMA | CATALOG | JSON | TO
    | RESCALED | APPROXIMATE | AT | CONFIDENCE
    | VIEW | REPLACE
    ;

SELECT: 'SELECT';
FROM: 'FROM';
AS: 'AS';
ALL: 'ALL';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
APPROXIMATE: 'APPROXIMATE';
AT: 'AT';
CONFIDENCE: 'CONFIDENCE';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULLS: 'NULLS';
FIRST: 'FIRST';
LAST: 'LAST';
ESCAPE: 'ESCAPE';
ASC: 'ASC';
DESC: 'DESC';
SUBSTRING: 'SUBSTRING';
FOR: 'FOR';
DATE: 'DATE';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
INTERVAL: 'INTERVAL';
YEAR: 'YEAR';
MONTH: 'MONTH';
DAY: 'DAY';
HOUR: 'HOUR';
MINUTE: 'MINUTE';
SECOND: 'SECOND';
ZONE: 'ZONE';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
EXTRACT: 'EXTRACT';
COALESCE: 'COALESCE';
NULLIF: 'NULLIF';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
IF: 'IF';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
USING: 'USING';
ON: 'ON';
OVER: 'OVER';
PARTITION: 'PARTITION';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
WITH: 'WITH';
RECURSIVE: 'RECURSIVE';
VALUES: 'VALUES';
CREATE: 'CREATE';
TABLE: 'TABLE';
VIEW: 'VIEW';
REPLACE: 'REPLACE';
CHAR: 'CHAR';
CHARACTER: 'CHARACTER';
VARYING: 'VARYING';
VARCHAR: 'VARCHAR';
NUMERIC: 'NUMERIC';
NUMBER: 'NUMBER';
DECIMAL: 'DECIMAL';
DEC: 'DEC';
INTEGER: 'INTEGER';
INT: 'INT';
DOUBLE: 'DOUBLE';
BIGINT: 'BIGINT';
BOOLEAN: 'BOOLEAN';
CONSTRAINT: 'CONSTRAINT';
DESCRIBE: 'DESCRIBE';
EXPLAIN: 'EXPLAIN';
FORMAT: 'FORMAT';
TYPE: 'TYPE';
TEXT: 'TEXT';
GRAPHVIZ: 'GRAPHVIZ';
JSON: 'JSON';
LOGICAL: 'LOGICAL';
DISTRIBUTED: 'DISTRIBUTED';
CAST: 'CAST';
SHOW: 'SHOW';
TABLES: 'TABLES';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
USE: 'USE';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
DROP: 'DROP';
UNION: 'UNION';
EXCEPT: 'EXCEPT';
INTERSECT: 'INTERSECT';
TO: 'TO';
SYSTEM: 'SYSTEM';
BERNOULLI: 'BERNOULLI';
POISSONIZED: 'POISSONIZED';
TABLESAMPLE: 'TABLESAMPLE';
RESCALED: 'RESCALED';
STRATIFY: 'STRATIFY';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
        { setText(getText().substring(1, getText().length() - 1).replace("''", "'")); }
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENT
    : (LETTER | '_') (LETTER | DIGIT | '_' | '\@' | ':')*
        { IdentifierSymbol.validateIdentifier(input, getText(), allowedIdentifierSymbols); }
    ;

DIGIT_IDENT
    : DIGIT (LETTER | DIGIT | '_' | '\@' | ':')+
    ;

QUOTED_IDENT
    : '"' ( ~'"' | '""' )* '"'
        { setText(getText().substring(1, getText().length() - 1).replace("\"\"", "\"")); }
    ;

BACKQUOTED_IDENT
    : '`' ( ~'`' | '``' )* '`'
        { setText(getText().substring(1, getText().length() - 1).replace("``", "`")); }
    ;

fragment EXPONENT
    : 'E' ('+' | '-')? DIGIT+
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : 'A'..'Z'
    ;

COMMENT
    : '--' (~('\r' | '\n'))* ('\r'? '\n')?     { $channel=HIDDEN; }
    | '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel=HIDDEN; }
    ;
