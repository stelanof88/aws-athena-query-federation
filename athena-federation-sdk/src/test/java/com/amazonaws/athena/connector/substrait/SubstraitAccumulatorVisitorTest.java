/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connector.substrait;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubstraitAccumulatorVisitorTest
{
    private List<SubstraitTypeAndValue> accumulator;
    private RelDataType schema;
    private SubstraitAccumulatorVisitor visitor;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    public void setUp()
    {
        accumulator = new ArrayList<>();
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        schema = typeFactory.createStructType(
                Arrays.asList(
                        Pair.of("int_col", typeFactory.createSqlType(SqlTypeName.INTEGER)),
                        Pair.of("bigint_col", typeFactory.createSqlType(SqlTypeName.BIGINT)),
                        Pair.of("varchar_col", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("bool_col", typeFactory.createSqlType(SqlTypeName.BOOLEAN)),
                        Pair.of("float_col", typeFactory.createSqlType(SqlTypeName.FLOAT))));
        visitor = new SubstraitAccumulatorVisitor(accumulator, schema);
    }

    // --- visit(SqlIdentifier): simple vs non-simple ---

    @Test
    public void testVisitSimpleIdentifier()
    {
        SqlIdentifier id = new SqlIdentifier("int_col", SqlParserPos.ZERO);
        assertEquals(id, visitor.visit(id));
    }

    @Test
    public void testVisitNonSimpleIdentifier()
    {
        SqlIdentifier id = new SqlIdentifier(Arrays.asList("s", "int_col"), SqlParserPos.ZERO);
        assertEquals(id, visitor.visit(id));
    }

    // --- visit(SqlLiteral): standalone literal without column context ---

    @Test
    public void testVisitStandaloneLiteralSkipped()
    {
        SqlLiteral lit = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        assertEquals(lit, visitor.visit(lit));
        assertEquals(0, accumulator.size());
    }

    // --- addToAccumulator: NlsString vs non-NlsString, field not found, null value ---

    @Test
    public void testAccumulatorNonNlsStringValue()
    {
        visitor.visit(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("int_col", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("123", SqlParserPos.ZERO)));
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.INTEGER, accumulator.get(0).getType());
        assertEquals("int_col", accumulator.get(0).getColumnName());
    }

    @Test
    public void testAccumulatorNlsStringValue()
    {
        visitor.visit(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("varchar_col", SqlParserPos.ZERO),
                SqlLiteral.createCharString("hello", SqlParserPos.ZERO)));
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.VARCHAR, accumulator.get(0).getType());
        assertEquals("hello", accumulator.get(0).getValue());
    }

    @Test
    public void testAccumulatorFieldNotFoundThrows()
    {
        SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("no_such_col", SqlParserPos.ZERO),
                SqlLiteral.createCharString("x", SqlParserPos.ZERO));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> visitor.visit(call));
        assertTrue(ex.getMessage().contains("field no_such_col not found"));
    }

    @Test
    public void testAccumulatorNullValueThrows()
    {
        RelDataType nullSchema = typeFactory.createStructType(
                Arrays.asList(Pair.of("null_col", typeFactory.createSqlType(SqlTypeName.NULL))));
        SubstraitAccumulatorVisitor v = new SubstraitAccumulatorVisitor(accumulator, nullSchema);
        SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("null_col", SqlParserPos.ZERO),
                SqlLiteral.createNull(SqlParserPos.ZERO));
        NullPointerException ex = assertThrows(NullPointerException.class, () -> v.visit(call));
        assertTrue(ex.getMessage().contains("value is null"));
    }

    // --- handleBinaryComparison: parameterized across all 6 operators ---

    static Stream<Arguments> binaryComparisonOperators()
    {
        return Stream.of(
                Arguments.of(SqlStdOperatorTable.EQUALS, "EQUALS"),
                Arguments.of(SqlStdOperatorTable.NOT_EQUALS, "NOT_EQUALS"),
                Arguments.of(SqlStdOperatorTable.GREATER_THAN, "GREATER_THAN"),
                Arguments.of(SqlStdOperatorTable.LESS_THAN, "LESS_THAN"),
                Arguments.of(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "GREATER_THAN_OR_EQUAL"),
                Arguments.of(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "LESS_THAN_OR_EQUAL"));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("binaryComparisonOperators")
    public void testBinaryComparisonLeftIdRightLit(SqlBinaryOperator op, String name)
    {
        SqlCall call = op.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("int_col", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("42", SqlParserPos.ZERO));
        SqlNode result = visitor.visit(call);
        assertEquals(1, accumulator.size());
        assertTrue(((SqlCall) result).operand(1) instanceof SqlDynamicParam);
    }

    @Test
    public void testBinaryComparisonReversedOperands()
    {
        SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                SqlLiteral.createExactNumeric("99", SqlParserPos.ZERO),
                new SqlIdentifier("int_col", SqlParserPos.ZERO));
        SqlNode result = visitor.visit(call);
        assertEquals(1, accumulator.size());
        assertEquals("int_col", accumulator.get(0).getColumnName());
        assertTrue(((SqlCall) result).operand(0) instanceof SqlDynamicParam);
    }

    @Test
    public void testBinaryComparisonNonSimpleIdFallsThrough()
    {
        visitor.visit(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier(Arrays.asList("s", "int_col"), SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)));
        assertEquals(0, accumulator.size());
    }

    @Test
    public void testBinaryComparisonNoMatchFallsThrough()
    {
        visitor.visit(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("int_col", SqlParserPos.ZERO),
                new SqlIdentifier("bigint_col", SqlParserPos.ZERO)));
        assertEquals(0, accumulator.size());
    }

    // --- handleIn: normal, non-id first, non-simple id, mixed nodes ---

    @Test
    public void testInWithLiterals()
    {
        SqlNodeList vals = new SqlNodeList(SqlParserPos.ZERO);
        vals.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        vals.add(SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO));
        visitor.visit(SqlStdOperatorTable.IN.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("int_col", SqlParserPos.ZERO), vals));
        assertEquals(2, accumulator.size());
    }

    @Test
    public void testInFirstOperandNotIdentifier()
    {
        SqlNodeList vals = new SqlNodeList(SqlParserPos.ZERO);
        vals.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        visitor.visit(SqlStdOperatorTable.IN.createCall(SqlParserPos.ZERO,
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO), vals));
        assertEquals(0, accumulator.size());
    }

    @Test
    public void testInNonSimpleIdentifier()
    {
        SqlNodeList vals = new SqlNodeList(SqlParserPos.ZERO);
        vals.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        visitor.visit(SqlStdOperatorTable.IN.createCall(SqlParserPos.ZERO,
                new SqlIdentifier(Arrays.asList("s", "int_col"), SqlParserPos.ZERO), vals));
        assertEquals(0, accumulator.size());
    }

    @Test
    public void testInMixedLiteralAndNonLiteral()
    {
        SqlNodeList vals = new SqlNodeList(SqlParserPos.ZERO);
        vals.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        vals.add(new SqlIdentifier("bigint_col", SqlParserPos.ZERO));
        vals.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));
        visitor.visit(SqlStdOperatorTable.IN.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("int_col", SqlParserPos.ZERO), vals));
        assertEquals(2, accumulator.size());
    }

    // --- handleBetween: parameterized for bound combinations, plus fallthrough cases ---

    static Stream<Arguments> betweenBoundCombinations()
    {
        SqlIdentifier intId = new SqlIdentifier("int_col", SqlParserPos.ZERO);
        SqlIdentifier otherId = new SqlIdentifier("bigint_col", SqlParserPos.ZERO);
        SqlLiteral litLow = SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO);
        SqlLiteral litHigh = SqlLiteral.createExactNumeric("20", SqlParserPos.ZERO);
        return Stream.of(
                Arguments.of(intId, litLow, litHigh, 2, "both_bounds_literal"),
                Arguments.of(intId, litLow, otherId, 1, "only_lower_literal"),
                Arguments.of(intId, otherId, litHigh, 1, "only_upper_literal"),
                Arguments.of(intId, otherId, new SqlIdentifier("float_col", SqlParserPos.ZERO), 0, "neither_bound_literal"));
    }

    @ParameterizedTest(name = "{4}")
    @MethodSource("betweenBoundCombinations")
    public void testBetweenBoundCombinations(SqlNode identifier, SqlNode lower, SqlNode upper, int expectedAccumulated, String name)
    {
        visitor.visit(SqlStdOperatorTable.BETWEEN.createCall(SqlParserPos.ZERO, identifier, lower, upper));
        assertEquals(expectedAccumulated, accumulator.size());
    }

    @Test
    public void testBetweenFirstOperandNotIdentifier()
    {
        visitor.visit(SqlStdOperatorTable.BETWEEN.createCall(SqlParserPos.ZERO,
                SqlLiteral.createExactNumeric("5", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO)));
        assertEquals(0, accumulator.size());
    }

    @Test
    public void testBetweenNonSimpleIdentifier()
    {
        visitor.visit(SqlStdOperatorTable.BETWEEN.createCall(SqlParserPos.ZERO,
                new SqlIdentifier(Arrays.asList("s", "int_col"), SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO)));
        assertEquals(0, accumulator.size());
    }

    // --- handleLike: basic, with escape, fallthrough cases ---

    @Test
    public void testLikeBasic()
    {
        visitor.visit(SqlStdOperatorTable.LIKE.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("varchar_col", SqlParserPos.ZERO),
                SqlLiteral.createCharString("%test%", SqlParserPos.ZERO)));
        assertEquals(1, accumulator.size());
        assertEquals("%test%", accumulator.get(0).getValue());
    }

    @Test
    public void testLikeWithEscapeCharacter()
    {
        SqlNode result = visitor.visit(SqlStdOperatorTable.LIKE.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("varchar_col", SqlParserPos.ZERO),
                SqlLiteral.createCharString("100\\%%", SqlParserPos.ZERO),
                SqlLiteral.createCharString("\\", SqlParserPos.ZERO)));
        assertEquals(1, accumulator.size());
        assertEquals(3, ((SqlCall) result).operandCount());
    }

    static Stream<Arguments> likeFallthroughCases()
    {
        return Stream.of(
                Arguments.of(
                        SqlLiteral.createCharString("val", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("%t%", SqlParserPos.ZERO),
                        "first_operand_not_identifier"),
                Arguments.of(
                        new SqlIdentifier("varchar_col", SqlParserPos.ZERO),
                        new SqlIdentifier("varchar_col", SqlParserPos.ZERO),
                        "second_operand_not_literal"),
                Arguments.of(
                        new SqlIdentifier(Arrays.asList("s", "varchar_col"), SqlParserPos.ZERO),
                        SqlLiteral.createCharString("%t%", SqlParserPos.ZERO),
                        "non_simple_identifier"));
    }

    @ParameterizedTest(name = "like_fallthrough_{2}")
    @MethodSource("likeFallthroughCases")
    public void testLikeFallthroughCases(SqlNode first, SqlNode second, String name)
    {
        visitor.visit(SqlStdOperatorTable.LIKE.createCall(SqlParserPos.ZERO, first, second));
        assertEquals(0, accumulator.size());
    }

    // --- handleNot: boolean col, non-boolean col, non-identifier, non-simple, non-existent ---

    @Test
    public void testNotOnBooleanColumn()
    {
        // Create a NOT call on a boolean column
        SqlCall notCall = SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("bool_col", SqlParserPos.ZERO));
        
        // Wrap it in a SELECT with WHERE clause to enable inWhereClause context
        SqlNodeList selectList = new SqlNodeList(Arrays.asList(new SqlIdentifier("*", SqlParserPos.ZERO)), SqlParserPos.ZERO);
        SqlIdentifier from = new SqlIdentifier("test_table", SqlParserPos.ZERO);
        
        SqlSelect select = new SqlSelect(
                SqlParserPos.ZERO,
                SqlNodeList.EMPTY, // keywords
                selectList, // selectList
                from, // from
                notCall, // where - this is the NOT call we're testing
                null, // groupBy
                null, // having
                SqlNodeList.EMPTY, // windowDecls
                null, // orderBy
                null, // offset
                null, // fetch
                null); // hints
        
        SqlNode result = visitor.visit(select);
        SqlNode transformedWhere = ((SqlSelect) result).getWhere();
        
        // Not bool_col -> Not bool_col = true
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.BOOLEAN, accumulator.get(0).getType());
        assertEquals(true, accumulator.get(0).getValue());
        assertEquals(SqlKind.NOT, ((SqlCall) transformedWhere).getOperator().getKind());
    }

    static Stream<Arguments> notFallthroughCases()
    {
        return Stream.of(
                Arguments.of(new SqlIdentifier("int_col", SqlParserPos.ZERO), "non_boolean_column"),
                Arguments.of(new SqlIdentifier(Arrays.asList("s", "bool_col"), SqlParserPos.ZERO), "non_simple_identifier"),
                Arguments.of(new SqlIdentifier("no_such_col", SqlParserPos.ZERO), "non_existent_column"));
    }

    @ParameterizedTest(name = "not_fallthrough_{1}")
    @MethodSource("notFallthroughCases")
    public void testNotFallthroughCases(SqlNode operand, String name)
    {
        visitor.visit(SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, operand));
        assertEquals(0, accumulator.size());
    }

    @Test
    public void testNotOnNonIdentifierOperand()
    {
        SqlCall inner = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("int_col", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        visitor.visit(SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, inner));
        // inner EQUALS processed via super.visit recursion
        assertEquals(1, accumulator.size());
    }

    // --- visit(SqlCall): unrecognized kind falls through to super.visit ---

    @Test
    public void testUnrecognizedCallKindFallsThrough()
    {
        SqlCall eq1 = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("int_col", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
        SqlCall eq2 = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier("varchar_col", SqlParserPos.ZERO),
                SqlLiteral.createCharString("x", SqlParserPos.ZERO));
        visitor.visit(SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, eq1, eq2));
        assertEquals(2, accumulator.size());
    }
}
