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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * A {@link SqlShuttle}-based AST visitor that converts inline-literal SQL into a parameterized form
 * by extracting literals and replacing them with {@link SqlDynamicParam} placeholders.
 *
 * <p>Beyond the standard recursive AST traversal provided by {@link SqlShuttle}, this visitor does
 * the following extra work:</p>
 * <ul>
 *   <li><b>Literal extraction &amp; parameterization:</b> Literals in comparisons ({@code =, <>, >, <,
 *       >=, <=}), {@code IN}, {@code BETWEEN}, and {@code LIKE} are replaced with {@code ?} params.
 *       Each extracted value is stored in the {@code accumulator} as a {@link SubstraitTypeAndValue}.</li>
 *   <li><b>Schema-aware type resolution:</b> Literal types are resolved from the column's schema type
 *       (not the literal's own type), ensuring correct Substrait type mapping.</li>
 *   <li><b>Column context tracking:</b> A {@code columnStack} associates literals with their column
 *       by pushing column names when visiting identifiers and peeking when visiting literals.</li>
 *   <li><b>WHERE clause scoping:</b> An {@code inWhereClause} flag is set only during WHERE traversal
 *       to enable boolean-specific handling without affecting other clauses.</li>
 *   <li><b>Implicit boolean expansion:</b> Bare boolean column references in WHERE (e.g.,
 *       {@code WHERE is_active}) are expanded to {@code WHERE is_active = ?} with {@code TRUE}
 *       accumulated as the parameter.</li>
 * </ul>
 */
public class SubstraitAccumulatorVisitor extends SqlShuttle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubstraitAccumulatorVisitor.class);

    /** Extracted literal values with schema-resolved types; indices correspond to {@link SqlDynamicParam} ordinals. */
    private final List<SubstraitTypeAndValue> accumulator;

    /** Row type schema used to resolve column types when parameterizing literals. */
    private final RelDataType schema;

    /** Tracks the current column context so literals can be associated with their column. */
    private final Deque<String> columnStack = new ArrayDeque<>();

    /** Flag scoped to WHERE clause traversal, enables implicit boolean column expansion. */
    private boolean inWhereClause = false;

    public SubstraitAccumulatorVisitor(final List<SubstraitTypeAndValue> accumulator, final RelDataType schema)
    {
        this.accumulator = accumulator;
        this.schema = schema;
    }

    @Override
    public SqlNode visit(SqlCall call)
    {
        // Handle SqlSelect specially to track WHERE clause context
        if (call instanceof SqlSelect) {
            return handleSelect((SqlSelect) call);
        }

        SqlKind kind = call.getOperator().getKind();
        
        // Binary comparisons: col = val, col > val, etc.
        if (isBinaryComparison(kind)) {
            return handleBinaryComparison(call);
        }
        
        // IN clause: col IN (val1, val2, ...)
        if (kind == SqlKind.IN) {
            return handleIn(call);
        }
        
        // BETWEEN: col BETWEEN val1 AND val2
        if (kind == SqlKind.BETWEEN) {
            return handleBetween(call);
        }
        
        // LIKE: col LIKE 'pattern'
        if (kind == SqlKind.LIKE) {
            return handleLike(call);
        }
        
        return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlIdentifier id)
    {
        if (inWhereClause && id.isSimple() && isBooleanColumn(id.getSimple())) {
            // Transform bool_col to bool_col = TRUE
            SqlLiteral trueLiteral = SqlLiteral.createBoolean(true, id.getParserPosition());
            addToAccumulator(id.getSimple(), trueLiteral);
            SqlDynamicParam param =
                    new SqlDynamicParam(accumulator.size() - 1, trueLiteral.getParserPosition());
            return org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS
                    .createCall(id.getParserPosition(), id, param);
        }
        if (id.isSimple()) {
            columnStack.push(id.getSimple());
        }
        SqlNode result = super.visit(id);
        if (id.isSimple() && !columnStack.isEmpty()) {
            columnStack.pop();
        }
        return result;
    }
    
    private boolean isBooleanColumn(String columnName)
    {
        RelDataTypeField field = schema.getField(columnName, true, true);
        return field != null && field.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
    }

    @Override
    public SqlNode visit(SqlLiteral literal)
    {
        // Standalone literals without column context (e.g., LIMIT, ORDER BY position)
        if (columnStack.isEmpty()) {
            LOGGER.debug("Standalone literal {} without column context, skipping", literal.toValue());
            return literal;
        }
        
        String columnName = columnStack.peek();
        addToAccumulator(columnName, literal);
        return new SqlDynamicParam(accumulator.size() - 1, literal.getParserPosition());
    }

    private SqlNode handleBinaryComparison(SqlCall call)
    {
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);
        
        SqlIdentifier identifier = null;
        SqlLiteral literal = null;
        int literalIndex = -1;
        
        if (left instanceof SqlIdentifier && right instanceof SqlLiteral) {
            identifier = (SqlIdentifier) left;
            literal = (SqlLiteral) right;
            literalIndex = 1;
        }
        else if (right instanceof SqlIdentifier && left instanceof SqlLiteral) {
            identifier = (SqlIdentifier) right;
            literal = (SqlLiteral) left;
            literalIndex = 0;
        }
        
        if (identifier != null && literal != null && identifier.isSimple()) {
            String columnName = identifier.getSimple();
            addToAccumulator(columnName, literal);
            
            SqlNode[] operands = call.getOperandList().toArray(new SqlNode[0]);
            operands[literalIndex] = new SqlDynamicParam(accumulator.size() - 1, literal.getParserPosition());
            return call.getOperator().createCall(call.getParserPosition(), operands);
        }
        
        return super.visit(call);
    }

    private SqlNode handleIn(SqlCall call)
    {
        if (!(call.operand(0) instanceof SqlIdentifier)) {
            return super.visit(call);
        }
        
        SqlIdentifier identifier = (SqlIdentifier) call.operand(0);
        if (!identifier.isSimple()) {
            return super.visit(call);
        }
        
        String columnName = identifier.getSimple();
        SqlNode valueListNode = call.operand(1);
        
        if (valueListNode instanceof SqlNodeList) {
            SqlNodeList valueList = (SqlNodeList) valueListNode;
            SqlNodeList newList = new SqlNodeList(valueList.getParserPosition());
            
            for (SqlNode node : valueList) {
                if (node instanceof SqlLiteral) {
                    addToAccumulator(columnName, (SqlLiteral) node);
                    newList.add(new SqlDynamicParam(accumulator.size() - 1, node.getParserPosition()));
                }
                else {
                    newList.add(node);
                }
            }
            
            return call.getOperator().createCall(call.getParserPosition(), identifier, newList);
        }
        
        return super.visit(call);
    }

    private SqlNode handleBetween(SqlCall call)
    {
        if (!(call.operand(0) instanceof SqlIdentifier)) {
            return super.visit(call);
        }
        
        SqlIdentifier identifier = (SqlIdentifier) call.operand(0);
        if (!identifier.isSimple()) {
            return super.visit(call);
        }
        
        String columnName = identifier.getSimple();
        SqlNode lower = call.operand(1);
        SqlNode upper = call.operand(2);
        
        SqlNode newLower = lower;
        SqlNode newUpper = upper;
        
        if (lower instanceof SqlLiteral) {
            addToAccumulator(columnName, (SqlLiteral) lower);
            newLower = new SqlDynamicParam(accumulator.size() - 1, lower.getParserPosition());
        }
        
        if (upper instanceof SqlLiteral) {
            addToAccumulator(columnName, (SqlLiteral) upper);
            newUpper = new SqlDynamicParam(accumulator.size() - 1, upper.getParserPosition());
        }
        
        if (newLower != lower || newUpper != upper) {
            return call.getOperator().createCall(call.getParserPosition(), identifier, newLower, newUpper);
        }
        
        return super.visit(call);
    }

    private SqlNode handleLike(SqlCall call)
    {
        if (!(call.operand(0) instanceof SqlIdentifier) || !(call.operand(1) instanceof SqlLiteral)) {
            return super.visit(call);
        }
        
        SqlIdentifier identifier = (SqlIdentifier) call.operand(0);
        if (!identifier.isSimple()) {
            return super.visit(call);
        }
        
        String columnName = identifier.getSimple();
        SqlLiteral literal = (SqlLiteral) call.operand(1);
        
        addToAccumulator(columnName, literal);
        
        SqlNode newPattern = new SqlDynamicParam(accumulator.size() - 1, literal.getParserPosition());
        
        if (call.operandCount() == 3) {
            return call.getOperator().createCall(call.getParserPosition(), identifier, newPattern, call.operand(2));
        }
        
        return call.getOperator().createCall(call.getParserPosition(), identifier, newPattern);
    }

    private void addToAccumulator(String columnName, SqlLiteral literal)
    {
        RelDataTypeField field = schema.getField(columnName, true, true);
        
        if (field == null) {
            throw new IllegalArgumentException("field " + columnName + " not found in schema with fields: " + schema.getFieldNames());
        }
        
        SqlTypeName typeName = field.getType().getSqlTypeName();
        Object value = literal.getValue() instanceof NlsString 
            ? ((NlsString) literal.getValue()).getValue() 
            : literal.getValue();
        
        accumulator.add(new SubstraitTypeAndValue(typeName, value, columnName));
    }
    
    private SqlNode handleSelect(SqlSelect select)
    {
        // Visit WHERE clause with inWhereClause flag set
        SqlNode where = select.getWhere();
        SqlNode newWhere = null;
        if (where != null) {
            boolean prev = inWhereClause;
            inWhereClause = true;
            newWhere = where.accept(this);
            inWhereClause = prev;
        }

        // Temporarily null out WHERE so super.visit() doesn't re-traverse it
        select.setWhere(null);

        // Let the default SqlShuttle traversal handle all other clauses
        // (SELECT list, FROM, GROUP BY, HAVING, WINDOW, ORDER BY, OFFSET, FETCH, etc.)
        SqlSelect result = (SqlSelect) super.visit(select);

        // Restore the (transformed) WHERE clause
        result.setWhere(newWhere);
        return result;
    }

    private boolean isBinaryComparison(SqlKind kind)
    {
        return kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS
            || kind == SqlKind.GREATER_THAN || kind == SqlKind.LESS_THAN
            || kind == SqlKind.GREATER_THAN_OR_EQUAL || kind == SqlKind.LESS_THAN_OR_EQUAL;
    }
}
