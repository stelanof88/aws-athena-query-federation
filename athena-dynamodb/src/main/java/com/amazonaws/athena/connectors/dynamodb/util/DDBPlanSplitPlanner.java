/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.dynamodb.util;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBIndex;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_NAMES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_VALUES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.NON_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.PARTITION_TYPE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.QUERY_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SCAN_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.TABLE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils.buildFilterPredicatesFromPlan;

/**
 * Turns a Substrait plan into the split properties the record handler executes, without a GetTableLayout /
 * GetSplits round trip. Used by the Glue StartAsyncQuery plan-export path (one StartAsyncQuery carrying the plan,
 * no split properties), where the caller expects the whole plan to run as a single read.
 *
 * <p>The properties match what {@code DynamoDBMetadataHandler} puts on a split for the same plan (table, index,
 * hash key value, range key filter, non-key filter, expression names and values), so the existing Query/Scan
 * request builders run them unchanged. Two deliberate differences:
 * <ul>
 *     <li>Index choice is restricted to indexes that return every attribute, because the export returns whole
 *     items (see {@link DDBPredicateUtils#getBestIndexForPlanReturningAllAttributes}).</li>
 *     <li>A plan without a usable hash key predicate becomes one unsegmented Scan instead of N parallel segments,
 *     since everything runs inside a single read.</li>
 * </ul>
 */
public final class DDBPlanSplitPlanner
{
    // A whole-item export carries no request Schema, so there is no Glue column metadata (name mapping, datetime
    // formats, non-comparable types) to honour; plan predicates are pushed down as-is.
    private static final DDBRecordMetadata NO_GLUE_METADATA = new DDBRecordMetadata(new Schema(Collections.emptyList()));
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DDBPlanSplitPlanner()
    {
    }

    /**
     * @param table the resolved DynamoDB table
     * @param plan the Substrait plan to execute
     * @return one property map per DynamoDB request to run: one per distinct hash key value for a Query, or a
     * single entry for a Scan. Never empty.
     */
    public static List<Map<String, String>> planSplits(DynamoDBTable table, Plan plan)
    {
        rejectCrossColumnOr(plan);
        Map<String, List<ColumnPredicate>> filterPredicates = buildFilterPredicatesFromPlan(plan);
        DynamoDBIndex index = DDBPredicateUtils.getBestIndexForPlanReturningAllAttributes(table, filterPredicates);
        String hashKeyName = index.getHashKey();
        List<ColumnPredicate> hashKeyPredicates = DDBPredicateUtils.getHashKeyAttributeValues(filterPredicates.get(hashKeyName));

        Map<String, String> metadata = new HashMap<>();
        metadata.put(TABLE_METADATA, table.getName());

        Set<String> columnsToIgnore = new HashSet<>();
        List<AttributeValue> valueAccumulator = new ArrayList<>();
        IncrementingValueNameProducer valueNameProducer = new IncrementingValueNameProducer();

        boolean isQuery = !hashKeyPredicates.isEmpty();
        if (isQuery) {
            metadata.put(HASH_KEY_NAME_METADATA, hashKeyName);
            metadata.put(PARTITION_TYPE_METADATA, QUERY_PARTITION_TYPE);
            columnsToIgnore.add(hashKeyName);
            if (!table.getName().equals(index.getName())) {
                metadata.put(INDEX_METADATA, index.getName());
            }
            Optional<String> rangeKey = index.getRangeKey();
            if (rangeKey.isPresent() && filterPredicates.containsKey(rangeKey.get())) {
                String rangeKeyFilter = DDBPredicateUtils.generateSingleColumnFilter(rangeKey.get(),
                        filterPredicates.get(rangeKey.get()), valueAccumulator, valueNameProducer, NO_GLUE_METADATA, true);
                if (rangeKeyFilter != null) {
                    metadata.put(RANGE_KEY_NAME_METADATA, rangeKey.get());
                    metadata.put(RANGE_KEY_FILTER_METADATA, rangeKeyFilter);
                    columnsToIgnore.add(rangeKey.get());
                }
            }
        }
        else {
            metadata.put(PARTITION_TYPE_METADATA, SCAN_PARTITION_TYPE);
        }

        String nonKeyFilter = DDBPredicateUtils.generateFilterExpressionForPlan(columnsToIgnore, filterPredicates,
                valueAccumulator, valueNameProducer, NO_GLUE_METADATA);
        if (nonKeyFilter != null) {
            metadata.put(NON_KEY_FILTER_METADATA, nonKeyFilter);
        }
        if (!valueAccumulator.isEmpty()) {
            putExpressionMappings(metadata, filterPredicates.keySet(), valueAccumulator);
        }

        List<Map<String, String>> splits = new ArrayList<>();
        if (isQuery) {
            // One DynamoDB Query per distinct hash key value (a Query takes exactly one). Duplicates in an IN list
            // would otherwise return the same items twice.
            Set<String> hashKeyValues = new LinkedHashSet<>();
            for (ColumnPredicate predicate : hashKeyPredicates) {
                Object value = DDBTypeUtils.convertArrowTypeIfNecessary(hashKeyName, predicate.getValue());
                hashKeyValues.add(DDBTypeUtils.attributeToJson(DDBTypeUtils.toAttributeValue(value), hashKeyName));
            }
            for (String hashKeyValue : hashKeyValues) {
                Map<String, String> split = new HashMap<>(metadata);
                split.put(hashKeyName, hashKeyValue);
                splits.add(split);
            }
        }
        else {
            Map<String, String> split = new HashMap<>(metadata);
            split.put(SEGMENT_ID_PROPERTY, "0");
            split.put(SEGMENT_COUNT_METADATA, "1");
            splits.add(split);
        }
        return splits;
    }

    /*
    The plan's filter is flattened into per-column predicates (buildFilterPredicatesFromPlan), which treats every
    predicate as AND-ed. An OR that spans columns, e.g. pk = 'a' OR status = 'x', would therefore run as
    pk = 'a' AND status = 'x' and silently drop rows. On the Athena split path the engine re-applies the filter to
    the connector's output; nothing does here, so such plans are rejected. An OR over a single column (pk = 'a' OR
    pk = 'b', i.e. IN) is safe: predicates on one column are combined with OR.
     */
    private static void rejectCrossColumnOr(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return;
        }
        SubstraitRelModel relModel = SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
        if (relModel.getFilterRel() == null) {
            return;
        }
        LogicalExpression filter = SubstraitFunctionParser.parseLogicalExpression(plan.getExtensionsList(),
                relModel.getFilterRel().getCondition(), SubstraitMetadataParser.getTableColumns(relModel));
        checkOrNodes(filter);
    }

    private static void checkOrNodes(LogicalExpression expression)
    {
        if (expression == null || expression.isLeaf()) {
            return;
        }
        if (expression.getOperator() == SubstraitOperator.OR) {
            Set<String> columns = new HashSet<>();
            collectColumns(expression, columns);
            if (columns.size() > 1) {
                throw new AthenaConnectorException("OR across different columns " + columns + " is not supported for "
                        + "DynamoDB query plans; split it into separate queries.",
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
            }
        }
        for (LogicalExpression child : expression.getChildren()) {
            checkOrNodes(child);
        }
    }

    private static void collectColumns(LogicalExpression expression, Set<String> columns)
    {
        if (expression == null) {
            return;
        }
        if (expression.isLeaf()) {
            if (expression.getLeafPredicate() != null) {
                columns.add(expression.getLeafPredicate().getColumn());
            }
            return;
        }
        for (LogicalExpression child : expression.getChildren()) {
            collectColumns(child, columns);
        }
    }

    /*
    Mirrors DynamoDBMetadataHandler#precomputeAdditionalMetadata: alias every filtered column and re-generate the
    value placeholder names in the same order the accumulator was filled.
     */
    private static void putExpressionMappings(Map<String, String> metadata, Set<String> filterColumns,
                                              List<AttributeValue> valueAccumulator)
    {
        Map<String, String> aliasedColumns = new HashMap<>();
        for (String column : filterColumns) {
            aliasedColumns.put(DDBPredicateUtils.aliasColumn(column), column);
        }
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        IncrementingValueNameProducer valueNames = new IncrementingValueNameProducer();
        for (AttributeValue value : valueAccumulator) {
            expressionValues.put(valueNames.getNext(), value);
        }
        try {
            metadata.put(EXPRESSION_NAMES_METADATA, MAPPER.writeValueAsString(aliasedColumns));
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize DynamoDB expression attribute names", e);
        }
        metadata.put(EXPRESSION_VALUES_METADATA, EnhancedDocument.fromAttributeValueMap(expressionValues).toJson());
    }
}
