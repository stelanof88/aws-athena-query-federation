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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.calcite.SubstraitSchema;
import io.substrait.plan.PlanProtoConverter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.EXPORT_ITEM_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.EXPORT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.ITEM_COLUMN_NAME;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.LIMIT;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.PLAN_EXPORT_MAX_ROWS;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.SCHEMA_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the Glue StartAsyncQuery plan-export path of {@link DynamoDBRecordHandler}: one StartAsyncQuery carrying a
 * Substrait plan with ExportSchema, no request Schema and no split properties. The plans are generated from SQL with
 * isthmus so the connector parses the same shape of plan a real caller sends.
 *
 * <p>Table {@code orders}: hash key {@code pk} (S), range key {@code sk} (N), plus a KEYS_ONLY GSI on
 * {@code keys_only_key} and an ALL GSI on {@code all_key}.
 */
public class DynamoDBRecordHandlerPlanExportTest
{
    private static final String TABLE = "orders";
    private static final String BUCKET = "spill-bucket";
    private static final String SPILL_KEY = "tenant/request-id/split-id";
    private static final long MAX_BLOCK_SIZE = 16_000_000L;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private BlockAllocator allocator;
    private DynamoDbClient ddbClient;
    private S3Client s3Client;
    private EncryptionKey encryptionKey;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
        ddbClient = mock(DynamoDbClient.class);
        s3Client = mock(S3Client.class);
        encryptionKey = new LocalKeyFactory().create();
        when(ddbClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(DescribeTableResponse.builder().table(ordersTable()).build());
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void hashKeyEquality_runsOneQueryWithoutProjectionAndExportsWholeItems()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(item("a", 1, "shipped"), item("a", 2, "pending")).build());

        RemoteReadRecordsResponse response = run("SELECT * FROM orders WHERE pk = 'a'", Map.of());

        QueryRequest query = captureQueries(1).get(0);
        assertEquals(TABLE, query.tableName());
        assertNull(query.indexName());
        assertEquals("#pk = :hashKeyValue", query.keyConditionExpression());
        assertEquals("a", query.expressionAttributeValues().get(":hashKeyValue").s());
        assertNull("whole-item export must not project", query.projectionExpression());
        verify(ddbClient, never()).scan(any(ScanRequest.class));

        List<Map<String, Object>> rows = readItems(response);
        assertEquals(2, rows.size());
        assertEquals("shipped", rows.get(0).get("status"));
        assertEquals("extra", rows.get(0).get("not_in_any_schema"));
    }

    @Test
    public void hashKeyIn_runsOneQueryPerDistinctValue()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(
                QueryResponse.builder().items(item("a", 1, "x")).build(),
                QueryResponse.builder().items(item("b", 1, "y")).build());

        RemoteReadRecordsResponse response = run("SELECT * FROM orders WHERE pk IN ('a', 'b', 'a')", Map.of());

        List<QueryRequest> queries = captureQueries(2);
        assertEquals("a", queries.get(0).expressionAttributeValues().get(":hashKeyValue").s());
        assertEquals("b", queries.get(1).expressionAttributeValues().get(":hashKeyValue").s());
        assertEquals(2, readItems(response).size());
    }

    @Test
    public void hashAndRangeKey_pushesRangeKeyIntoKeyCondition()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(item("a", 9, "x")).build());

        run("SELECT * FROM orders WHERE pk = 'a' AND sk > 5", Map.of());

        QueryRequest query = captureQueries(1).get(0);
        assertTrue(query.keyConditionExpression(), query.keyConditionExpression().startsWith("#pk = :hashKeyValue AND "));
        assertTrue(query.keyConditionExpression(), query.keyConditionExpression().contains("#sk"));
        assertNull(query.filterExpression());
    }

    @Test
    public void nonKeyPredicate_runsOneUnsegmentedScanWithFilter()
            throws Exception
    {
        when(ddbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(item("a", 1, "shipped")).build());

        RemoteReadRecordsResponse response = run("SELECT * FROM orders WHERE status = 'shipped'", Map.of());

        ScanRequest scan = captureScans(1).get(0);
        assertEquals(TABLE, scan.tableName());
        assertEquals(Integer.valueOf(0), scan.segment());
        assertEquals(Integer.valueOf(1), scan.totalSegments());
        assertTrue(scan.filterExpression(), scan.filterExpression().contains("#status"));
        assertEquals("shipped", scan.expressionAttributeValues().values().iterator().next().s());
        assertNull(scan.projectionExpression());
        assertEquals(1, readItems(response).size());
    }

    @Test
    public void keysOnlyGsiIsNotUsed_soWholeItemsAreReturned()
            throws Exception
    {
        // A Query on the KEYS_ONLY GSI would only return key attributes; the base table must be scanned instead.
        when(ddbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(item("a", 1, "x")).build());

        run("SELECT * FROM orders WHERE keys_only_key = 'k'", Map.of());

        verify(ddbClient, never()).query(any(QueryRequest.class));
        ScanRequest scan = captureScans(1).get(0);
        assertNull(scan.indexName());
        assertTrue(scan.filterExpression(), scan.filterExpression().contains("#keys_only_key"));
    }

    @Test
    public void allProjectionGsiIsUsedForItsHashKey()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(item("a", 1, "x")).build());

        run("SELECT * FROM orders WHERE all_key = 'k'", Map.of());

        QueryRequest query = captureQueries(1).get(0);
        assertEquals("all_gsi", query.indexName());
        assertEquals("#all_key = :hashKeyValue", query.keyConditionExpression());
        assertNull(query.projectionExpression());
    }

    @Test
    public void noPredicate_scansAndFollowsEveryPage()
            throws Exception
    {
        when(ddbClient.scan(any(ScanRequest.class))).thenReturn(
                ScanResponse.builder().items(item("a", 1, "x")).lastEvaluatedKey(Map.of("pk", s("a"))).build(),
                ScanResponse.builder().items(item("b", 1, "y")).build());

        RemoteReadRecordsResponse response = run("SELECT * FROM orders", Map.of());

        List<ScanRequest> scans = captureScans(2);
        assertNull(scans.get(0).filterExpression());
        assertEquals("a", scans.get(1).exclusiveStartKey().get("pk").s());
        assertEquals(2, readItems(response).size());
    }

    @Test
    public void planLimit_isGlobalAcrossQueriesAndStopsEarly()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(
                QueryResponse.builder().items(item("a", 1, "x"), item("a", 2, "x")).build(),
                QueryResponse.builder().items(item("b", 1, "y"), item("b", 2, "y")).build(),
                QueryResponse.builder().items(item("c", 1, "z")).build());

        RemoteReadRecordsResponse response = run("SELECT * FROM orders WHERE pk IN ('a', 'b', 'c') LIMIT 3", Map.of());

        assertEquals(3, readItems(response).size());
        // The third hash key value is never queried once the limit is reached.
        verify(ddbClient, times(2)).query(any(QueryRequest.class));
    }

    @Test
    public void limitOption_capsRowsWhenSmallerThanPlanLimit()
            throws Exception
    {
        when(ddbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder()
                .items(item("a", 1, "x"), item("b", 1, "y"), item("c", 1, "z")).build());

        RemoteReadRecordsResponse response = run("SELECT * FROM orders LIMIT 10", Map.of(LIMIT, "2"));

        assertEquals(2, readItems(response).size());
    }

    @Test
    public void exceedingMaxRows_failsInsteadOfRunningUnbounded()
    {
        when(ddbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder()
                .items(item("a", 1, "x"), item("b", 1, "y"), item("c", 1, "z")).build());
        DynamoDBRecordHandler handler = handler(Map.of(PLAN_EXPORT_MAX_ROWS, "2"));

        AthenaConnectorException e = assertThrows(AthenaConnectorException.class,
                () -> handler.doReadRecords(allocator, request("SELECT * FROM orders", Map.of())));
        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(), e.getErrorDetails().errorCode());
    }

    @Test
    public void invalidLimitOption_isRejectedBeforeAnyDynamoDbCall()
    {
        assertThrows(AthenaConnectorException.class,
                () -> handler(Map.of()).doReadRecords(allocator, request("SELECT * FROM orders", Map.of(LIMIT, "0"))));
        verify(ddbClient, never()).describeTable(any(DescribeTableRequest.class));
    }

    @Test
    public void writesSchemaSidecarWithItemColumn()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        RemoteReadRecordsResponse response = run("SELECT * FROM orders WHERE pk = 'missing'", Map.of());

        assertTrue(response.getRemoteBlocks().isEmpty());
        Map<String, byte[]> written = capturePutObjects();
        AesGcmBlockCrypto crypto = new AesGcmBlockCrypto(allocator);
        Schema sidecar = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(crypto.decrypt(encryptionKey, written.get(SPILL_KEY + SCHEMA_SUFFIX))))));
        assertEquals(EXPORT_ITEM_SCHEMA, sidecar);
    }

    @Test
    public void crossColumnOr_isRejectedRatherThanRunAsAnd()
    {
        AthenaConnectorException e = assertThrows(AthenaConnectorException.class,
                () -> run("SELECT * FROM orders WHERE pk = 'a' OR status = 'x'", Map.of()));
        assertEquals(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString(), e.getErrorDetails().errorCode());
        verify(ddbClient, never()).query(any(QueryRequest.class));
        verify(ddbClient, never()).scan(any(ScanRequest.class));
    }

    @Test
    public void sameColumnOr_isAllowedAndRunsAsOneQueryPerValue()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        run("SELECT * FROM orders WHERE pk = 'a' OR pk = 'b'", Map.of());

        captureQueries(2);
    }

    @Test
    public void orInsideAnd_onOneColumn_isAllowedAndPushedAsFilter()
            throws Exception
    {
        when(ddbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        run("SELECT * FROM orders WHERE pk = 'a' AND (status = 'x' OR status = 'y')", Map.of());

        QueryRequest query = captureQueries(1).get(0);
        assertEquals("#pk = :hashKeyValue", query.keyConditionExpression());
        assertEquals("#status IN (:v0,:v1)", query.filterExpression());
    }

    @Test
    public void unsupportedFunction_failsInsteadOfReturningUnfilteredRows()
    {
        assertThrows(RuntimeException.class, () -> run("SELECT * FROM orders WHERE status LIKE 'sh%'", Map.of()));
        verify(ddbClient, never()).scan(any(ScanRequest.class));
    }

    private RemoteReadRecordsResponse run(String sql, Map<String, String> extraOptions)
            throws Exception
    {
        return (RemoteReadRecordsResponse) handler(Map.of()).doReadRecords(allocator, request(sql, extraOptions));
    }

    private DynamoDBRecordHandler handler(Map<String, String> configOptions)
    {
        return new DynamoDBRecordHandler(ddbClient, s3Client, mock(SecretsManagerClient.class),
                mock(AthenaClient.class), "ddb", configOptions);
    }

    /**
     * Builds the request the way GlueDNAConnectorService StartAsyncQueryHandler does in plan-export mode: request
     * TableName, empty Schema, the Substrait plan, ExportSchema (and optional Limit) as query passthrough arguments,
     * and a Split with no properties.
     */
    private ReadRecordsRequest request(String sql, Map<String, String> extraOptions)
            throws Exception
    {
        Map<String, String> qptArguments = new HashMap<>(extraOptions);
        qptArguments.put(EXPORT_SCHEMA, "true");
        Constraints constraints = new Constraints(Map.of(), List.of(), List.of(), -1L, qptArguments,
                new QueryPlan("", plan(sql)));
        Split split = new Split(new S3SpillLocation(BUCKET, SPILL_KEY, true), encryptionKey, Collections.emptyMap());
        return new ReadRecordsRequest(
                new FederatedIdentity("", "", Map.of(), List.of(), Map.of()),
                "catalog",
                "query-id",
                new TableName("default", TABLE),
                new Schema(Collections.emptyList()),
                split,
                constraints,
                MAX_BLOCK_SIZE,
                0);
    }

    private static String plan(String sql)
            throws Exception
    {
        AbstractTable orders = new AbstractTable()
        {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory)
            {
                return factory.createStructType(Arrays.asList(
                        Pair.of("pk", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("sk", factory.createSqlType(SqlTypeName.BIGINT)),
                        Pair.of("status", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("keys_only_key", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("all_key", factory.createSqlType(SqlTypeName.VARCHAR))));
            }
        };
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        root.add("default", new SubstraitSchema(Map.of(TABLE, orders)));
        CalciteCatalogReader catalog = new CalciteCatalogReader(root, List.of("default"),
                new SqlTypeFactoryImpl(AnsiSqlDialect.DEFAULT.getTypeSystem()),
                CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString()));
        io.substrait.proto.Plan plan = new PlanProtoConverter().toProto(new SqlToSubstrait().convert(sql, catalog));
        return Base64.getEncoder().encodeToString(plan.toByteArray());
    }

    private static TableDescription ordersTable()
    {
        return TableDescription.builder()
                .tableName(TABLE)
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("keys_only_key").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("all_key").attributeType(ScalarAttributeType.S).build())
                .globalSecondaryIndexes(
                        gsi("keys_only_gsi", "keys_only_key", ProjectionType.KEYS_ONLY),
                        gsi("all_gsi", "all_key", ProjectionType.ALL))
                .tableSizeBytes(1_000L)
                .itemCount(10L)
                .build();
    }

    private static GlobalSecondaryIndexDescription gsi(String name, String hashKey, ProjectionType projectionType)
    {
        return GlobalSecondaryIndexDescription.builder()
                .indexName(name)
                .keySchema(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build())
                .projection(Projection.builder().projectionType(projectionType).build())
                .indexStatus(IndexStatus.ACTIVE)
                .build();
    }

    private static Map<String, AttributeValue> item(String pk, int sk, String status)
    {
        return Map.of("pk", s(pk), "sk", AttributeValue.builder().n(String.valueOf(sk)).build(),
                "status", s(status), "not_in_any_schema", s("extra"));
    }

    private static AttributeValue s(String value)
    {
        return AttributeValue.builder().s(value).build();
    }

    private List<QueryRequest> captureQueries(int expected)
    {
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(ddbClient, times(expected)).query(captor.capture());
        return captor.getAllValues();
    }

    private List<ScanRequest> captureScans(int expected)
    {
        ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(ddbClient, times(expected)).scan(captor.capture());
        return captor.getAllValues();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readItems(RemoteReadRecordsResponse response)
            throws Exception
    {
        Map<String, byte[]> written = capturePutObjects();
        AesGcmBlockCrypto crypto = new AesGcmBlockCrypto(allocator);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (SpillLocation location : response.getRemoteBlocks()) {
            try (Block block = crypto.decrypt(encryptionKey, written.get(((S3SpillLocation) location).getKey()), EXPORT_ITEM_SCHEMA)) {
                for (int i = 0; i < block.getRowCount(); i++) {
                    block.getFieldReader(ITEM_COLUMN_NAME).setPosition(i);
                    rows.add(objectMapper.readValue(block.getFieldReader(ITEM_COLUMN_NAME).readText().toString(), Map.class));
                }
            }
        }
        return rows;
    }

    private Map<String, byte[]> capturePutObjects()
            throws Exception
    {
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, org.mockito.Mockito.atLeastOnce()).putObject(requestCaptor.capture(), bodyCaptor.capture());
        Map<String, byte[]> written = new HashMap<>();
        for (int i = 0; i < requestCaptor.getAllValues().size(); i++) {
            try (InputStream in = bodyCaptor.getAllValues().get(i).contentStreamProvider().newStream()) {
                written.put(requestCaptor.getAllValues().get(i).key(), in.readAllBytes());
            }
        }
        return written;
    }
}
