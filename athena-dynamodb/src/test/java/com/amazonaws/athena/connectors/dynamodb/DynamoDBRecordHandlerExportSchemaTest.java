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
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.qpt.DDBQueryPassthrough;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.EXPORT_ITEM_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.EXPORT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.ITEM_COLUMN_NAME;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler.SCHEMA_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests the Glue StartAsyncQuery direct-query path (QUERY + ExportSchema) of {@link DynamoDBRecordHandler}. The
 * request is shaped the way GlueDNAConnectorService builds it: empty table name and schema, a placeholder query plan,
 * and only QUERY and ExportSchema as query passthrough arguments (no schemaFunctionName).
 */
public class DynamoDBRecordHandlerExportSchemaTest
{
    private static final String BUCKET = "spill-bucket";
    private static final String SPILL_KEY = "prefix/account/query-id/split-id";
    private static final String QUERY = "SELECT \"pk\", \"sk\" FROM \"probe_table\"";
    private static final long MAX_BLOCK_SIZE = 16_000_000L;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private BlockAllocator allocator;
    private DynamoDbClient ddbClient;
    private S3Client s3Client;
    private DynamoDBRecordHandler handler;
    private EncryptionKey encryptionKey;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
        ddbClient = mock(DynamoDbClient.class);
        s3Client = mock(S3Client.class);
        handler = new DynamoDBRecordHandler(ddbClient, s3Client, mock(SecretsManagerClient.class),
                mock(AthenaClient.class), "ddb", Collections.emptyMap());
        encryptionKey = new LocalKeyFactory().create();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void doReadRecords_withExportSchema_writesAllPagesAsJsonItemsAndSchemaSidecar()
            throws Exception
    {
        Map<String, AttributeValue> item1 = Map.of(
                "pk", AttributeValue.builder().s("a").build(),
                "sk", AttributeValue.builder().n("1").build());
        // Different attributes than item1: every attribute must survive since each item is exported whole.
        Map<String, AttributeValue> item2 = Map.of(
                "pk", AttributeValue.builder().s("b").build(),
                "tags", AttributeValue.builder().ss("x").build(),
                "nested", AttributeValue.builder().m(Map.of("flag", AttributeValue.builder().bool(true).build())).build());
        Map<String, AttributeValue> item3 = Map.of("pk", AttributeValue.builder().s("c").build());

        when(ddbClient.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(
                ExecuteStatementResponse.builder().items(item1, item2).nextToken("page-2").build(),
                ExecuteStatementResponse.builder().items(item3).build());

        RecordResponse rawResponse = handler.doReadRecords(allocator, exportRequest(encryptionKey, QUERY));

        // Every page is read, following NextToken until it is absent.
        ArgumentCaptor<ExecuteStatementRequest> ddbCaptor = ArgumentCaptor.forClass(ExecuteStatementRequest.class);
        verify(ddbClient, times(2)).executeStatement(ddbCaptor.capture());
        assertEquals(QUERY, ddbCaptor.getAllValues().get(0).statement());
        assertNull(ddbCaptor.getAllValues().get(0).nextToken());
        assertEquals("page-2", ddbCaptor.getAllValues().get(1).nextToken());

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);
        RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse;
        assertEquals(EXPORT_ITEM_SCHEMA, response.getSchema());
        assertEquals(encryptionKey, response.getEncryptionKey());

        Map<String, byte[]> written = capturePutObjects();
        AesGcmBlockCrypto crypto = new AesGcmBlockCrypto(allocator);

        // Schema sidecar: encrypted with the split's key, holds the single JSON item column.
        byte[] sidecar = written.remove(SPILL_KEY + SCHEMA_SUFFIX);
        Schema sidecarSchema = MessageSerializer.deserializeSchema(
                new ReadChannel(Channels.newChannel(new ByteArrayInputStream(crypto.decrypt(encryptionKey, sidecar)))));
        assertEquals(EXPORT_ITEM_SCHEMA, sidecarSchema);

        // Data objects: the advertised spill locations are exactly the remaining objects, and the sidecar is not one.
        assertEquals(written.size(), response.getRemoteBlocks().size());
        List<JsonNode> rows = new ArrayList<>();
        response.getRemoteBlocks().forEach(location -> {
            S3SpillLocation s3Location = (S3SpillLocation) location;
            assertEquals(BUCKET, s3Location.getBucket());
            assertTrue(s3Location.getKey().startsWith(SPILL_KEY + "."));
            try (Block block = crypto.decrypt(encryptionKey, written.get(s3Location.getKey()), EXPORT_ITEM_SCHEMA)) {
                for (int i = 0; i < block.getRowCount(); i++) {
                    block.getFieldReader(ITEM_COLUMN_NAME).setPosition(i);
                    rows.add(objectMapper.readTree(block.getFieldReader(ITEM_COLUMN_NAME).readText().toString()));
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(3, rows.size());
        assertEquals("a", rows.get(0).get("pk").asText());
        assertEquals(1, rows.get(0).get("sk").asInt());
        assertEquals("b", rows.get(1).get("pk").asText());
        assertEquals("x", rows.get(1).get("tags").get(0).asText());
        assertTrue(rows.get(1).get("nested").get("flag").asBoolean());
        assertEquals("c", rows.get(2).get("pk").asText());
    }

    @Test
    public void doReadRecords_withExportSchemaAndNoItems_writesOnlySchemaSidecar()
            throws Exception
    {
        when(ddbClient.executeStatement(any(ExecuteStatementRequest.class)))
                .thenReturn(ExecuteStatementResponse.builder().items(Collections.emptyList()).build());

        RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) handler.doReadRecords(allocator,
                exportRequest(encryptionKey, QUERY));

        assertTrue(response.getRemoteBlocks().isEmpty());
        Map<String, byte[]> written = capturePutObjects();
        assertEquals(1, written.size());
        assertTrue(written.containsKey(SPILL_KEY + SCHEMA_SUFFIX));
    }

    @Test
    public void doReadRecords_withExportSchemaAndNoEncryptionKey_writesPlaintextSidecar()
            throws Exception
    {
        when(ddbClient.executeStatement(any(ExecuteStatementRequest.class)))
                .thenReturn(ExecuteStatementResponse.builder()
                        .items(Map.of("pk", AttributeValue.builder().s("a").build())).build());

        RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) handler.doReadRecords(allocator,
                exportRequest(null, QUERY));

        assertEquals(1, response.getRemoteBlocks().size());
        Schema sidecarSchema = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(capturePutObjects().get(SPILL_KEY + SCHEMA_SUFFIX)))));
        assertEquals(EXPORT_ITEM_SCHEMA, sidecarSchema);
    }

    @Test
    public void doReadRecords_withExportSchemaAndNonSelectStatement_throwsWithoutCallingDynamoDB()
    {
        assertThrows(AthenaConnectorException.class, () -> handler.doReadRecords(allocator,
                exportRequest(encryptionKey, "DELETE FROM \"probe_table\" WHERE \"pk\" = 'a'")));
        verifyNoInteractions(ddbClient);
        verifyNoInteractions(s3Client);
    }

    @Test
    public void doReadRecords_withExportSchemaAndBlankQuery_throwsWithoutCallingDynamoDB()
    {
        assertThrows(AthenaConnectorException.class, () -> handler.doReadRecords(allocator,
                exportRequest(encryptionKey, " ")));
        verifyNoInteractions(ddbClient);
        verifyNoInteractions(s3Client);
    }

    /**
     * Builds the request the way GlueDNAConnectorService StartAsyncQueryHandler does in pass-through mode.
     */
    private ReadRecordsRequest exportRequest(EncryptionKey key, String query)
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(DDBQueryPassthrough.QUERY, query);
        qptArguments.put(EXPORT_SCHEMA, "true");
        Constraints constraints = new Constraints(Map.of(), List.of(), List.of(), -1L, qptArguments,
                new QueryPlan("", ""));
        Split split = new Split(new S3SpillLocation(BUCKET, SPILL_KEY, true), key, Collections.emptyMap());
        return new ReadRecordsRequest(
                new FederatedIdentity("", "", Map.of(), List.of(), Map.of()),
                "catalog",
                "query-id",
                new TableName("", ""),
                new Schema(Collections.emptyList()),
                split,
                constraints,
                MAX_BLOCK_SIZE,
                0);
    }

    private Map<String, byte[]> capturePutObjects()
            throws Exception
    {
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client, org.mockito.Mockito.atLeastOnce()).putObject(requestCaptor.capture(), bodyCaptor.capture());
        Map<String, byte[]> written = new HashMap<>();
        for (int i = 0; i < requestCaptor.getAllValues().size(); i++) {
            PutObjectRequest request = requestCaptor.getAllValues().get(i);
            assertEquals(BUCKET, request.bucket());
            try (InputStream in = bodyCaptor.getAllValues().get(i).contentStreamProvider().newStream()) {
                written.put(request.key(), in.readAllBytes());
            }
        }
        return written;
    }
}
