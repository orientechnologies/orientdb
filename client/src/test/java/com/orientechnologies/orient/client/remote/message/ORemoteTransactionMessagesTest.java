package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.Ignore;
import org.junit.Test;

public class ORemoteTransactionMessagesTest {

  @Test
  public void testBeginTransactionEmptyWriteRead() throws IOException {
    MockChannel channel = new MockChannel();
    OBeginTransactionRequest request = new OBeginTransactionRequest(0, false, true, null, null);
    request.write(channel, null);
    channel.close();
    OBeginTransactionRequest readRequest = new OBeginTransactionRequest();
    readRequest.read(channel, 0, null);
    assertFalse(readRequest.isHasContent());
  }

  @Test
  public void testBeginTransactionWriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new ODocument(), ORecordOperation.CREATED));
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new ORecordId(1, 2), OPERATION.PUT);
    keyChange.add(new ORecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OBeginTransactionRequest request =
        new OBeginTransactionRequest(0, true, true, operations, changes);
    request.write(channel, null);

    channel.close();

    OBeginTransactionRequest readRequest = new OBeginTransactionRequest();
    readRequest.read(channel, 0, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(readRequest.getOperations().size(), 1);
    assertEquals(readRequest.getTxId(), 0);
    assertEquals(readRequest.getIndexChanges().size(), 1);
    assertEquals(readRequest.getIndexChanges().get(0).getName(), "some");
    OTransactionIndexChanges val = readRequest.getIndexChanges().get(0).getKeyChanges();
    assertEquals(val.cleared, false);
    assertEquals(val.changesPerKey.size(), 1);
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals(entryChange.key, "key");
    assertEquals(entryChange.size(), 2);
    assertEquals(entryChange.getEntriesAsList().get(0).getValue(), new ORecordId(1, 2));
    assertEquals(entryChange.getEntriesAsList().get(0).getOperation(), OPERATION.PUT);
    assertEquals(entryChange.getEntriesAsList().get(1).getValue(), new ORecordId(2, 2));
    assertEquals(entryChange.getEntriesAsList().get(1).getOperation(), OPERATION.REMOVE);
  }

  @Test
  public void testFullCommitTransactionWriteRead() throws IOException {
    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new ODocument(), ORecordOperation.CREATED));
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new ORecordId(1, 2), OPERATION.PUT);
    keyChange.add(new ORecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OCommit37Request request = new OCommit37Request(0, true, true, operations, changes);
    request.write(channel, null);

    channel.close();

    OCommit37Request readRequest = new OCommit37Request();
    readRequest.read(channel, 0, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertEquals(readRequest.getOperations().size(), 1);
    assertEquals(readRequest.getTxId(), 0);
    assertEquals(readRequest.getIndexChanges().size(), 1);
    assertEquals(readRequest.getIndexChanges().get(0).getName(), "some");
    OTransactionIndexChanges val = readRequest.getIndexChanges().get(0).getKeyChanges();
    assertEquals(val.cleared, false);
    assertEquals(val.changesPerKey.size(), 1);
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals(entryChange.key, "key");
    assertEquals(entryChange.size(), 2);
    assertEquals(entryChange.getEntriesAsList().get(0).getValue(), new ORecordId(1, 2));
    assertEquals(entryChange.getEntriesAsList().get(0).getOperation(), OPERATION.PUT);
    assertEquals(entryChange.getEntriesAsList().get(1).getValue(), new ORecordId(2, 2));
    assertEquals(entryChange.getEntriesAsList().get(1).getOperation(), OPERATION.REMOVE);
  }

  @Test
  public void testCommitResponseTransactionWriteRead() throws IOException {

    MockChannel channel = new MockChannel();
    List<OCommit37Response.OCreatedRecordResponse> creates = new ArrayList<>();
    creates.add(
        new OCommit37Response.OCreatedRecordResponse(
            new ORecordId(1, 2), new ORecordId(-1, -2), 10));
    creates.add(
        new OCommit37Response.OCreatedRecordResponse(
            new ORecordId(1, 3), new ORecordId(-1, -3), 20));

    List<OCommit37Response.OUpdatedRecordResponse> updates = new ArrayList<>();
    updates.add(new OCommit37Response.OUpdatedRecordResponse(new ORecordId(10, 20), 3));
    updates.add(new OCommit37Response.OUpdatedRecordResponse(new ORecordId(10, 21), 4));

    List<OCommit37Response.ODeletedRecordResponse> deletes = new ArrayList<>();
    deletes.add(new OCommit37Response.ODeletedRecordResponse(new ORecordId(10, 50)));
    deletes.add(new OCommit37Response.ODeletedRecordResponse(new ORecordId(10, 51)));

    Map<UUID, OBonsaiCollectionPointer> changes = new HashMap<>();
    UUID val = UUID.randomUUID();
    changes.put(val, new OBonsaiCollectionPointer(10, new OBonsaiBucketPointer(30, 40)));
    OCommit37Response response = new OCommit37Response(creates, updates, deletes, changes);
    response.write(channel, 0, null);
    channel.close();

    OCommit37Response readResponse = new OCommit37Response();
    readResponse.read(channel, null);
    assertEquals(readResponse.getCreated().size(), 2);
    assertEquals(readResponse.getCreated().get(0).getCurrentRid(), new ORecordId(1, 2));
    assertEquals(readResponse.getCreated().get(0).getCreatedRid(), new ORecordId(-1, -2));
    assertEquals(readResponse.getCreated().get(0).getVersion(), 10);

    assertEquals(readResponse.getCreated().get(1).getCurrentRid(), new ORecordId(1, 3));
    assertEquals(readResponse.getCreated().get(1).getCreatedRid(), new ORecordId(-1, -3));
    assertEquals(readResponse.getCreated().get(1).getVersion(), 20);

    assertEquals(readResponse.getUpdated().size(), 2);
    assertEquals(readResponse.getUpdated().get(0).getRid(), new ORecordId(10, 20));
    assertEquals(readResponse.getUpdated().get(0).getVersion(), 3);

    assertEquals(readResponse.getUpdated().get(1).getRid(), new ORecordId(10, 21));
    assertEquals(readResponse.getUpdated().get(1).getVersion(), 4);

    assertEquals(readResponse.getDeleted().size(), 2);
    assertEquals(readResponse.getDeleted().get(0).getRid(), new ORecordId(10, 50));
    assertEquals(readResponse.getDeleted().get(1).getRid(), new ORecordId(10, 51));

    assertEquals(readResponse.getCollectionChanges().size(), 1);
    assertNotNull(readResponse.getCollectionChanges().get(val));
    assertEquals(readResponse.getCollectionChanges().get(val).getFileId(), 10);
    assertEquals(readResponse.getCollectionChanges().get(val).getRootPointer().getPageIndex(), 30);
    assertEquals(readResponse.getCollectionChanges().get(val).getRootPointer().getPageOffset(), 40);
  }

  @Test
  public void testEmptyCommitTransactionWriteRead() throws IOException {

    MockChannel channel = new MockChannel();
    OCommit37Request request = new OCommit37Request(0, false, true, null, null);
    request.write(channel, null);

    channel.close();

    OCommit37Request readRequest = new OCommit37Request();
    readRequest.read(channel, 0, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertTrue(readRequest.isUsingLog());
    assertNull(readRequest.getOperations());
    assertEquals(readRequest.getTxId(), 0);
    assertNull(readRequest.getIndexChanges());
  }

  @Test
  public void testTransactionFetchResponseWriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new ODocument(), ORecordOperation.CREATED));
    operations.add(
        new ORecordOperation(new ODocument(new ORecordId(10, 2)), ORecordOperation.UPDATED));
    operations.add(
        new ORecordOperation(new ODocument(new ORecordId(10, 1)), ORecordOperation.DELETED));
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new ORecordId(1, 2), OPERATION.PUT);
    keyChange.add(new ORecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OFetchTransactionResponse response =
        new OFetchTransactionResponse(10, operations, changes, new HashMap<>());
    response.write(channel, 0, ORecordSerializerNetworkV37.INSTANCE);

    channel.close();

    OFetchTransactionResponse readResponse = new OFetchTransactionResponse();
    readResponse.read(channel, null);

    assertEquals(readResponse.getOperations().size(), 3);
    assertEquals(readResponse.getOperations().get(0).getType(), ORecordOperation.CREATED);
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(readResponse.getOperations().get(1).getType(), ORecordOperation.UPDATED);
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(readResponse.getOperations().get(2).getType(), ORecordOperation.DELETED);
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(readResponse.getTxId(), 10);
    assertEquals(readResponse.getIndexChanges().size(), 1);
    assertEquals(readResponse.getIndexChanges().get(0).getName(), "some");
    OTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertEquals(val.cleared, false);
    assertEquals(val.changesPerKey.size(), 1);
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals(entryChange.key, "key");
    assertEquals(entryChange.size(), 2);
    assertEquals(entryChange.getEntriesAsList().get(0).getValue(), new ORecordId(1, 2));
    assertEquals(entryChange.getEntriesAsList().get(0).getOperation(), OPERATION.PUT);
    assertEquals(entryChange.getEntriesAsList().get(1).getValue(), new ORecordId(2, 2));
    assertEquals(entryChange.getEntriesAsList().get(1).getOperation(), OPERATION.REMOVE);
  }

  @Test
  @Ignore
  public void testTransactionFetchResponse38WriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new ODocument(), ORecordOperation.CREATED));
    operations.add(
        new ORecordOperation(new ODocument(new ORecordId(10, 2)), ORecordOperation.UPDATED));
    operations.add(
        new ORecordOperation(new ODocument(new ORecordId(10, 1)), ORecordOperation.DELETED));
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = false;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new ORecordId(1, 2), OPERATION.PUT);
    keyChange.add(new ORecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OFetchTransaction38Response response =
        new OFetchTransaction38Response(10, operations, changes, new HashMap<>(), null);
    response.write(channel, 0, ORecordSerializerNetworkV37.INSTANCE);

    channel.close();

    OFetchTransaction38Response readResponse = new OFetchTransaction38Response();
    readResponse.read(channel, null);

    assertEquals(readResponse.getOperations().size(), 3);
    assertEquals(readResponse.getOperations().get(0).getType(), ORecordOperation.CREATED);
    assertNotNull(readResponse.getOperations().get(0).getRecord());
    assertEquals(readResponse.getOperations().get(1).getType(), ORecordOperation.UPDATED);
    assertNotNull(readResponse.getOperations().get(1).getRecord());
    assertEquals(readResponse.getOperations().get(2).getType(), ORecordOperation.DELETED);
    assertNotNull(readResponse.getOperations().get(2).getRecord());
    assertEquals(readResponse.getTxId(), 10);
    assertEquals(readResponse.getIndexChanges().size(), 1);
    assertEquals(readResponse.getIndexChanges().get(0).getName(), "some");
    OTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertEquals(val.cleared, false);
    assertEquals(val.changesPerKey.size(), 1);
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals(entryChange.key, "key");
    assertEquals(entryChange.size(), 2);
    assertEquals(entryChange.getEntriesAsList().get(0).getValue(), new ORecordId(1, 2));
    assertEquals(entryChange.getEntriesAsList().get(0).getOperation(), OPERATION.PUT);
    assertEquals(entryChange.getEntriesAsList().get(1).getValue(), new ORecordId(2, 2));
    assertEquals(entryChange.getEntriesAsList().get(1).getOperation(), OPERATION.REMOVE);
  }

  @Test
  public void testTransactionClearIndexFetchResponseWriteRead() throws IOException {

    List<ORecordOperation> operations = new ArrayList<>();
    Map<String, OTransactionIndexChanges> changes = new HashMap<>();
    OTransactionIndexChanges change = new OTransactionIndexChanges();
    change.cleared = true;
    change.changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
    OTransactionIndexChangesPerKey keyChange = new OTransactionIndexChangesPerKey("key");
    keyChange.add(new ORecordId(1, 2), OPERATION.PUT);
    keyChange.add(new ORecordId(2, 2), OPERATION.REMOVE);
    change.changesPerKey.put(keyChange.key, keyChange);
    changes.put("some", change);

    MockChannel channel = new MockChannel();
    OFetchTransactionResponse response =
        new OFetchTransactionResponse(10, operations, changes, new HashMap<>());
    response.write(channel, 0, ORecordSerializerNetworkV37.INSTANCE);

    channel.close();

    OFetchTransactionResponse readResponse =
        new OFetchTransactionResponse(10, operations, changes, new HashMap<>());
    readResponse.read(channel, null);

    assertEquals(readResponse.getTxId(), 10);
    assertEquals(readResponse.getIndexChanges().size(), 1);
    assertEquals(readResponse.getIndexChanges().get(0).getName(), "some");
    OTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertEquals(val.cleared, true);
    assertEquals(val.changesPerKey.size(), 1);
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals(entryChange.key, "key");
    assertEquals(entryChange.size(), 2);
    assertEquals(entryChange.getEntriesAsList().get(0).getValue(), new ORecordId(1, 2));
    assertEquals(entryChange.getEntriesAsList().get(0).getOperation(), OPERATION.PUT);
    assertEquals(entryChange.getEntriesAsList().get(1).getValue(), new ORecordId(2, 2));
    assertEquals(entryChange.getEntriesAsList().get(1).getOperation(), OPERATION.REMOVE);
  }
}
