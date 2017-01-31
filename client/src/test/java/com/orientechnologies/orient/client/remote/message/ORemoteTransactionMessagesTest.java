package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

public class ORemoteTransactionMessagesTest {

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
    OBeginTransactionRequest request = new OBeginTransactionRequest(0, true, operations, changes);
    request.write(channel, null);

    channel.close();

    OBeginTransactionRequest readRequest = new OBeginTransactionRequest();
    readRequest.read(channel, 0, ORecordSerializerNetwork.INSTANCE);
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
    assertEquals(entryChange.entries.size(), 2);
    assertEquals(entryChange.entries.get(0).value, new ORecordId(1, 2));
    assertEquals(entryChange.entries.get(0).operation, OPERATION.PUT);
    assertEquals(entryChange.entries.get(1).value, new ORecordId(2, 2));
    assertEquals(entryChange.entries.get(1).operation, OPERATION.REMOVE);
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
    readRequest.read(channel, 0, ORecordSerializerNetwork.INSTANCE);
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
    assertEquals(entryChange.entries.size(), 2);
    assertEquals(entryChange.entries.get(0).value, new ORecordId(1, 2));
    assertEquals(entryChange.entries.get(0).operation, OPERATION.PUT);
    assertEquals(entryChange.entries.get(1).value, new ORecordId(2, 2));
    assertEquals(entryChange.entries.get(1).operation, OPERATION.REMOVE);
  }

  @Test
  public void testEmptyCommitTransactionWriteRead() throws IOException {

    MockChannel channel = new MockChannel();
    OCommit37Request request = new OCommit37Request(0, false, true, null, null);
    request.write(channel, null);

    channel.close();

    OCommit37Request readRequest = new OCommit37Request();
    readRequest.read(channel, 0, ORecordSerializerNetwork.INSTANCE);
    assertTrue(readRequest.isUsingLog());
    assertNull(readRequest.getOperations());
    assertEquals(readRequest.getTxId(), 0);
    assertNull(readRequest.getIndexChanges());
  }

  @Test
  public void testTransactionFetchResponseWriteRead() throws IOException {

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
    OFetchTransactionResponse response = new OFetchTransactionResponse(10, operations, changes);
    response.write(channel, 0, ORecordSerializerNetwork.INSTANCE);

    channel.close();

    OFetchTransactionResponse readResponse = new OFetchTransactionResponse(10, operations, changes);
    readResponse.read(channel, null);

    assertEquals(readResponse.getOperations().size(), 1);
    assertEquals(readResponse.getTxId(), 10);
    assertEquals(readResponse.getIndexChanges().size(), 1);
    assertEquals(readResponse.getIndexChanges().get(0).getName(), "some");
    OTransactionIndexChanges val = readResponse.getIndexChanges().get(0).getKeyChanges();
    assertEquals(val.cleared, false);
    assertEquals(val.changesPerKey.size(), 1);
    OTransactionIndexChangesPerKey entryChange = val.changesPerKey.firstEntry().getValue();
    assertEquals(entryChange.key, "key");
    assertEquals(entryChange.entries.size(), 2);
    assertEquals(entryChange.entries.get(0).value, new ORecordId(1, 2));
    assertEquals(entryChange.entries.get(0).operation, OPERATION.PUT);
    assertEquals(entryChange.entries.get(1).value, new ORecordId(2, 2));
    assertEquals(entryChange.entries.get(1).operation, OPERATION.REMOVE);

  }



}
