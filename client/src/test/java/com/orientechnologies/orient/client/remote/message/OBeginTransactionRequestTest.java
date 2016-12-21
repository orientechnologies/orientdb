package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
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

public class OBeginTransactionRequestTest {

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
    assertTrue(readRequest.isUsingLong());
    assertEquals(readRequest.getOperations().size(), 1);
    assertEquals(readRequest.getTxId(), 0);
    assertEquals(readRequest.getChanges().size(), 1);
    assertEquals(readRequest.getChanges().get(0).getName(), "some");
    OTransactionIndexChanges val = readRequest.getChanges().get(0).getKeyChanges();
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
