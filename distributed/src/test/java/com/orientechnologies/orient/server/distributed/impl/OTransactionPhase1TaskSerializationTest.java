package com.orientechnologies.orient.server.distributed.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.Test;

public class OTransactionPhase1TaskSerializationTest {
  @Test
  public void testUniqueIndexKeysSerialization() throws IOException {
    OTransactionId txId = new OTransactionId(Optional.empty(), 0, 1);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outStream);

    txId.write(out);
    out.writeInt(0);
    OTransactionUniqueKey keyChange1 = new OTransactionUniqueKey("idx1", null, 0);
    OTransactionUniqueKey keyChange2 = new OTransactionUniqueKey("idx2", "k2", 0);
    OTransactionUniqueKey keyChange3 = new OTransactionUniqueKey("idx3", 5, 0);

    OTransactionUniqueKey keyChange4 =
        new OTransactionUniqueKey("idx4", new OCompositeKey("user1", 123), 0);
    SortedSet<OTransactionUniqueKey> actualUniqueKeys =
        new TreeSet<OTransactionUniqueKey>() {
          {
            add(keyChange1);
            add(keyChange2);
            add(keyChange3);
            add(keyChange4);
          }
        };
    OTransactionPhase1Task.writeTxUniqueIndexKeys(
        actualUniqueKeys, ORecordSerializerNetworkDistributed.INSTANCE, out);

    OTransactionPhase1Task task = new OTransactionPhase1Task();
    task.fromStream(new DataInputStream(new ByteArrayInputStream(outStream.toByteArray())), null);

    SortedSet<OTransactionUniqueKey> receivedUniqueKeys = task.getUniqueKeys();
    assertEquals(receivedUniqueKeys.size(), actualUniqueKeys.size());
    Iterator<OTransactionUniqueKey> it1 = receivedUniqueKeys.iterator();
    Iterator<OTransactionUniqueKey> it2 = actualUniqueKeys.iterator();

    while (it1.hasNext() && it2.hasNext()) {
      assertEquals(0, it1.next().compareTo(it2.next()));
    }
  }
}
