package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.remote.message.OMessageHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class OTransactionPhase1TaskSerializationTest {
  @Test
  public void testUniqueIndexKeysSerialization() throws IOException {
    OTransactionId txId = new OTransactionId(Optional.empty(), 0, 1);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outStream);

    // OPair.compareTo only compares keys
    Comparator<OPair<String, Object>> kvComp = (o1, o2) -> {
      int k = o1.key.compareTo(o2.key);
      if (k != 0) {
        return k;
      }
      if (o1.value == null) {
        return o2.value == null ? 0 : 1;
      }
      return o1.value.equals(o2.value) ? 0 : 1;
    };

    txId.write(out);
    out.writeInt(0);
    new OLogSequenceNumber(-1, -1).toStream(out);
    OPair<String, Object> keyChange1 = new OPair<>("idx1", null);
    OPair<String, Object> keyChange2 = new OPair<>("idx2", "k2");
    OPair<String, Object> keyChange3 = new OPair<>("idx3", 5);
    SortedSet<OPair<String, Object>> actualUniqueKeys = new TreeSet<OPair<String, Object>>() {{
      add(keyChange1);
      add(keyChange2);
      add(keyChange3);
    }};
    OMessageHelper.writeTxUniqueIndexKeys(actualUniqueKeys, ORecordSerializerNetworkDistributed.INSTANCE, out);

    OTransactionPhase1Task task = new OTransactionPhase1Task();
    task.fromStream(new DataInputStream(new ByteArrayInputStream(outStream.toByteArray())), null);

    SortedSet<OPair<String, Object>> receivedUniqueKeys = task.getUniqueKeys();
    assertEquals(receivedUniqueKeys.size(), actualUniqueKeys.size());
    Iterator<OPair<String, Object>> it1 = receivedUniqueKeys.iterator();
    Iterator<OPair<String, Object>> it2 = actualUniqueKeys.iterator();

    while (it1.hasNext() && it2.hasNext()) {
      assertEquals(0, kvComp.compare(it1.next(), it2.next()));
    }
  }
}
