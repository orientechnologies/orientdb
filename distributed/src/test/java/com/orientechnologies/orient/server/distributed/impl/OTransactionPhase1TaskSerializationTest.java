package com.orientechnologies.orient.server.distributed.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class OTransactionPhase1TaskSerializationTest {
  @Test
  public void testUniqueIndexKeysSerialization() throws IOException {
    OTransactionId txId = new OTransactionId(Optional.empty(), 0, 1);
    ByteArrayOutputStream outStream1 = new ByteArrayOutputStream();
    DataOutputStream out1 = new DataOutputStream(outStream1);

    txId.write(out1);
    out1.writeInt(0);
    new OLogSequenceNumber(-1, -1).toStream(out1);
    out1.writeInt(2);
    out1.writeUTF("idx1");
    out1.writeUTF("k1");
    out1.writeUTF("idx2");
    out1.writeUTF("k2");

    OTransactionPhase1Task task = new OTransactionPhase1Task();
    task.fromStream(new DataInputStream(new ByteArrayInputStream(outStream1.toByteArray())), null);

    Collection<OPair<String, String>> expectedKeys =
        Arrays.asList(new OPair<>("idx1", "k1"), new OPair<>("idx2", "k2"));

    Set<OPair<String, String>> uniqueKeys = task.getUniqueKeys();
    assertEquals(uniqueKeys.size(), 2);
    assertTrue(uniqueKeys.containsAll(expectedKeys));

    ByteArrayOutputStream outStream2 = new ByteArrayOutputStream();
    task.toStream(new DataOutputStream(outStream2));

    OTransactionPhase1Task task2 = new OTransactionPhase1Task();
    task2.fromStream(new DataInputStream(new ByteArrayInputStream(outStream2.toByteArray())), null);
    Set<OPair<String, String>> uniqueKeys2 = task2.getUniqueKeys();
    assertEquals(uniqueKeys2.size(), 2);
    assertTrue(uniqueKeys2.containsAll(expectedKeys));
  }
}
