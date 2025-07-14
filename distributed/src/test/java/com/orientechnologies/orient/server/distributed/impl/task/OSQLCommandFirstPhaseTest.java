package com.orientechnologies.orient.server.distributed.impl.task;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.tx.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionIdPromise;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Test;

public class OSQLCommandFirstPhaseTest {

  @Test
  public void testReadWrite() throws IOException {
    String command = "create cluster bla";
    ONodeId nodeId = new ONodeId("node");
    OTransactionIdPromise first = new OTransactionIdPromise(nodeId, new OTransactionId(10, 20));
    OTransactionIdPromise second = new OTransactionIdPromise(nodeId, new OTransactionId(30, 40));
    OSQLCommandTaskFirstPhase message = new OSQLCommandTaskFirstPhase(command, first, second);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    message.toStream(new DataOutputStream(out));
    OSQLCommandTaskFirstPhase messageRead = new OSQLCommandTaskFirstPhase();
    messageRead.fromStream(new DataInputStream(new ByteArrayInputStream(out.toByteArray())), null);

    assertEquals(message.getQuery(), messageRead.getQuery());
    assertEquals(message.getPreChangeId(), messageRead.getPreChangeId());
    assertEquals(message.getAfterChangeId(), messageRead.getAfterChangeId());
  }
}
