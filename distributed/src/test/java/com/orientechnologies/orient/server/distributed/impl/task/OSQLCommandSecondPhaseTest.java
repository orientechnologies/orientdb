package com.orientechnologies.orient.server.distributed.impl.task;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Test;

public class OSQLCommandSecondPhaseTest {

  @Test
  public void testReadWrite() throws IOException {
    ODistributedRequestId id = new ODistributedRequestId();
    OSQLCommandTaskSecondPhase message = new OSQLCommandTaskSecondPhase(id, true);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    message.toStream(new DataOutputStream(out));
    OSQLCommandTaskSecondPhase messageRead = new OSQLCommandTaskSecondPhase();
    messageRead.fromStream(new DataInputStream(new ByteArrayInputStream(out.toByteArray())), null);

    assertEquals(message.getConfirmSentRequest(), messageRead.getConfirmSentRequest());
    assertEquals(message.isApply(), messageRead.isApply());
  }
}
