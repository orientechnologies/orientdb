package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class AbstractRemoteTaskTest {

  protected void serializeDeserialize(ORemoteTask from, ORemoteTask to) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    from.toStream(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);
    to.fromStream(in, new ODefaultRemoteTaskFactoryV3());
  }
}
