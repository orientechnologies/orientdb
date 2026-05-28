package com.orientechnologies.orient.distributed.context.coordination.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ONodeInfoListener(String protocol, String address) {

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(protocol);
    out.writeUTF(address);
  }

  public static ONodeInfoListener readNetwork(DataInput input) throws IOException {
    String protocol = input.readUTF();
    String address = input.readUTF();
    return new ONodeInfoListener(protocol, address);
  }
}
