package com.orientechnologies.orient.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ONodeMessages(String name, long messages) {

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(name);
    out.writeLong(messages);
  }

  public static ONodeMessages readNetwork(DataInput input) throws IOException {
    var name = input.readUTF();
    var messages = input.readLong();
    return new ONodeMessages(name, messages);
  }
}
