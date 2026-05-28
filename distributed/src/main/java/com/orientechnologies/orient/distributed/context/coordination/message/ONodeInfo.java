package com.orientechnologies.orient.distributed.context.coordination.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public record ONodeInfo(
    String version,
    List<ONodeInfoListener> listeners,
    long usedMemory,
    long freeMemory,
    long maxMemory) {

  // TODO: handle latencies and stats probably specific messages are better for it
  // TODO: maybe move also the memory stats somewhere else
  //  nodeCfg.setLatencies("latencies", getMessageService().getLatencies());
  //  nodeCfg.setMessages("messages", getMessageService().getMessageStats());

  public static ONodeInfo fromNetwork(DataInput input) throws IOException {
    String version = input.readUTF();
    int size = input.readInt();
    List<ONodeInfoListener> listeners = new ArrayList<>(size);
    while (size-- > 0) {
      listeners.add(ONodeInfoListener.readNetwork(input));
    }
    var usedMemory = input.readLong();
    var freeMemory = input.readLong();
    var maxMemory = input.readLong();
    return new ONodeInfo(version, listeners, usedMemory, freeMemory, maxMemory);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(version);
    out.writeInt(listeners.size());
    for (var listener : listeners) {
      listener.writeNetwork(out);
    }
    out.writeLong(usedMemory);
    out.writeLong(freeMemory);
    out.writeLong(maxMemory);
  }
}
