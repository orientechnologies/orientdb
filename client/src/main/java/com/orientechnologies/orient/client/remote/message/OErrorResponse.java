package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class OErrorResponse implements OBinaryResponse {
  private Map<String, String> messages;
  private byte[] result;

  public OErrorResponse() {}

  public OErrorResponse(Map<String, String> messages, byte[] result) {
    this.messages = messages;
    this.result = result;
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    messages = new HashMap<>();
    while (network.readByte() == 1) {
      String key = network.readString();
      String value = network.readString();
      messages.put(key, value);
    }
    result = network.readBytes();
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    for (Entry<String, String> entry : messages.entrySet()) {
      // MORE DETAILS ARE COMING AS EXCEPTION
      channel.writeByte((byte) 1);

      channel.writeString(entry.getKey());
      channel.writeString(entry.getValue());
    }
    channel.writeByte((byte) 0);

    channel.writeBytes(result);
  }

  public Map<String, String> getMessages() {
    return messages;
  }

  public byte[] getResult() {
    return result;
  }
}
