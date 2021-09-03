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

public class OListGlobalConfigurationsResponse implements OBinaryResponse {
  private Map<String, String> configs;

  public OListGlobalConfigurationsResponse() {}

  public OListGlobalConfigurationsResponse(Map<String, String> configs) {
    super();
    this.configs = configs;
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeShort((short) configs.size());
    for (Entry<String, String> entry : configs.entrySet()) {
      channel.writeString(entry.getKey());
      channel.writeString(entry.getValue());
    }
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    configs = new HashMap<String, String>();
    final int num = network.readShort();
    for (int i = 0; i < num; ++i) configs.put(network.readString(), network.readString());
  }

  public Map<String, String> getConfigs() {
    return configs;
  }
}
