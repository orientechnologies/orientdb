package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tglman on 11/01/17.
 */
public class OPushDistributedConfigurationRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  public  ODocument    configuration;
  private List<String> hosts;

  public OPushDistributedConfigurationRequest(List<String> hosts) {
    this.hosts = hosts;
  }

  public OPushDistributedConfigurationRequest() {
  }

  @Override
  public void write(OChannelDataOutput channel) throws IOException {
    channel.writeInt(hosts.size());
    for (String host : hosts) {
      channel.writeString(host);
    }
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
    int size = network.readInt();
    hosts = new ArrayList<>(size);
    while (size-- > 0) {
      hosts.add(network.readString());
    }
  }

  public OBinaryPushResponse execute(OStorageRemote remote) {
    remote.updateDistributedNodes(this.hosts);
    return new OPushDistributedConfigurationResponse();
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return new OPushDistributedConfigurationResponse();
  }

  public List<String> getHosts() {
    return hosts;
  }
}
