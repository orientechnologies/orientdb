package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;

/**
 * Created by tglman on 11/01/17.
 */
public class OPushDistributedConfigurationRequest implements OBinaryPushRequest {

  public ODocument configuration;

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    channel.writeBytes(OMessageHelper.getRecordBytes(configuration, serializer));
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetwork.INSTANCE;
    final ORecord record = Orient.instance().getRecordFactoryManager().newInstance(network.readByte());
    serializer.fromStream(network.readBytes(), record, null);
    configuration = (ODocument) record;
  }
}
