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

/**
 * Created by tglman on 11/01/17.
 */
public class OPushDistributedConfigurationRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  public ODocument configuration;

  @Override
  public void write(OChannelDataOutput channel) throws IOException {
    channel.writeBytes(OMessageHelper.getRecordBytes(configuration, ORecordSerializerNetworkV37.INSTANCE));
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetworkV37.INSTANCE;
    final ORecord record = Orient.instance().getRecordFactoryManager().newInstance(network.readByte());
    serializer.fromStream(network.readBytes(), record, null);
    configuration = (ODocument) record;
  }

  public OBinaryPushResponse execute(OStorageRemote remote) {
    return null;
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return new OPushDistributedConfigurationResponse();
  }
}
