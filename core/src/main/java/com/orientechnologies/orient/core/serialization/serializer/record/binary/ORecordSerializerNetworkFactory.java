package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/** Created by Enrico Risa on 10/04/17. */
public class ORecordSerializerNetworkFactory {

  public static ORecordSerializerNetworkFactory INSTANCE = new ORecordSerializerNetworkFactory();

  public ORecordSerializer current() {
    return forProtocol(OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);
  }

  public ORecordSerializer forProtocol(int protocolNumber) {

    if (protocolNumber >= OChannelBinaryProtocol.PROTOCOL_VERSION_37) {
      return ORecordSerializerNetworkV37.INSTANCE;
    } else {
      return ORecordSerializerNetwork.INSTANCE;
    }
  }
}
