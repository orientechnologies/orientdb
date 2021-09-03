package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 07/06/16. */
public interface OBinaryRequest<T extends OBinaryResponse> {

  void write(final OChannelDataOutput network, OStorageRemoteSession session) throws IOException;

  void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException;

  byte getCommand();

  T createResponse();

  OBinaryResponse execute(OBinaryRequestExecutor executor);

  String getDescription();

  default boolean requireServerUser() {
    return false;
  }

  default boolean requireDatabaseSession() {
    return true;
  }

  default String requiredServerRole() {
    return "";
  }
}
