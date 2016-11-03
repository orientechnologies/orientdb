package com.orientechnologies.orient.client.remote;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

/**
 * Created by tglman on 07/06/16.
 */
public interface OBinaryRequest<T extends OBinaryResponse> {

  void write(final OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException;

  void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException;

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
