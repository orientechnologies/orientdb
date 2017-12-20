package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

/**
 * Created by tglman on 10/05/17.
 */
public interface ORemotePushHandler {

  OChannelBinary getNetwork(String host);

  OBinaryPushRequest createPush(byte push);

  OBinaryPushResponse executeUpdateDistributedConfig(OPushDistributedConfigurationRequest request);

  OBinaryPushResponse executeUpdateStorageConfig(OPushStorageConfigurationRequest request);

  void executeLiveQueryPush(OLiveQueryPushRequest pushRequest);

  void onPushReconnect(String host);

  void onPushDisconnect(OChannelBinary network, Exception e);

  void returnSocket(OChannelBinary network);

  OBinaryPushResponse executeUpdateSchema(OPushSchemaRequest request);
}
