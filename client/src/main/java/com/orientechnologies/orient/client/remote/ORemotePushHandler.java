package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OPushFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OPushIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OPushSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OPushSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OPushStorageConfigurationRequest;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

/** Created by tglman on 10/05/17. */
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

  OBinaryPushResponse executeUpdateIndexManager(OPushIndexManagerRequest request);

  OBinaryPushResponse executeUpdateFunction(OPushFunctionsRequest request);

  OBinaryPushResponse executeUpdateSequences(OPushSequencesRequest request);
}
