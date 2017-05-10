package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

/**
 * Created by tglman on 10/05/17.
 */
public interface ORemotePushHandler {

  OChannelBinary getNetwork(String host);

  OBinaryPushRequest createPush(byte push);

  OBinaryPushResponse executeUpdateDistributedConfig(OPushDistributedConfigurationRequest request);
}
