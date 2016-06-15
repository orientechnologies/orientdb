package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;

import java.io.IOException;

/**
 * Created by tglman on 07/06/16.
 */
public interface OStorageRemoteOperationWrite {

  void execute(final OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException;
}
