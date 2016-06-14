package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;

import java.io.IOException;

/**
 * Created by tglman on 07/06/16.
 */
public interface OStorageRemoteOperationRead<T> {

  T execute(final OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException;
}
