package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;

import java.io.IOException;

/**
 * Created by tglman on 16/12/15.
 */
public interface OStorageRemoteOperation<T> {

  T execute(final OChannelBinaryAsynchClient network) throws IOException;

}


