package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;

/**
 * Created by tglman on 11/01/17.
 */
public class OStorageRemotePushThread extends Thread {

  public OStorageRemote storage;

  OStorageRemotePushThread(OStorageRemote storage) {
    this.storage = storage;
  }

  @Override
  public void run() {
    OChannelBinaryAsynchClient network = storage.getNetwork(storage.getURL());

  }
}
