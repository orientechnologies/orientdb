package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

public class OStorageRemoteAsynchEventListener implements ORemoteServerEventListener {

  private OStorageRemote storage;

  public OStorageRemoteAsynchEventListener(final OStorageRemote storage) {
    this.storage = storage;
  }

  public void onRequest(final byte iRequestCode, final Object obj) {
    if (iRequestCode == OChannelBinaryProtocol.REQUEST_PUSH_RECORD)
      // ASYNCHRONOUS PUSH INTO THE LEVEL2 CACHE
      storage.getLevel2Cache().updateRecord((ORecordInternal<?>) obj);
    else if (iRequestCode == OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG) {
      storage.updateClusterConfiguration((byte[]) obj);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Received new cluster configuration: %s", storage.getClusterConfiguration().toJSON(""));
    }
  }

  public OStorageRemote getStorage() {
    return storage;
  }
}
