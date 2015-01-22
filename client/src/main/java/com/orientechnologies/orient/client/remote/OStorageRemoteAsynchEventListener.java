/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

public class OStorageRemoteAsynchEventListener implements ORemoteServerEventListener {

  private OStorageRemote storage;

  public OStorageRemoteAsynchEventListener(final OStorageRemote storage) {
    this.storage = storage;
  }

  public void onRequest(final byte iRequestCode, final Object obj) {
    if (iRequestCode == OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG) {
      storage.updateClusterConfiguration(storage.getCurrentServerURL(), (byte[]) obj);

      if (OLogManager.instance().isDebugEnabled()) {
        synchronized (storage.getClusterConfiguration()) {
          OLogManager.instance()
              .debug(this, "Received new cluster configuration: %s", storage.getClusterConfiguration().toJSON("prettyPrint"));
        }
      }
    }
  }

  public OStorageRemote getStorage() {
    return storage;
  }
}
