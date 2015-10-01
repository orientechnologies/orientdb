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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OStorageRemoteAsynchEventListener implements ORemoteServerEventListener {

  private Map<Integer, OLiveResultListener> liveQueryListeners = new ConcurrentHashMap<Integer, OLiveResultListener>();

  private OStorageRemote                    storage;

  public OStorageRemoteAsynchEventListener(final OStorageRemote storage) {
    this.storage = storage;
  }

  public void onRequest(final byte iRequestCode, final Object obj) {
    if (iRequestCode == OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG) {
      storage.updateClusterConfiguration(storage.getCurrentServerURL(), (byte[]) obj);

      if (OLogManager.instance().isDebugEnabled()) {
        synchronized (storage.getClusterConfiguration()) {
          OLogManager.instance().debug(this, "Received new cluster configuration: %s",
              storage.getClusterConfiguration().toJSON("prettyPrint"));
        }
      }
    } else if (iRequestCode == OChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY) {
      byte[] bytes = (byte[]) obj;
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
      Integer id = null;
      try {
        byte op = dis.readByte();
        id = dis.readInt();

        final ORecord record = Orient.instance().getRecordFactoryManager().newInstance(dis.readByte());

        final ORecordId rid = readRID(dis);
        final ORecordVersion version = readVersion(dis);
        final byte[] content = readBytes(dis);
        ORecordInternal.fill(record, rid, version, content, false);

        OLiveResultListener listener = liveQueryListeners.get(id);
        if (listener != null) {
          listener.onLiveResult(id, new ORecordOperation(record, op));
        } else {
          OLogManager.instance().warn(this, "Receiving invalid LiveQuery token: " + id);
        }

      } catch (IOException e) {
        if (id != null) {
          OLiveResultListener listener = liveQueryListeners.get(id);
          if (listener != null) {
            listener.onError(id);
          }
        }
        e.printStackTrace();
      }

    }
    byte op;

  }

  private ORecordVersion readVersion(DataInputStream dis) throws IOException {
    final ORecordVersion version = OVersionFactory.instance().createVersion();
    version.setCounter(dis.readInt());
    return version;
  }

  private ORecordId readRID(DataInputStream dis) throws IOException {
    final int clusterId = dis.readShort();
    final long clusterPosition = dis.readLong();
    return new ORecordId(clusterId, clusterPosition);
  }

  public byte[] readBytes(DataInputStream in) throws IOException {
    // TODO see OChannelBinary
    final int len = in.readInt();
    if (len < 0)
      return null;
    final byte[] tmp = new byte[len];
    in.readFully(tmp);
    return tmp;
  }

  public OStorageRemote getStorage() {
    return storage;
  }

  public void registerLiveListener(Integer id, OLiveResultListener listener) {
    this.liveQueryListeners.put(id, listener);
  }

  public void unregisterLiveListener(Integer id) {
    this.liveQueryListeners.remove(id);
  }
}
