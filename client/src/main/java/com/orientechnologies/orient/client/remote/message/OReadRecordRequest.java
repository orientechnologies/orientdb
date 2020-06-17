/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public final class OReadRecordRequest implements OBinaryRequest<OReadRecordResponse> {
  private boolean ignoreCache;
  private ORecordId rid;
  private String fetchPlan;
  private boolean loadTumbstone;

  public OReadRecordRequest(
      boolean iIgnoreCache, ORecordId iRid, String iFetchPlan, boolean iLoadTumbstone) {
    this.ignoreCache = iIgnoreCache;
    this.rid = iRid;
    this.fetchPlan = iFetchPlan;
    this.loadTumbstone = iLoadTumbstone;
  }

  public OReadRecordRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeRID(rid);
    network.writeString(fetchPlan != null ? fetchPlan : "");
    network.writeByte((byte) (ignoreCache ? 1 : 0));
    network.writeByte((byte) (loadTumbstone ? 1 : 0));
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    rid = channel.readRID();
    fetchPlan = channel.readString();
    ignoreCache = channel.readByte() != 0;
    loadTumbstone = channel.readByte() != 0;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_LOAD;
  }

  @Override
  public String getDescription() {
    return "Load record";
  }

  public ORecordId getRid() {
    return rid;
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public boolean isIgnoreCache() {
    return ignoreCache;
  }

  public boolean isLoadTumbstone() {
    return loadTumbstone;
  }

  @Override
  public OReadRecordResponse createResponse() {
    return new OReadRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeReadRecord(this);
  }
}
