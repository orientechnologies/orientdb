/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.task;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;

/**
 * Distributed task base abstract class used for distributed actions and events.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractDistributedTask<T> implements Callable<T>, Externalizable {
  private static final long serialVersionUID = 1L;

  protected String          nodeSource;
  protected String          databaseName;
  protected EXECUTION_MODE  mode;
  protected boolean         redistribute;

  public abstract String getName();

  public OAbstractDistributedTask() {
    redistribute = false;
  }

  public OAbstractDistributedTask(final String nodeSource, final String databaseName, final EXECUTION_MODE iMode) {
    this.nodeSource = nodeSource;
    this.databaseName = databaseName;
    this.mode = iMode;
    this.redistribute = true;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(nodeSource);
    out.writeUTF(databaseName);
    out.writeByte(mode.ordinal());
    out.writeByte(redistribute ? 1 : 0);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    nodeSource = in.readUTF();
    databaseName = in.readUTF();
    mode = EXECUTION_MODE.values()[in.readByte()];
    redistribute = in.readByte() == 1;
  }

  public String getNodeSource() {
    return nodeSource;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public EXECUTION_MODE getMode() {
    return mode;
  }

  public void setMode(final EXECUTION_MODE iMode) {
    mode = iMode;
  }

  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  protected OStorage getStorage() {
    OStorage stg = Orient.instance().getStorage(databaseName);
    if (stg == null) {
      final String url = OServerMain.server().getStorageURL(databaseName);

      if (url == null) {
        OLogManager.instance().error(this,
            "DISTRIBUTED <- database '%s' is not configured on this server. Copy the database here to enable the replication",
            databaseName);
        return null;
      }

      stg = Orient.instance().loadStorage(url);
    }

    if (stg.isClosed())
      stg.open(null, null, null);
    return stg;
  }

  protected OStorageSynchronizer getDatabaseSynchronizer() {
    return getDistributedServerManager().getDatabaseSynchronizer(databaseName);
  }

  protected ODistributedServerManager getDistributedServerManager() {
    return (ODistributedServerManager) OServerMain.server().getVariable("ODistributedAbstractPlugin");
  }

  public boolean isRedistribute() {
    return redistribute;
  }

  public OAbstractDistributedTask<T> setRedistribute(boolean redistribute) {
    this.redistribute = redistribute;
    return this;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  protected void setAsCompleted(final OStorageSynchronizer dbSynchronizer, long operationLogOffset) throws IOException {
    dbSynchronizer.getLog().changeOperationStatus(operationLogOffset, null);
  }
}
