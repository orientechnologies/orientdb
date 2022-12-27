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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class OStorageAbstract implements OStorage {
  public static final ThreadGroup storageThreadGroup;

  static {
    ThreadGroup parentThreadGroup = Thread.currentThread().getThreadGroup();

    final ThreadGroup parentThreadGroupBackup = parentThreadGroup;

    boolean found = false;

    while (parentThreadGroup.getParent() != null) {
      if (parentThreadGroup.equals(Orient.instance().getThreadGroup())) {
        parentThreadGroup = parentThreadGroup.getParent();
        found = true;
        break;
      } else parentThreadGroup = parentThreadGroup.getParent();
    }

    if (!found) parentThreadGroup = parentThreadGroupBackup;

    storageThreadGroup = new ThreadGroup(parentThreadGroup, "OrientDB Storage");
  }

  protected final String url;
  protected final String mode;
  protected final ReentrantReadWriteLock stateLock;

  protected volatile OStorageConfiguration configuration;
  protected volatile OCurrentStorageComponentsFactory componentsFactory;
  protected String name;
  private final AtomicLong version = new AtomicLong();

  protected volatile STATUS status = STATUS.CLOSED;

  protected AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

  public OStorageAbstract(final String name, final String iURL, final String mode) {
    this.name = normalizeName(name);

    if (OStringSerializerHelper.contains(this.name, ','))
      throw new IllegalArgumentException("Invalid character in storage name: " + this.name);

    url = iURL;
    this.mode = mode;

    stateLock = new ReentrantReadWriteLock();
  }

  protected String normalizeName(String name) {
    if (OStringSerializerHelper.contains(name, '/')) {
      name = name.substring(name.lastIndexOf("/") + 1);

      if (OStringSerializerHelper.contains(name, '\\'))
        return name.substring(name.lastIndexOf("\\") + 1);
      else return name;

    } else if (OStringSerializerHelper.contains(name, '\\')) {
      name = name.substring(name.lastIndexOf("\\") + 1);

      if (OStringSerializerHelper.contains(name, '/'))
        return name.substring(name.lastIndexOf("/") + 1);
      else return name;
    } else {
      return name;
    }
  }

  @Override
  @Deprecated
  public OStorage getUnderlying() {
    return this;
  }

  @Override
  public OStorageConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public boolean isClosed() {
    return status == STATUS.CLOSED;
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return ppos != null && !ORecordVersionHelper.isTombstone(ppos.recordVersion);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getURL() {
    return url;
  }

  @Override
  public void close() {
    close(false, false);
  }

  @Override
  public void close(final boolean iForce, boolean onDelete) {}

  /** Returns current storage's version as serial. */
  @Override
  public long getVersion() {
    return version.get();
  }

  @Override
  public boolean dropCluster(final String iClusterName) {
    return dropCluster(getClusterIdByName(iClusterName));
  }

  @Override
  public long countRecords() {
    long tot = 0;

    for (OCluster c : getClusterInstances())
      if (c != null) tot += c.getEntries() - c.getTombstonesCount();

    return tot;
  }

  @Override
  public String toString() {
    return url != null ? url : "?";
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Deprecated
  @Override
  public boolean isDistributed() {
    return false;
  }

  @Override
  public boolean isAssigningClusterIds() {
    return true;
  }

  @Override
  public OCurrentStorageComponentsFactory getComponentsFactory() {
    return componentsFactory;
  }

  @Override
  public void shutdown() {
    close(true, false);
  }
}
