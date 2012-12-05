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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OStorage;

public abstract class OClusterMemory extends OSharedResourceAdaptive implements OCluster {
  public static final String TYPE = "MEMORY";

  private OStorage           storage;
  private int                id;
  private String             name;
  private int                dataSegmentId;

  public OClusterMemory() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
  }

  public void configure(final OStorage iStorage, final OStorageClusterConfiguration iConfig) throws IOException {
    configure(iStorage, iConfig.getId(), iConfig.getName(), iConfig.getLocation(), iConfig.getDataSegmentId());
  }

  public void configure(final OStorage iStorage, final int iId, final String iClusterName, final String iLocation,
      final int iDataSegmentId, final Object... iParameters) {
    this.storage = iStorage;
    this.id = iId;
    this.name = iClusterName;
    this.dataSegmentId = iDataSegmentId;
  }

  public int getDataSegmentId() {
    acquireSharedLock();
    try {

      return dataSegmentId;

    } finally {
      releaseSharedLock();
    }
  }

  public OClusterEntryIterator absoluteIterator() {
    return new OClusterEntryIterator(this);
  }

  public void close() {
    acquireExclusiveLock();
    try {

      clear();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void open() throws IOException {
  }

  public void create(final int iStartSize) throws IOException {
  }

  public void delete() throws IOException {
    acquireExclusiveLock();
    try {

      close();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void truncate() throws IOException {
    storage.checkForClusterPermissions(getName());

    acquireExclusiveLock();
    try {

      clear();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void set(OCluster.ATTRIBUTES iAttribute, Object iValue) throws IOException {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    switch (iAttribute) {
    case NAME:
      name = stringValue;
      break;

    case DATASEGMENT:
      dataSegmentId = storage.getDataSegmentIdByName(stringValue);
      break;
    }
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public void synch() {
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  public void lock() {
    acquireSharedLock();
  }

  public void unlock() {
    releaseSharedLock();
  }

  public String getType() {
    return TYPE;
  }

  protected abstract void clear();
}
