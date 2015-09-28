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

package com.orientechnologies.orient.core.config;

/**
 * @author Andrey Lomakin
 * @since 09.07.13
 */
public class OStoragePaginatedClusterConfiguration implements OStorageClusterConfiguration {
  public static float                    DEFAULT_GROW_FACTOR      = (float) 1.2;
  public float                           recordOverflowGrowFactor = DEFAULT_GROW_FACTOR;
  public float                           recordGrowFactor         = DEFAULT_GROW_FACTOR;
  public String                          compression;
  public String                          encryption;
  public String                          encryptionKey;
  public transient OStorageConfiguration root;
  public int                             id;
  public String                          name;
  public String                          location;
  public boolean                         useWal                   = true;
  public String                          conflictStrategy;
  private STATUS                         status                   = STATUS.ONLINE;

  public OStoragePaginatedClusterConfiguration(final OStorageConfiguration root, final int id, final String name,
      final String location, final boolean useWal, final float recordOverflowGrowFactor, final float recordGrowFactor,
      final String iCompression, final String iEncryption, final String iEncryptionKey, final String conflictStrategy,
      final STATUS iStatus) {
    this.root = root;
    this.id = id;
    this.name = name;
    this.location = location;
    this.useWal = useWal;
    this.recordOverflowGrowFactor = recordOverflowGrowFactor;
    this.recordGrowFactor = recordGrowFactor;
    this.compression = iCompression;
    this.encryption = iEncryption;
    this.encryptionKey = iEncryptionKey;
    this.conflictStrategy = conflictStrategy;
    this.status = iStatus;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getLocation() {
    return location;
  }

  @Override
  public int getDataSegmentId() {
    return -1;
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public void setStatus(final STATUS iStatus) {
    status = iStatus;
  }
}
