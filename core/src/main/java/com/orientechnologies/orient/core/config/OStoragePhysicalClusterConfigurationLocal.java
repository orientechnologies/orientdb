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
package com.orientechnologies.orient.core.config;

public class OStoragePhysicalClusterConfigurationLocal extends OStorageSegmentConfiguration implements
    OStoragePhysicalClusterConfiguration {
  private static final long         serialVersionUID = 1L;
  private static final String       START_SIZE       = "1Mb";

  private OStorageFileConfiguration holeFile;
  private int                       dataSegmentId;

  public OStoragePhysicalClusterConfigurationLocal(final OStorageConfiguration iStorageConfiguration, final int iId,
      final int iDataSegmentId) {
    super(iStorageConfiguration, null, iId);
    fileStartSize = START_SIZE;
    dataSegmentId = iDataSegmentId;
  }

  public OStoragePhysicalClusterConfigurationLocal(final OStorageConfiguration iStorageConfiguration, final int iId,
      final int iDataSegmentId, final String iSegmentName) {
    super(iStorageConfiguration, iSegmentName, iId);
    fileStartSize = START_SIZE;
    dataSegmentId = iDataSegmentId;
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  @Override
  public void setRoot(final OStorageConfiguration root) {
    super.setRoot(root);
    holeFile.parent = this;
  }

  public int getDataSegmentId() {
    return dataSegmentId;
  }

  public OStorageFileConfiguration[] getInfoFiles() {
    return infoFiles;
  }

  public String getMaxSize() {
    return maxSize;
  }

  public OStorageFileConfiguration getHoleFile() {
    return holeFile;
  }

  public void setHoleFile(OStorageFileConfiguration holeFile) {
    this.holeFile = holeFile;
  }

  public void setDataSegmentId(int dataSegmentId) {
    this.dataSegmentId = dataSegmentId;
  }

}
