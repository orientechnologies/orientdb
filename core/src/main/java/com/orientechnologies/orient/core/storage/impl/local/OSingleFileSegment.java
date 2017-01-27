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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;
import java.nio.file.Paths;

import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

public class OSingleFileSegment {
  protected OLocalPaginatedStorage    storage;
  protected OFile                     file;
  protected OStorageFileConfiguration config;
  private boolean wasSoftlyClosedAtPreviousTime = true;

  public OSingleFileSegment(final OLocalPaginatedStorage iStorage, final OStorageFileConfiguration iConfig) throws IOException {
    this(iStorage, iConfig, iConfig.type);
  }

  public OSingleFileSegment(final OLocalPaginatedStorage iStorage, final OStorageFileConfiguration iConfig, final String iType)
       {
    config = iConfig;
    storage = iStorage;
    file = new OFileClassic(Paths.get(iStorage.getVariableParser().resolveVariables(iConfig.path)));
  }

  public void open() throws IOException {
    file.open();
  }

  public void create(final int iStartSize) throws IOException {
    file.create();
  }

  public void close() throws IOException {
    if (file != null)
      file.close();
  }

  public void delete() throws IOException {
    if (file != null)
      file.delete();
  }

  public void truncate() throws IOException {
    // SHRINK TO 0
    file.shrink(0);
  }

  public boolean exists() {
    return file.exists();
  }

  public long getSize() {
    return file.getFileSize();
  }

  public long getFilledUpTo() {
    return file.getFileSize();
  }

  public OStorageFileConfiguration getConfig() {
    return config;
  }

  public OFile getFile() {
    return file;
  }

  public void synch() throws IOException {
    file.synch();
  }
}
