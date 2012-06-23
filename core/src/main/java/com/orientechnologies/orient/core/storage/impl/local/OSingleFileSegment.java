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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.fs.OMMapManager;

public class OSingleFileSegment {
  protected OStorageLocal             storage;
  protected OFile                     file;
  protected OStorageFileConfiguration config;

  public OSingleFileSegment(final String iPath, final String iType) throws IOException {
    file = OFileFactory.instance().create(iType, OSystemVariableResolver.resolveSystemVariables(iPath), "rw");
  }

  public OSingleFileSegment(final OStorageLocal iStorage, final OStorageFileConfiguration iConfig) throws IOException {
    this(iStorage, iConfig, iConfig.type);
  }

  public OSingleFileSegment(final OStorageLocal iStorage, final OStorageFileConfiguration iConfig, final String iType)
      throws IOException {
    config = iConfig;
    storage = iStorage;
    file = OFileFactory.instance().create(iType, iStorage.getVariableParser().resolveVariables(iConfig.path), iStorage.getMode());
    file.setMaxSize((int) OFileUtils.getSizeAsNumber(iConfig.maxSize));
    file.setIncrementSize((int) OFileUtils.getSizeAsNumber(iConfig.incrementSize));
  }

  public boolean open() throws IOException {
    boolean softClosed = file.open();
    if (!softClosed) {
      // LAST TIME THE FILE WAS NOT CLOSED IN SOFT WAY
      OLogManager.instance().warn(this,
          "file " + file.getAbsolutePath() + " was not closed correctly last time. Checking segments...");
    }

    return softClosed;
  }

  public void create(final int iStartSize) throws IOException {
    file.create(iStartSize);
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
    return file.getFilledUpTo();
  }

  public OStorageFileConfiguration getConfig() {
    return config;
  }

  public OFile getFile() {
    return file;
  }
}
