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

package com.orientechnologies.orient.core.storage.impl.memory;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageMemoryConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OMemoryWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 7/9/14
 */
public class ODirectMemoryStorage extends OAbstractPaginatedStorage {
  private static final int ONE_KB = 1024;

  public ODirectMemoryStorage(String name, String filePath, String mode, int id) {
    super(name, filePath, mode, id);
    configuration = new OStorageMemoryConfiguration(this);
  }

  @Override
  protected void initWalAndDiskCache() throws IOException {
    if (configuration.getContextConfiguration().getValueAsBoolean(OGlobalConfiguration.USE_WAL)) {
      if (writeAheadLog == null)
        writeAheadLog = new OMemoryWriteAheadLog();
    } else
      writeAheadLog = null;

    final ODirectMemoryOnlyDiskCache diskCache = new ODirectMemoryOnlyDiskCache(
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * ONE_KB, 1);

    if (readCache == null) {
      readCache = diskCache;
    }

    if (writeCache == null) {
      writeCache = diskCache;
    }
  }

  @Override
  public boolean exists() {
    return readCache != null && writeCache.exists("default" + OPaginatedCluster.DEF_EXTENSION);
  }

  @Override
  public String getType() {
    return OEngineMemory.NAME;
  }

  public String getURL() {
    return OEngineMemory.NAME + ":" + url;
  }

  @Override
  public void makeFullCheckpoint() throws IOException {
  }

  @Override
  public List<String> backup(OutputStream out, Map<String, Object> options, Callable<Object> callable,
      OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected OLogSequenceNumber copyWALToIncrementalBackup(ZipOutputStream zipOutputStream, long startSegment) throws IOException {
    return null;
  }

  @Override
  protected boolean isWritesAllowedDuringBackup() {
    return false;
  }

  @Override
  protected File createWalTempDirectory() {
    return null;
  }

  @Override
  protected void addFileToDirectory(String name, InputStream stream, File directory) throws IOException {
  }

  @Override
  protected OWriteAheadLog createWalFromIBUFiles(File directory) throws IOException {
    return null;
  }
}
