package com.orientechnologies.orient.core.storage.impl.memory;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageMemoryConfiguration;
import com.orientechnologies.orient.core.version.OSimpleVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 7/9/14
 */
public class ODirectMemoryStorage extends OAbstractPaginatedStorage {
  private static final int ONE_KB = 1024;

  public ODirectMemoryStorage(String name, String filePath, String mode) {
    super(name, filePath, mode);
    configuration = new OStorageMemoryConfiguration(this);
  }

  @Override
  protected void initWalAndDiskCache() throws IOException {
    if (OGlobalConfiguration.USE_WAL.getValueAsBoolean()) {
      if (writeAheadLog == null)
        writeAheadLog = new OMemoryWriteAheadLog();
    } else
      writeAheadLog = null;

    if (diskCache == null)
      diskCache = new ODirectMemoryOnlyDiskCache(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * ONE_KB);
  }

  @Override
  protected void postCreateSteps() {
		ORecordId recordId = new ORecordId();
		recordId.clusterId = 0;
    createRecord(-1, recordId, new byte[0], new OSimpleVersion(), ORecordBytes.RECORD_TYPE,
        ODatabaseComplex.OPERATION_MODE.SYNCHRONOUS.ordinal(), null);
  }

  @Override
  public boolean exists() {
    return diskCache != null && diskCache.exists("default" + OPaginatedCluster.DEF_EXTENSION);
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
  protected void makeFuzzyCheckPoint() throws IOException {
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener,
      int compressionLevel, int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener)
      throws IOException {
    throw new UnsupportedOperationException();
  }
}