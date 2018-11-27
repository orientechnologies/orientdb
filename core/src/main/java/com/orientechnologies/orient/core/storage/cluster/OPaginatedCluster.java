package com.orientechnologies.orient.core.storage.cluster;

import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.File;
import java.io.IOException;

public abstract class OPaginatedCluster extends ODurableComponent implements OCluster {
  public enum RECORD_STATUS {
    NOT_EXISTENT, PRESENT, ALLOCATED, REMOVED
  }

  public static final String DEF_EXTENSION = ".pcl";

  public static int getLatestBinaryVersion() {
    return 1;
  }

  public OPaginatedCluster(OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }

  public abstract void replaceFile(File file) throws IOException;

  public abstract void replaceClusterMapFile(File file) throws IOException;

  public abstract RECORD_STATUS getRecordStatus(final long clusterPosition) throws IOException;

  public abstract OPaginatedClusterDebug readDebug(long clusterPosition) throws IOException;

  public abstract void registerInStorageConfig(OStorageConfigurationImpl root);

  public abstract long getFileId();
}
