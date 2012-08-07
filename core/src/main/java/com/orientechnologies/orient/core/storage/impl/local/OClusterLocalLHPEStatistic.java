package com.orientechnologies.orient.core.storage.impl.local;

import java.io.File;
import java.io.IOException;

import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

/**
 * @author Andrey Lomakin
 * @since 03.08.12
 */
public class OClusterLocalLHPEStatistic extends OSingleFileSegment {
  public OClusterLocalLHPEStatistic(String iPath, String iType) throws IOException {
    super(iPath, iType);
  }

  public OClusterLocalLHPEStatistic(OStorageLocal iStorage, OStorageFileConfiguration iConfig) throws IOException {
    super(iStorage, iConfig);
  }

  public OClusterLocalLHPEStatistic(OStorageLocal iStorage, OStorageFileConfiguration iConfig, String iType) throws IOException {
    super(iStorage, iConfig, iType);
  }

  public void rename(String iOldName, String iNewName) {
    final String osFileName = file.getName();
    if (osFileName.startsWith(iOldName)) {
      final File newFile = new File(storage.getStoragePath() + '/' + iNewName
          + osFileName.substring(osFileName.lastIndexOf(iOldName) + iOldName.length()));
      boolean renamed = file.renameTo(newFile);
      while (!renamed) {
        OMemoryWatchDog.freeMemory(100);
        renamed = file.renameTo(newFile);
      }
    }
  }
}
