package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import java.io.InputStream;
import java.nio.file.Path;

public interface OStorageEngine {

  public enum OBackupType {
    FOLDER_ZIP,
    FULL_INCREMENTAL
  }

  OStorage createLocal(OrientDBInternal context, String name, OContextConfiguration config);

  OStorage createMemory(OrientDBInternal context, String name, OContextConfiguration config);

  OStorage openLocal(OrientDBInternal context, String name, OContextConfiguration config);

  OStorage openMemory(OrientDBInternal context, String name, OContextConfiguration config);

  OStorage restoreStream(
      OrientDBInternal context,
      String name,
      OContextConfiguration config,
      InputStream stream,
      OBackupType type);

  OStorage restoreFile(
      OrientDBInternal context, String name, OContextConfiguration config, Path path);

  void init(Path basePath, OContextConfiguration configurations);

  void shutdown();
}
