package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import java.nio.file.Path;

public interface OStorageEngine {

  OStorage createLocal(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config);

  OStorage createMemory(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config);

  OStorage openLocal(OrientDBInternal context, String name, OContextConfiguration config);

  boolean exists(String name);

  OStorage createForRestoreLocal(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config);

  OStorage createForRestoreMemory(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config);

  void init(Path basePath, OContextConfiguration configurations);

  String getName();

  void shutdown();

  record RegisterResult(OStorage storage, boolean created) {}
  ;

  RegisterResult registerLocal(
      OrientDBInternal orientDBEmbedded, String name, Path p, OContextConfiguration configurations);
}
