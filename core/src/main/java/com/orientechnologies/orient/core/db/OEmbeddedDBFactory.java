package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OSnowFlakeIdGen;
import com.orientechnologies.orient.core.storage.cache.OWriteCacheIdGen;
import com.orientechnologies.orient.core.storage.cache.local.twoq.O2QCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.memory.ODirectMemoryStorage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 08/04/16.
 */
public class OEmbeddedDBFactory implements OrientDBFactory {
  private volatile Map<String, OAbstractPaginatedStorage> storages = new HashMap<>();
  private final OrientDBSettings configurations;
  private       String           basePath;
  private       OEngine          memory;
  private       OEngine          disk;

  public OEmbeddedDBFactory(String directoryPath, OrientDBSettings configurations) {
    super();

    memory = Orient.instance().getEngine("memory");
    memory.startup();
    disk = Orient.instance().getEngine("plocal");
    disk.startup();

    this.basePath = new java.io.File(directoryPath).getAbsolutePath();
    this.configurations = configurations != null ? configurations : OrientDBSettings.defaultSettings();

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();

    try {
      if (OByteBufferPool.instance() != null)
        OByteBufferPool.instance().registerMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for byte buffer pool cannot be registered", e);
    }

  }

  @Override
  public synchronized ODatabaseDocument open(String name, String user, String password) {
    OAbstractPaginatedStorage storage = getStorage(name);
    storage.open(new HashMap<>());
    final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage);
    embedded.internalOpen(user, password);

    return embedded;
  }

  private OAbstractPaginatedStorage getStorage(String name) {
    OAbstractPaginatedStorage storage = storages.get(name);
    if (storage == null) {
      storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(name), new HashMap<>());
      storages.put("storage", storage);
    }
    return storage;
  }

  private String buildName(String name) {
    return basePath + "/" + name;
  }

  @Override
  public synchronized void create(String name, String user, String password, DatabaseType type) {
    if (!exist(name, user, password)) {
      OAbstractPaginatedStorage storage;
      if (type == DatabaseType.MEMORY) {
        storage = (OAbstractPaginatedStorage) memory.createStorage(buildName(name), new HashMap<>());
      } else {
        storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(name), new HashMap<>());
      }
      //CHECK Configurations
      storage.create(new HashMap<>());
      storages.put(name, storage);
      ORecordSerializer serializer = ORecordSerializerFactory.instance().getDefaultRecordSerializer();
      storage.getConfiguration().setRecordSerializer(serializer.toString());
      storage.getConfiguration().setRecordSerializerVersion(serializer.getCurrentVersion());
      // since 2.1 newly created databases use strict SQL validation by default
      storage.getConfiguration().setProperty(OStatement.CUSTOM_STRICT_SQL, "true");

      storage.getConfiguration().update();

      final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage);
      embedded.internalCreate();
    }
  }

  @Override
  public synchronized boolean exist(String name, String user, String password) {
    if (!storages.containsKey(name)) {
      return OLocalPaginatedStorage.exists(buildName(name));
    }
    return true;
  }

  @Override
  public synchronized void drop(String name, String user, String password) {
    if (exist(name, user, password)) {
      getStorage(name).delete();
      storages.remove(name);
    }
  }

  @Override
  public Map<String, String> listDatabases(String user, String password) {
    return null;
  }

  @Override
  public OPool<ODatabaseDocument> openPool(String name, String user, String password, Map<String, Object> poolSettings) {
    return null;
  }

  @Override
  public synchronized void close() {

    final List<OStorage> storagesCopy = new ArrayList<OStorage>(storages.values());
    for (OStorage stg : storagesCopy) {
      try {
        OLogManager.instance().info(this, "- shutdown storage: " + stg.getName() + "...");
        stg.shutdown();
      } catch (Throwable e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
      }
    }
    storages.clear();

    // SHUTDOWN ENGINES
    memory.shutdown();
    disk.shutdown();


  }

  public /*OServer*/ Object spawnServer(/*OServerConfiguration*/Object serverConfiguration) {
    return null;
  }

}
