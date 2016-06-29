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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 08/04/16.
 */
public class OEmbeddedDBFactory implements OrientDBFactory {
  private static final OWriteCacheIdGen writeCacheIdGen = new OSnowFlakeIdGen();
  private volatile O2QCache readCache;
  private volatile Map<String, OAbstractPaginatedStorage> storages = new HashMap<>();
  private final OrientDBSettings configurations;
  private       String           basePath;

  public OEmbeddedDBFactory(String directoryPath, OrientDBSettings configurations) {
    super();
    this.basePath = new java.io.File(directoryPath).getAbsolutePath();
    this.configurations = configurations != null ? configurations : OrientDBSettings.defaultSettings();

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();

    readCache = new O2QCache(calculateReadCacheMaxMemory(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024),
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, true,
        OGlobalConfiguration.DISK_CACHE_PINNED_PAGES.getValueAsInteger());

    try {
      if (OByteBufferPool.instance() != null)
        OByteBufferPool.instance().registerMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for byte buffer pool cannot be registered", e);
    }

  }

  private long calculateReadCacheMaxMemory(final long cacheSize) {
    return (long) (cacheSize * ((100 - OGlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0));
  }

  @Override
  public synchronized ODatabaseDocument open(String name, String user, String password) {
    OAbstractPaginatedStorage storage = getStorage(name);
    storage.open(new HashMap<>());
    final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage);
    embedded.internalOpen(user,password);

    return embedded;
  }

  private OAbstractPaginatedStorage getStorage(String name) {
    OAbstractPaginatedStorage storage = storages.get(name);
    if (storage == null) {
      try {
        storage = new OLocalPaginatedStorage(name, buildName(name), configurations.getStorageMode(), writeCacheIdGen.nextId(),
            readCache);
      } catch (Exception e) {
        final String message = "Error on opening database: " + name + ". Current location is: " + basePath;
        OLogManager.instance().error(this, message, e);

        throw OException.wrapException(new ODatabaseException(message), e);
      }
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
        storage = new ODirectMemoryStorage(name, buildName(name), configurations.getStorageMode(), writeCacheIdGen.nextId());
      } else {
        try {
          storage = new OLocalPaginatedStorage(name, buildName(name), configurations.getStorageMode(), writeCacheIdGen.nextId(),
              readCache);
        } catch (IOException e) {
          final String message = "Error on opening database: " + name + ". Current location is: " + basePath;
          OLogManager.instance().error(this, message, e);

          throw OException.wrapException(new ODatabaseException(message), e);
        }
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
  public void close() {

  }

  public /*OServer*/ Object spawnServer(/*OServerConfiguration*/Object serverConfiguration) {
    return null;
  }

}
