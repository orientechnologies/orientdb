package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.MemTrace;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.jnr.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEngine;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.chm.AsyncReadCache;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.memory.ODirectMemoryStorage;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OStorageEnginePaginatedLocal implements OStorageEngine {
  private static final OLogger logger =
      OLogManager.instance().logger(OStorageEnginePaginatedLocal.class);

  private OByteBufferPool memoryPool;
  protected volatile OReadCache readCache;

  protected OClosableLinkedContainer<Long, OFile> files;

  /** Keeps track of next possible storage id. */
  private static final AtomicInteger nextStorageId = new AtomicInteger();

  /** Storage IDs current assigned to the storage. */
  private static final Set<Integer> currentStorageIds =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  protected long maxWALSegmentSize;
  protected long doubleWriteLogMaxSegSize;
  private Path basePath;

  protected final int generateStorageId() {
    int storageId = Math.abs(nextStorageId.getAndIncrement());
    while (!currentStorageIds.add(storageId)) {
      storageId = Math.abs(nextStorageId.getAndIncrement());
    }

    return storageId;
  }

  private static long calculateReadCacheMaxMemory(final long cacheSize) {
    return (long)
        (cacheSize
            * ((100 - OGlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0));
  }

  private static int getOpenFilesLimit() {
    if (OGlobalConfiguration.OPEN_FILES_LIMIT.getValueAsInteger() > 0) {
      logger.infoNoDb(
          "Limit of open files for disk cache will be set to %d.",
          OGlobalConfiguration.OPEN_FILES_LIMIT.getValueAsInteger());
      return OGlobalConfiguration.OPEN_FILES_LIMIT.getValueAsInteger();
    }

    final int defaultLimit = 512;
    final int recommendedLimit = 256 * 1024;

    return ONative.instance().getOpenFilesLimit(true, recommendedLimit, defaultLimit);
  }

  public void init(Path basePath, OContextConfiguration configurations) {
    this.basePath = basePath;
    memoryPool = OByteBufferPool.instance(configurations);
    files = new OClosableLinkedContainer<>(getOpenFilesLimit());
    final String userName = System.getProperty("user.name", "unknown");
    logger.infoNoDb("System is started under an effective user : `%s`", userName);
    if (ONative.instance().isOsRoot()) {
      logger.warnNoDb(
          "You are running under the \"root\" user privileges that introduces security risks."
              + " Please consider to run under a user dedicated to be used to run current"
              + " server instance.");
    }

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();

    final long diskCacheSize =
        calculateReadCacheMaxMemory(
            OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024);
    final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

    if (OGlobalConfiguration.DIRECT_MEMORY_PREALLOCATE.getValueAsBoolean()) {
      final int pageCount = (int) (diskCacheSize / pageSize);
      logger.info("Allocation of %d pages.", pageCount);

      final List<OPointer> pages = new ArrayList<>(pageCount);

      for (int i = 0; i < pageCount; i++) {
        pages.add(memoryPool.acquireDirect(true, MemTrace.PAGE_PRE_ALLOCATION));
      }

      for (final OPointer pointer : pages) {
        memoryPool.release(pointer);
      }

      pages.clear();
    }

    readCache = new AsyncReadCache(memoryPool, diskCacheSize, pageSize, false);

    if (basePath == null) {
      maxWALSegmentSize = -1;
      doubleWriteLogMaxSegSize = -1;
    } else {
      try {
        doubleWriteLogMaxSegSize = calculateDoubleWriteLogMaxSegSize(configurations, basePath);
        maxWALSegmentSize = calculateInitialMaxWALSegSize(basePath, configurations);

        if (maxWALSegmentSize <= 0) {
          throw new ODatabaseException(
              "Invalid configuration settings. Can not set maximum size of WAL segment");
        }

        logger.infoNoDb(
            "WAL maximum segment size is set to %,d MB", maxWALSegmentSize / 1024 / 1024);
      } catch (IOException e) {
        throw OException.wrapException(
            new ODatabaseException("Cannot initialize OrientDB engine"), e);
      }
    }

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();
  }

  @Override
  public OLocalPaginatedStorage createLocal(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config) {

    try {
      Path path = buildPath(name);
      OLocalPaginatedStorage storage = newLocalInstance(context, name, path, memoryPool);
      storage.create(config, id);
      return storage;
    } catch (Exception e) {
      final String message =
          "Error on opening database: "
              + name
              + ". Current location is: "
              + new java.io.File(".").getAbsolutePath();
      logger.error("%s", e, message);

      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

  @Override
  public RegisterResult registerLocal(
      OrientDBInternal context, String name, Path path, OContextConfiguration config) {
    OLocalPaginatedStorage storage = newLocalInstance(context, name, path, memoryPool);
    if (OLocalPaginatedStorage.exists(path)) {
      storage.open(config);
      return new RegisterResult(storage, false);
    } else {
      storage.create(config, new ODatabaseId());
      return new RegisterResult(storage, true);
    }
  }

  private int getIntConfig(OContextConfiguration configurations, OGlobalConfiguration config) {
    return configurations.getValueAsInteger(config);
  }

  private long getLongConfig(OContextConfiguration configurations, OGlobalConfiguration config) {
    return configurations.getValueAsLong(config);
  }

  private long calculateInitialMaxWALSegSize(Path basePath, OContextConfiguration configurations)
      throws IOException {
    String walPath = configurations.getValueAsString(OGlobalConfiguration.WAL_LOCATION);

    if (walPath == null) {
      walPath = basePath.toString();
    }

    final FileStore fileStore = Files.getFileStore(Paths.get(walPath));
    final long freeSpace = fileStore.getUsableSpace();

    long filesSize;
    try {
      filesSize =
          Files.walk(Paths.get(walPath))
              .mapToLong(
                  p -> {
                    try {
                      if (Files.isRegularFile(p)) {
                        return Files.size(p);
                      }

                      return 0;
                    } catch (IOException | UncheckedIOException e) {
                      logger.error("Error during calculation of free space for database", e);
                      return 0;
                    }
                  })
              .sum();
    } catch (IOException | UncheckedIOException e) {
      logger.error("Error during calculation of free space for database", e);

      filesSize = 0;
    }

    long maxSegSize =
        getLongConfig(configurations, OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE) * 1024 * 1024;

    if (maxSegSize <= 0) {
      int sizePercent =
          getIntConfig(configurations, OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE_PERCENT);
      if (sizePercent <= 0) {
        throw new ODatabaseException(
            "Invalid configuration settings. Can not set maximum size of WAL segment");
      }

      maxSegSize = (freeSpace + filesSize) / 100 * sizePercent;
    }

    final long minSegSizeLimit = (long) (freeSpace * 0.25);

    long minSegSize =
        getLongConfig(configurations, OGlobalConfiguration.WAL_MIN_SEG_SIZE) * 1024 * 1024;

    if (minSegSize > minSegSizeLimit) {
      minSegSize = minSegSizeLimit;
    }

    if (minSegSize > 0 && maxSegSize < minSegSize) {
      maxSegSize = minSegSize;
    }
    return maxSegSize;
  }

  private long calculateDoubleWriteLogMaxSegSize(
      OContextConfiguration configurations, Path storagePath) throws IOException {
    final FileStore fileStore = Files.getFileStore(storagePath);
    final long freeSpace = fileStore.getUsableSpace();

    long filesSize;
    try {
      filesSize =
          Files.walk(storagePath)
              .mapToLong(
                  p -> {
                    try {
                      if (Files.isRegularFile(p)) {
                        return Files.size(p);
                      }

                      return 0;
                    } catch (IOException | UncheckedIOException e) {
                      logger.error("Error during calculation of free space for database", e);

                      return 0;
                    }
                  })
              .sum();
    } catch (IOException | UncheckedIOException e) {
      logger.error("Error during calculation of free space for database", e);

      filesSize = 0;
    }

    long maxSegSize =
        getLongConfig(configurations, OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE)
            * 1024
            * 1024;

    if (maxSegSize <= 0) {
      int sizePercent =
          getIntConfig(
              configurations, OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE_PERCENT);

      if (sizePercent <= 0) {
        throw new ODatabaseException(
            "Invalid configuration settings. Can not set maximum size of WAL segment");
      }

      maxSegSize = (freeSpace + filesSize) / 100 * sizePercent;
    }

    long minSegSize =
        getLongConfig(configurations, OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MIN_SEG_SIZE)
            * 1024
            * 1024;

    if (minSegSize > 0 && maxSegSize < minSegSize) {
      maxSegSize = minSegSize;
    }
    return maxSegSize;
  }

  @Override
  public OStorage createMemory(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config) {
    try {
      ODirectMemoryStorage storage =
          new ODirectMemoryStorage(name, name, generateStorageId(), context, memoryPool);
      storage.create(config, id);
      return storage;
    } catch (Exception e) {
      logger.error("Error on opening in memory storage: %s", e, name);

      throw OException.wrapException(
          new ODatabaseException("Error on opening in memory storage:" + name), e);
    }
  }

  @Override
  public boolean exists(String name) {
    if (basePath == null) {
      return false;
    } else {
      return OLocalPaginatedStorage.exists(buildPath(name));
    }
  }

  protected Path buildPath(String name) {
    if (basePath == null) {
      throw new ODatabaseException(
          "OrientDB instanced created without physical path, only memory databases are allowed");
    }
    return this.basePath.resolve(name);
  }

  @Override
  public OStorage openLocal(OrientDBInternal context, String name, OContextConfiguration config) {
    try {
      Path path = buildPath(name);

      OLocalPaginatedStorage storage = newLocalInstance(context, name, path, memoryPool);
      storage.open(config);
      return storage;
    } catch (Exception e) {
      final String message =
          "Error on opening database: "
              + name
              + ". Current location is: "
              + new java.io.File(".").getAbsolutePath();
      logger.error("%s", e, message);

      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

  protected OLocalPaginatedStorage newLocalInstance(
      OrientDBInternal context, String name, Path path, OByteBufferPool pool) {
    return new OLocalPaginatedStorage(
        name,
        path.toString(),
        generateStorageId(),
        readCache,
        files,
        maxWALSegmentSize,
        doubleWriteLogMaxSegSize,
        context,
        pool);
  }

  @Override
  public OStorage createForRestoreLocal(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config) {
    return createLocal(context, new ODatabaseId("mock"), name, config);
  }

  @Override
  public OStorage createForRestoreMemory(
      OrientDBInternal context, ODatabaseId id, String name, OContextConfiguration config) {
    throw new UnsupportedOperationException("Cannot restore in memory database");
  }

  @Override
  public void shutdown() {
    readCache.clear();
    files.clear();
  }

  @Override
  public String getName() {
    return "plocal";
  }
}
