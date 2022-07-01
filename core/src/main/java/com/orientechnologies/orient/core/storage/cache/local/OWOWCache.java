/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.common.collection.closabledictionary.OClosableEntry;
import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OQuarto;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OWriteCacheException;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLog;
import com.orientechnologies.orient.core.storage.fs.AsyncFile;
import com.orientechnologies.orient.core.storage.fs.IOResult;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.OPageIsBrokenListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.MetaDataRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Write part of disk cache which is used to collect pages which were changed on read cache and
 * store them to the disk in background thread. In current implementation only single background
 * thread is used to store all changed data, despite of SSD parallelization capabilities we suppose
 * that better to write data in single big chunk by one thread than by many small chunks from many
 * threads introducing contention and multi threading overhead. Another reasons for usage of only
 * one thread are
 *
 * <ol>
 *   <li>That we should give room for readers to read data during data write phase
 *   <li>It provides much less synchronization overhead
 * </ol>
 *
 * Background thread is running by with predefined intervals. Such approach allows SSD GC to use
 * pauses to make some clean up of half empty erase blocks. Also write cache is used for checking of
 * free space left on disk and putting of database in "read mode" if space limit is reached and to
 * perform fuzzy checkpoints. Write cache holds two different type of pages, pages which are shared
 * with read cache and pages which belong only to write cache (so called exclusive pages). Files in
 * write cache are accessed by id , there are two types of ids, internal used inside of write cache
 * and external used outside of write cache. Presence of two types of ids is caused by the fact that
 * read cache is global across all storages but each storage has its own write cache. So all ids of
 * files should be global across whole read cache. External id is created from internal id by
 * prefixing of internal id (in byte presentation) with bytes of write cache id which is unique
 * across all storages opened inside of single JVM. Write cache accepts external ids as file ids and
 * converts them to internal ids inside of its methods.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/23/13
 */
public final class OWOWCache extends OAbstractWriteCache
    implements OWriteCache, OCachePointer.WritersListener {

  private static final XXHashFactory XX_HASH_FACTORY = XXHashFactory.fastestInstance();
  private static final XXHash64 XX_HASH_64 = XX_HASH_FACTORY.hash64();
  private static final long XX_HASH_SEED = 0xAEF5634;

  private static final String ALGORITHM_NAME = "AES";
  private static final String TRANSFORMATION = "AES/CTR/NoPadding";

  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(OWOWCache::getCipherInstance);

  /** Extension for the file which contains mapping between file name and file id */
  private static final String NAME_ID_MAP_EXTENSION = ".cm";

  /** Name for file which contains first version of binary format */
  private static final String NAME_ID_MAP_V1 = "name_id_map" + NAME_ID_MAP_EXTENSION;

  /**
   * Name for file which contains second version of binary format. Second version of format contains
   * not only file name which is used in write cache but also file name which is used in file system
   * so those two names may be different which allows usage of case sensitive file names.
   */
  private static final String NAME_ID_MAP_V2 = "name_id_map_v2" + NAME_ID_MAP_EXTENSION;

  /**
   * Name for file which contains third version of binary format. Third version of format contains
   * not only file name which is used in write cache but also file name which is used in file system
   * so those two names may be different which allows usage of case sensitive file names. All this
   * information is wrapped by XX_HASH code which followed by content length, so any damaged records
   * are filtered out during loading of storage.
   */
  private static final String NAME_ID_MAP_V3 = "name_id_map_v3" + NAME_ID_MAP_EXTENSION;

  /**
   * Name of file temporary which contains third version of binary format. Temporary file is used to
   * prevent situation when DB is crashed because of migration to third version of binary format and
   * data are lost.
   *
   * @see #NAME_ID_MAP_V3
   * @see #storedNameIdMapToV3()
   */
  private static final String NAME_ID_MAP_V3_T = "name_id_map_v3_t" + NAME_ID_MAP_EXTENSION;

  /**
   * Name of the file which is used to compact file registry on close. All compacted data will be
   * written first to this file and then file will be atomically moved on the place of existing
   * registry.
   */
  private static final String NAME_ID_MAP_V2_BACKUP =
      "name_id_map_v2_backup" + NAME_ID_MAP_EXTENSION;

  /**
   * Maximum length of the row in file registry
   *
   * @see #NAME_ID_MAP_V3
   */
  private static final int MAX_FILE_RECORD_LEN = 16 * 1024;

  /** Marks pages which have a checksum stored. */
  public static final long MAGIC_NUMBER_WITH_CHECKSUM = 0xFACB03FEL;

  /** Marks pages which have a checksum stored and data encrypted */
  public static final long MAGIC_NUMBER_WITH_CHECKSUM_ENCRYPTED = 0x1L;

  /** Marks pages which have no checksum stored. */
  private static final long MAGIC_NUMBER_WITHOUT_CHECKSUM = 0xEF30BCAFL;

  /** Marks pages which have no checksum stored but have data encrypted */
  private static final long MAGIC_NUMBER_WITHOUT_CHECKSUM_ENCRYPTED = 0x2L;

  private static final int MAGIC_NUMBER_OFFSET = 0;

  public static final int CHECKSUM_OFFSET = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int PAGE_OFFSET_TO_CHECKSUM_FROM =
      OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

  private static final int CHUNK_SIZE = 64 * 1024 * 1024;

  /** Executor which runs in single thread all tasks are related to flush of write cache data. */
  private static final ScheduledExecutorService commitExecutor;

  static {
    commitExecutor =
        OThreadPoolExecutors.newSingleThreadScheduledPool(
            "OrientDB Write Cache Flush Task", OStorageAbstract.storageThreadGroup);
  }

  /** Limit of free space on disk after which database will be switched to "read only" mode */
  private final long freeSpaceLimit =
      OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * 1024L * 1024L;

  /** Listeners which are called once we detect that some of the pages of files are broken. */
  private final List<WeakReference<OPageIsBrokenListener>> pageIsBrokenListeners =
      new CopyOnWriteArrayList<>();

  /** Path to the storage root directory where all files served by write cache will be stored */
  private final Path storagePath;

  private final FileStore fileStore;

  /**
   * Container of all files are managed by write cache. That is special type of container which
   * ensures that only limited amount of files is open at the same time and opens closed files upon
   * request
   */
  private final OClosableLinkedContainer<Long, OFile> files;

  /**
   * The main storage of pages for write cache. If pages is hold by write cache it should be present
   * in this map. Map is ordered by position to speed up flush of pages to the disk
   */
  private final ConcurrentHashMap<PageKey, OCachePointer> writeCachePages =
      new ConcurrentHashMap<>();

  /**
   * Storage for the pages which are hold only by write cache and are not shared with read cache.
   */
  private final ConcurrentSkipListSet<PageKey> exclusiveWritePages = new ConcurrentSkipListSet<>();

  /**
   * Container for dirty pages. Dirty pages table is concept taken from ARIES protocol. It contains
   * earliest LSNs of operations on each page which is potentially changed but not flushed to the
   * disk. It allows us by calculation of minimal LSN contained by this container calculate which
   * part of write ahead log may be already truncated. "dirty pages" itself is managed using
   * following algorithm.
   *
   * <ol>
   *   <li>Just after acquiring the exclusive lock on page we fetch LSN of latest record logged into
   *       WAL
   *   <li>If page with given index is absent into table we add it to this container
   * </ol>
   *
   * Because we add last WAL LSN if we are going to modify page, it means that we can calculate
   * smallest LSN of operation which is not flushed to the log yet without locking of all operations
   * on database. There is may be situation when thread locks the page but did not add LSN to the
   * dirty pages table yet. If at the moment of start of iteration over the dirty pages table we
   * have a non empty dirty pages table it means that new operation on page will have LSN bigger
   * than any LSN already stored in table. If dirty pages table is empty at the moment of iteration
   * it means at the moment of start of iteration all page changes were flushed to the disk.
   */
  private final ConcurrentHashMap<PageKey, OLogSequenceNumber> dirtyPages =
      new ConcurrentHashMap<>();

  /**
   * Copy of content of {@link #dirtyPages} table at the moment when {@link
   * #convertSharedDirtyPagesToLocal()} was called. This field is not thread safe because it is used
   * inside of tasks which are running inside of {@link #commitExecutor} thread. It is used to keep
   * results of postprocessing of {@link #dirtyPages} table. Every time we invoke {@link
   * #convertSharedDirtyPagesToLocal()} all content of dirty pages is removed and copied to current
   * field and {@link #localDirtyPagesBySegment} filed. Such approach is possible because {@link
   * #dirtyPages} table is filled by many threads but is read only from inside of {@link
   * #commitExecutor} thread.
   */
  private final HashMap<PageKey, OLogSequenceNumber> localDirtyPages = new HashMap<>();

  /**
   * Copy of content of {@link #dirtyPages} table sorted by log segment and pages sorted by page
   * index.
   *
   * @see #localDirtyPages for details
   */
  private final TreeMap<Long, TreeSet<PageKey>> localDirtyPagesBySegment = new TreeMap<>();

  /** Approximate amount of all pages contained by write cache at the moment */
  private final AtomicLong writeCacheSize = new AtomicLong();

  /** Amount of exclusive pages are hold by write cache. */
  private final AtomicLong exclusiveWriteCacheSize = new AtomicLong();

  /** Serialized is used to encode/decode names of files are managed by write cache. */
  private final OBinarySerializer<String> stringSerializer;

  /** Size of single page in cache in bytes. */
  private final int pageSize;

  /** WAL instance */
  private final OWriteAheadLog writeAheadLog;

  /**
   * Lock manager is used to acquire locks in RW mode for cases when we are going to read or write
   * page from write cache.
   */
  private final OLockManager<PageKey> lockManager = new OPartitionedLockManager<>();

  /**
   * We acquire lock managed by this manager in read mode if we need to read data from files, and in
   * write mode if we add/remove/truncate file.
   */
  private final OReadersWriterSpinLock filesLock = new OReadersWriterSpinLock();

  /**
   * Mapping between case sensitive file names are used in write cache and file's internal id. Name
   * of file in write cache is case sensitive and can be different from file name which is used to
   * store file in file system.
   */
  private final ConcurrentMap<String, Integer> nameIdMap = new ConcurrentHashMap<>();

  /**
   * Mapping between file's internal ids and case sensitive file names are used in write cache. Name
   * of file in write cache is case sensitive and can be different from file name which is used to
   * store file in file system.
   */
  private final ConcurrentMap<Integer, String> idNameMap = new ConcurrentHashMap<>();

  private final Random fileIdGen = new Random();

  /** Path to the file which contains metadata for the files registered in storage. */
  private Path nameIdMapHolderPath;

  /** Write cache id , which should be unique across all storages. */
  private final int id;

  /**
   * Pool of direct memory <code>ByteBuffer</code>s. We can not use them directly because they do
   * not have deallocator.
   */
  private final OByteBufferPool bufferPool;

  private final String storageName;

  private volatile OChecksumMode checksumMode;

  /** Error thrown during data flush. Once error registered no more write operations are allowed. */
  private Throwable flushError;

  /** IV is used for AES encryption */
  private final byte[] iv;

  /** Key is used for AES encryption */
  private final byte[] aesKey;

  private final int exclusiveWriteCacheMaxSize;

  private final boolean callFsync;

  private final int chunkSize;

  private final long pagesFlushInterval;
  private volatile boolean stopFlush;
  private volatile Future<?> flushFuture;

  private final ConcurrentHashMap<ExclusiveFlushTask, CountDownLatch> triggeredTasks =
      new ConcurrentHashMap<>();

  private final int shutdownTimeout;

  /** Listeners which are called when exception in background data flush thread is happened. */
  private final List<WeakReference<OBackgroundExceptionListener>> backgroundExceptionListeners =
      new CopyOnWriteArrayList<>();

  /**
   * Double write log which is used in write cache to prevent page tearing in case of server crash.
   */
  private final DoubleWriteLog doubleWriteLog;

  private boolean closed;

  public OWOWCache(
      final int pageSize,
      final OByteBufferPool bufferPool,
      final OWriteAheadLog writeAheadLog,
      final DoubleWriteLog doubleWriteLog,
      final long pagesFlushInterval,
      final int shutdownTimeout,
      final long exclusiveWriteCacheMaxSize,
      final Path storagePath,
      final String storageName,
      final OBinarySerializer<String> stringSerializer,
      final OClosableLinkedContainer<Long, OFile> files,
      final int id,
      final OChecksumMode checksumMode,
      final byte[] iv,
      final byte[] aesKey,
      final boolean callFsync) {

    if (aesKey != null && aesKey.length != 16 && aesKey.length != 24 && aesKey.length != 32) {
      throw new OInvalidStorageEncryptionKeyException(
          "Invalid length of the encryption key, provided size is " + aesKey.length);
    }

    if (aesKey != null && iv == null) {
      throw new OInvalidStorageEncryptionKeyException("IV can not be null");
    }

    this.shutdownTimeout = shutdownTimeout;
    this.pagesFlushInterval = pagesFlushInterval;
    this.iv = iv;
    this.aesKey = aesKey;
    this.callFsync = callFsync;

    filesLock.acquireWriteLock();
    try {
      this.closed = true;

      this.id = id;
      this.files = files;
      this.chunkSize = CHUNK_SIZE / pageSize;

      this.pageSize = pageSize;
      this.writeAheadLog = writeAheadLog;
      this.bufferPool = bufferPool;

      this.checksumMode = checksumMode;
      this.exclusiveWriteCacheMaxSize = normalizeMemory(exclusiveWriteCacheMaxSize, pageSize);

      this.storagePath = storagePath;
      try {
        this.fileStore = Files.getFileStore(this.storagePath);
      } catch (final IOException e) {
        throw OException.wrapException(
            new OStorageException("Error during retrieving of file store"), e);
      }

      this.stringSerializer = stringSerializer;
      this.storageName = storageName;

      this.doubleWriteLog = doubleWriteLog;

      if (pagesFlushInterval > 0) {
        flushFuture =
            commitExecutor.schedule(
                new PeriodicFlushTask(), pagesFlushInterval, TimeUnit.MILLISECONDS);
      }

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /** Loads files already registered in storage. Has to be called before usage of this cache */
  public void loadRegisteredFiles() throws IOException, InterruptedException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      doubleWriteLog.open(storageName, storagePath, pageSize);

      closed = false;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * Adds listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to trigger
   */
  @Override
  public void addBackgroundExceptionListener(final OBackgroundExceptionListener listener) {
    backgroundExceptionListeners.add(new WeakReference<>(listener));
  }

  /**
   * Removes listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to remove
   */
  @Override
  public void removeBackgroundExceptionListener(final OBackgroundExceptionListener listener) {
    final List<WeakReference<OBackgroundExceptionListener>> itemsToRemove = new ArrayList<>(1);

    for (final WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener l = ref.get();
      if (l != null && l.equals(listener)) {
        itemsToRemove.add(ref);
      }
    }

    backgroundExceptionListeners.removeAll(itemsToRemove);
  }

  /** Fires event about exception is thrown in data flush thread */
  private void fireBackgroundDataFlushExceptionEvent(final Throwable e) {
    for (final WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener listener = ref.get();
      if (listener != null) {
        listener.onException(e);
      }
    }
  }

  private static int normalizeMemory(final long maxSize, final int pageSize) {
    final long tmpMaxSize = maxSize / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
  }

  /**
   * Directory which contains all files managed by write cache.
   *
   * @return Directory which contains all files managed by write cache or <code>null</code> in case
   *     of in memory database.
   */
  @Override
  public Path getRootDirectory() {
    return storagePath;
  }

  /** @inheritDoc */
  @Override
  public void addPageIsBrokenListener(final OPageIsBrokenListener listener) {
    pageIsBrokenListeners.add(new WeakReference<>(listener));
  }

  /** @inheritDoc */
  @Override
  public void removePageIsBrokenListener(final OPageIsBrokenListener listener) {
    final List<WeakReference<OPageIsBrokenListener>> itemsToRemove = new ArrayList<>(1);

    for (final WeakReference<OPageIsBrokenListener> ref : pageIsBrokenListeners) {
      final OPageIsBrokenListener pageIsBrokenListener = ref.get();

      if (pageIsBrokenListener == null || pageIsBrokenListener.equals(listener)) {
        itemsToRemove.add(ref);
      }
    }

    pageIsBrokenListeners.removeAll(itemsToRemove);
  }

  private void callPageIsBrokenListeners(final String fileName, final long pageIndex) {
    for (final WeakReference<OPageIsBrokenListener> pageIsBrokenListenerWeakReference :
        pageIsBrokenListeners) {
      final OPageIsBrokenListener listener = pageIsBrokenListenerWeakReference.get();
      if (listener != null) {
        try {
          listener.pageIsBroken(fileName, pageIndex);
        } catch (final Exception e) {
          OLogManager.instance()
              .error(
                  this,
                  "Error during notification of page is broken for storage " + storageName,
                  e);
        }
      }
    }
  }

  @Override
  public long bookFileId(final String fileName) {
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      final Integer fileId = nameIdMap.get(fileName);
      if (fileId != null) {
        if (fileId < 0) {
          return composeFileId(id, -fileId);
        } else {
          throw new OStorageException(
              "File " + fileName + " has already been added to the storage");
        }
      }
      while (true) {
        final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
        if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
          nameIdMap.put(fileName, -nextId);
          idNameMap.put(-nextId, fileName);
          return composeFileId(id, nextId);
        }
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /** @inheritDoc */
  @Override
  public int pageSize() {
    return pageSize;
  }

  @Override
  public long loadFile(final String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      Integer fileId = nameIdMap.get(fileName);
      final OFile fileClassic;

      // check that file is already registered
      if (!(fileId == null || fileId < 0)) {
        final long externalId = composeFileId(id, fileId);
        fileClassic = files.get(externalId);

        if (fileClassic != null) {
          return externalId;
        } else {
          throw new OStorageException(
              "File with given name " + fileName + " only partially registered in storage");
        }
      }

      if (fileId == null) {
        while (true) {
          final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
          if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
            fileId = nextId;
            break;
          }
        }
      } else {
        idNameMap.remove(fileId);
        fileId = -fileId;
      }

      fileClassic = createFileInstance(fileName, fileId);

      if (!fileClassic.exists()) {
        throw new OStorageException(
            "File with name " + fileName + " does not exist in storage " + storageName);
      } else {
        // REGISTER THE FILE
        OLogManager.instance()
            .debug(
                this,
                "File '"
                    + fileName
                    + "' is not registered in 'file name - id' map, but exists in file system. Registering it");

        openFile(fileClassic);

        final long externalId = composeFileId(id, fileId);
        files.add(externalId, fileClassic);

        nameIdMap.put(fileName, fileId);
        idNameMap.put(fileId, fileName);

        writeNameIdEntry(new NameFileIdEntry(fileName, fileId, fileClassic.getName()), true);

        return externalId;
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Load file was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public long addFile(final String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      Integer fileId = nameIdMap.get(fileName);
      final OFile fileClassic;

      if (fileId != null && fileId >= 0) {
        throw new OStorageException(
            "File with name " + fileName + " already exists in storage " + storageName);
      }

      if (fileId == null) {
        while (true) {
          final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
          if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
            fileId = nextId;
            break;
          }
        }
      } else {
        idNameMap.remove(fileId);
        fileId = -fileId;
      }

      fileClassic = createFileInstance(fileName, fileId);
      createFile(fileClassic, callFsync);

      final long externalId = composeFileId(id, fileId);
      files.add(externalId, fileClassic);

      nameIdMap.put(fileName, fileId);
      idNameMap.put(fileId, fileName);

      writeNameIdEntry(new NameFileIdEntry(fileName, fileId, fileClassic.getName()), true);

      return externalId;
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("File add was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public long fileIdByName(final String fileName) {
    final Integer intId = nameIdMap.get(fileName);

    if (intId == null || intId < 0) {
      return -1;
    }

    return composeFileId(id, intId);
  }

  @Override
  public int internalFileId(final long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public long externalFileId(final int fileId) {
    return composeFileId(id, fileId);
  }

  @Override
  public Long getMinimalNotFlushedSegment() {
    final Future<Long> future = commitExecutor.submit(new FindMinDirtySegment());
    try {
      return future.get();
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void updateDirtyPagesTable(
      final OCachePointer pointer, final OLogSequenceNumber startLSN) {
    final long fileId = pointer.getFileId();
    final long pageIndex = pointer.getPageIndex();

    final PageKey pageKey = new PageKey(internalFileId(fileId), pageIndex);

    OLogSequenceNumber dirtyLSN;
    if (startLSN != null) {
      dirtyLSN = startLSN;
    } else {
      dirtyLSN = writeAheadLog.end();
    }

    if (dirtyLSN == null) {
      dirtyLSN = new OLogSequenceNumber(0, 0);
    }

    dirtyPages.putIfAbsent(pageKey, dirtyLSN);
  }

  @Override
  public void create() {}

  @Override
  public void open() {}

  @Override
  public long addFile(final String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      OFile fileClassic;

      final Integer existingFileId = nameIdMap.get(fileName);

      final int intId = extractFileId(fileId);

      if (existingFileId != null && existingFileId >= 0) {
        if (existingFileId == intId) {
          throw new OStorageException(
              "File with name '" + fileName + "'' already exists in storage '" + storageName + "'");
        } else {
          throw new OStorageException(
              "File with given name '"
                  + fileName
                  + "' already exists but has different id "
                  + existingFileId
                  + " vs. proposed "
                  + fileId);
        }
      }

      fileId = composeFileId(id, intId);
      fileClassic = files.get(fileId);

      if (fileClassic != null) {
        if (!fileClassic.getName().equals(createInternalFileName(fileName, intId))) {
          throw new OStorageException(
              "File with given id exists but has different name "
                  + fileClassic.getName()
                  + " vs. proposed "
                  + fileName);
        }

        fileClassic.shrink(0);

        if (callFsync) {
          fileClassic.synch();
        }
      } else {
        fileClassic = createFileInstance(fileName, intId);
        createFile(fileClassic, callFsync);

        files.add(fileId, fileClassic);
      }

      idNameMap.remove(-intId);

      nameIdMap.put(fileName, intId);
      idNameMap.put(intId, fileName);

      writeNameIdEntry(new NameFileIdEntry(fileName, intId, fileClassic.getName()), true);

      return fileId;
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("File add was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public boolean checkLowDiskSpace() throws IOException {
    final long freeSpace = fileStore.getUsableSpace();
    return freeSpace < freeSpaceLimit;
  }

  @Override
  public void syncDataFiles(final long segmentId, final byte[] lastMetadata) throws IOException {
    filesLock.acquireReadLock();
    try {
      checkForClose();

      doubleWriteLog.startCheckpoint();
      try {
        if (lastMetadata != null) {
          writeAheadLog.log(new MetaDataRecord(lastMetadata));
        }

        for (final Integer intId : nameIdMap.values()) {
          if (intId < 0) {
            continue;
          }

          if (callFsync) {
            final long fileId = composeFileId(id, intId);
            final OClosableEntry<Long, OFile> entry = files.acquire(fileId);
            try {
              final OFile fileClassic = entry.get();
              fileClassic.synch();
            } finally {
              files.release(entry);
            }
          }
        }

        writeAheadLog.flush();

        writeAheadLog.cutAllSegmentsSmallerThan(segmentId);

        doubleWriteLog.truncate();
      } finally {
        doubleWriteLog.endCheckpoint();
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Fuzzy checkpoint was interrupted"), e);
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void flushTillSegment(final long segmentId) {
    final Future<Void> future = commitExecutor.submit(new FlushTillSegmentTask(segmentId));
    try {
      future.get();
    } catch (final Exception e) {
      throw ODatabaseException.wrapException(new OStorageException("Error during data flush"), e);
    }
  }

  @Override
  public boolean exists(final String fileName) {
    filesLock.acquireReadLock();
    try {
      checkForClose();

      final Integer intId = nameIdMap.get(fileName);
      if (intId != null && intId >= 0) {
        final OFile fileClassic = files.get(externalFileId(intId));

        if (fileClassic == null) {
          return false;
        }
        return fileClassic.exists();
      }
      return false;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public boolean exists(long fileId) {
    filesLock.acquireReadLock();
    try {
      checkForClose();

      final int intId = extractFileId(fileId);
      fileId = composeFileId(id, intId);

      final OFile file = files.get(fileId);
      if (file == null) {
        return false;
      }
      return file.exists();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void restoreModeOn() throws IOException {
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      doubleWriteLog.restoreModeOn();
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void restoreModeOff() {
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      doubleWriteLog.restoreModeOff();
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void checkCacheOverflow() throws InterruptedException {
    while (exclusiveWriteCacheSize.get() > exclusiveWriteCacheMaxSize) {
      final CountDownLatch cacheBoundaryLatch = new CountDownLatch(1);
      final CountDownLatch completionLatch = new CountDownLatch(1);
      final ExclusiveFlushTask exclusiveFlushTask =
          new ExclusiveFlushTask(cacheBoundaryLatch, completionLatch);

      triggeredTasks.put(exclusiveFlushTask, completionLatch);
      commitExecutor.submit(exclusiveFlushTask);

      cacheBoundaryLatch.await();
    }
  }

  @Override
  public void store(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      checkForClose();

      final PageKey pageKey = new PageKey(intId, pageIndex);

      final Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
      try {
        final OCachePointer pagePointer = writeCachePages.get(pageKey);

        if (pagePointer == null) {
          doPutInCache(dataPointer, pageKey);
        } else {
          assert pagePointer.equals(dataPointer);
        }

      } finally {
        groupLock.unlock();
      }

    } finally {
      filesLock.releaseReadLock();
    }
  }

  private void doPutInCache(final OCachePointer dataPointer, final PageKey pageKey) {
    writeCachePages.put(pageKey, dataPointer);

    writeCacheSize.incrementAndGet();

    dataPointer.setWritersListener(this);
    dataPointer.incrementWritersReferrer();
  }

  @Override
  public Map<String, Long> files() {
    filesLock.acquireReadLock();
    try {
      checkForClose();

      final Map<String, Long> result = new HashMap<>(1_000);

      for (final Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
        if (entry.getValue() > 0) {
          result.put(entry.getKey(), composeFileId(id, entry.getValue()));
        }
      }

      return result;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public OCachePointer load(
      final long fileId,
      final long startPageIndex,
      final OModifiableBoolean cacheHit,
      final boolean verifyChecksums)
      throws IOException {
    final int intId = extractFileId(fileId);
    filesLock.acquireReadLock();
    try {
      checkForClose();

      final PageKey pageKey = new PageKey(intId, startPageIndex);
      final Lock pageLock = lockManager.acquireSharedLock(pageKey);

      // check if page already presented in write cache
      final OCachePointer pagePointer = writeCachePages.get(pageKey);

      // page is not cached load it from file
      if (pagePointer == null) {
        try {
          // load requested page and preload requested amount of pages
          final OCachePointer filePagePointer =
              loadFileContent(intId, startPageIndex, verifyChecksums);
          if (filePagePointer != null) {
            filePagePointer.incrementReadersReferrer();
          }

          return filePagePointer;
        } finally {
          pageLock.unlock();
        }
      }

      pagePointer.incrementReadersReferrer();
      pageLock.unlock();

      cacheHit.setValue(true);

      return pagePointer;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public int allocateNewPage(final long fileId) throws IOException {
    filesLock.acquireReadLock();
    try {
      checkForClose();

      final OClosableEntry<Long, OFile> entry = files.acquire(fileId);
      try {
        final OFile fileClassic = entry.get();
        final long allocatedPosition = fileClassic.allocateSpace(pageSize);
        final long allocationIndex = allocatedPosition / pageSize;

        final int pageIndex = (int) allocationIndex;
        if (pageIndex < 0) {
          throw new IllegalStateException("Illegal page index value " + pageIndex);
        }

        return pageIndex;
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(
          new OStorageException("Allocation of page was interrupted"), e);
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void addOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.incrementAndGet();
    exclusiveWritePages.add(new PageKey(extractFileId(fileId), pageIndex));
  }

  @Override
  public void removeOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.decrementAndGet();
    exclusiveWritePages.remove(new PageKey(extractFileId(fileId), pageIndex));
  }

  @Override
  public void flush(final long fileId) {
    final Future<Void> future =
        commitExecutor.submit(new FileFlushTask(Collections.singleton(extractFileId(fileId))));
    try {
      future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw OException.wrapException(new OInterruptedException("File flush was interrupted"), e);
    } catch (final Exception e) {
      throw OException.wrapException(
          new OWriteCacheException("File flush was abnormally terminated"), e);
    }
  }

  @Override
  public void flush() {

    final Future<Void> future = commitExecutor.submit(new FileFlushTask(nameIdMap.values()));
    try {
      future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw OException.wrapException(new OInterruptedException("File flush was interrupted"), e);
    } catch (final Exception e) {
      throw OException.wrapException(
          new OWriteCacheException("File flush was abnormally terminated"), e);
    }
  }

  @Override
  public long getFilledUpTo(long fileId) {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireReadLock();
    try {
      checkForClose();

      final OClosableEntry<Long, OFile> entry = files.acquire(fileId);
      try {
        return entry.get().getFileSize() / pageSize;
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(
          new OStorageException("Calculation of file size was interrupted"), e);
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public long getExclusiveWriteCachePagesSize() {
    return exclusiveWriteCacheSize.get();
  }

  @Override
  public void deleteFile(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireWriteLock();
    try {
      checkForClose();

      final ORawPair<String, String> file;
      final Future<ORawPair<String, String>> future =
          commitExecutor.submit(new DeleteFileTask(fileId));
      try {
        file = future.get();
      } catch (final InterruptedException e) {
        throw OException.wrapException(
            new OInterruptedException("File data removal was interrupted"), e);
      } catch (final Exception e) {
        throw OException.wrapException(
            new OWriteCacheException("File data removal was abnormally terminated"), e);
      }

      if (file != null) {
        writeNameIdEntry(new NameFileIdEntry(file.first, -intId, file.second), true);
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      checkForClose();

      removeCachedPages(intId);
      final OClosableEntry<Long, OFile> entry = files.acquire(fileId);
      try {
        entry.get().shrink(0);
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("File truncation was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public boolean fileIdsAreEqual(final long firsId, final long secondId) {
    final int firstIntId = extractFileId(firsId);
    final int secondIntId = extractFileId(secondId);

    return firstIntId == secondIntId;
  }

  @Override
  public void renameFile(long fileId, final String newFileName) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      checkForClose();

      final OClosableEntry<Long, OFile> entry = files.acquire(fileId);

      if (entry == null) {
        return;
      }

      final String oldOsFileName;
      final String newOsFileName = createInternalFileName(newFileName, intId);

      try {
        final OFile file = entry.get();
        oldOsFileName = file.getName();

        final Path newFile = storagePath.resolve(newOsFileName);
        file.renameTo(newFile);
      } finally {
        files.release(entry);
      }

      final String oldFileName = idNameMap.get(intId);

      nameIdMap.remove(oldFileName);
      nameIdMap.put(newFileName, intId);

      idNameMap.put(intId, newFileName);

      writeNameIdEntry(new NameFileIdEntry(oldFileName, -1, oldOsFileName), false);
      writeNameIdEntry(new NameFileIdEntry(newFileName, intId, newOsFileName), true);
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Rename of file was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public void replaceFileId(final long fileId, final long newFileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      final OFile file = files.remove(fileId);
      final OFile newFile = files.remove(newFileId);

      final int intFileId = extractFileId(fileId);
      final int newIntFileId = extractFileId(newFileId);

      final String fileName = idNameMap.get(intFileId);
      final String newFileName = idNameMap.remove(newIntFileId);

      if (!file.isOpen()) {
        file.open();
      }
      if (!newFile.isOpen()) {
        newFile.open();
      }

      // invalidate old entries
      writeNameIdEntry(new NameFileIdEntry(fileName, 0, ""), false);
      writeNameIdEntry(new NameFileIdEntry(newFileName, 0, ""), false);

      // add new one
      writeNameIdEntry(new NameFileIdEntry(newFileName, intFileId, file.getName()), true);

      file.delete();

      files.add(fileId, newFile);

      idNameMap.put(intFileId, newFileName);
      nameIdMap.remove(fileName);
      nameIdMap.put(newFileName, intFileId);
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Replace of file was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  private void stopFlush() {
    stopFlush = true;

    for (final CountDownLatch completionLatch : triggeredTasks.values()) {
      try {
        if (!completionLatch.await(shutdownTimeout, TimeUnit.MINUTES)) {
          throw new OWriteCacheException("Can not shutdown data flush for storage " + storageName);
        }
      } catch (final InterruptedException e) {
        throw OException.wrapException(
            new OWriteCacheException(
                "Flush of the data for storage " + storageName + " has been interrupted"),
            e);
      }
    }

    if (flushFuture != null) {
      try {
        flushFuture.get(shutdownTimeout, TimeUnit.MINUTES);
      } catch (final InterruptedException | CancellationException e) {
        // ignore
      } catch (final ExecutionException e) {
        throw OException.wrapException(
            new OWriteCacheException("Error in execution of data flush for storage " + storageName),
            e);
      } catch (final TimeoutException e) {
        throw OException.wrapException(
            new OWriteCacheException("Can not shutdown data flush for storage " + storageName), e);
      }
    }
  }

  @Override
  public long[] close() throws IOException {
    flush();
    stopFlush();

    filesLock.acquireWriteLock();
    try {
      if (closed) {
        return new long[0];
      }

      closed = true;

      final Collection<Integer> fileIds = nameIdMap.values();

      final List<Long> closedIds = new ArrayList<>(1_000);
      final Map<Integer, String> idFileNameMap = new HashMap<>(1_000);

      for (final Integer intId : fileIds) {
        if (intId >= 0) {
          final long extId = composeFileId(id, intId);
          final OFile fileClassic = files.remove(extId);

          idFileNameMap.put(intId, fileClassic.getName());
          fileClassic.close();
          closedIds.add(extId);
        }
      }

      final Path nameIdMapBackupPath = storagePath.resolve(NAME_ID_MAP_V2_BACKUP);
      try (final FileChannel nameIdMapHolder =
          FileChannel.open(
              nameIdMapBackupPath,
              StandardOpenOption.CREATE,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE)) {
        nameIdMapHolder.truncate(0);

        for (final Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
          final String fileName;

          if (entry.getValue() >= 0) {
            fileName = idFileNameMap.get(entry.getValue());
          } else {
            fileName = entry.getKey();
          }

          writeNameIdEntry(
              nameIdMapHolder,
              new NameFileIdEntry(entry.getKey(), entry.getValue(), fileName),
              false);
        }

        nameIdMapHolder.force(true);
      }

      try {
        Files.move(
            nameIdMapBackupPath,
            nameIdMapHolderPath,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(nameIdMapBackupPath, nameIdMapHolderPath, StandardCopyOption.REPLACE_EXISTING);
      }

      doubleWriteLog.close();

      nameIdMap.clear();
      idNameMap.clear();

      final long[] ids = new long[closedIds.size()];
      int n = 0;

      for (final long id : closedIds) {
        ids[n] = id;
        n++;
      }

      return ids;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  private void checkForClose() {
    if (closed) {
      throw new OStorageException("Write cache is closed and can not be used");
    }
  }

  @Override
  public void close(long fileId, final boolean flush) {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      checkForClose();

      if (flush) {
        flush(intId);
      } else {
        removeCachedPages(intId);
      }

      if (!files.close(fileId)) {
        throw new OStorageException(
            "Can not close file with id " + internalFileId(fileId) + " because it is still in use");
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public String restoreFileById(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      for (final Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
        if (entry.getValue() == -intId) {
          addFile(entry.getKey(), fileId);
          return entry.getKey();
        }
      }
    } finally {
      filesLock.releaseWriteLock();
    }

    return null;
  }

  @Override
  public OPageDataVerificationError[] checkStoredPages(
      final OCommandOutputListener commandOutputListener) {
    final int notificationTimeOut = 5000;

    final List<OPageDataVerificationError> errors = new ArrayList<>(0);

    filesLock.acquireWriteLock();
    try {
      checkForClose();

      for (final Integer intId : nameIdMap.values()) {
        if (intId < 0) {
          continue;
        }

        checkFileStoredPages(commandOutputListener, notificationTimeOut, errors, intId);
      }

      return errors.toArray(new OPageDataVerificationError[0]);
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  private void checkFileStoredPages(
      final OCommandOutputListener commandOutputListener,
      @SuppressWarnings("SameParameterValue") final int notificationTimeOut,
      final List<OPageDataVerificationError> errors,
      final Integer intId)
      throws InterruptedException {
    boolean fileIsCorrect;
    final long externalId = composeFileId(id, intId);
    final OClosableEntry<Long, OFile> entry = files.acquire(externalId);
    final OFile fileClassic = entry.get();
    final String fileName = idNameMap.get(intId);

    try {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage("Flashing file " + fileName + "... ");
      }

      flush(intId);

      if (commandOutputListener != null) {
        commandOutputListener.onMessage(
            "Start verification of content of " + fileName + "file ...\n");
      }

      long time = System.currentTimeMillis();

      final long filledUpTo = fileClassic.getFileSize();
      fileIsCorrect = true;

      for (long pos = 0; pos < filledUpTo; pos += pageSize) {
        boolean checkSumIncorrect = false;
        boolean magicNumberIncorrect = false;

        final byte[] data = new byte[pageSize];

        final OPointer pointer = bufferPool.acquireDirect(true, Intention.CHECK_FILE_STORAGE);
        try {
          final ByteBuffer byteBuffer = pointer.getNativeByteBuffer();
          fileClassic.read(pos, byteBuffer, true);
          byteBuffer.rewind();
          byteBuffer.get(data);
        } finally {
          bufferPool.release(pointer);
        }

        final long magicNumber =
            OLongSerializer.INSTANCE.deserializeNative(data, MAGIC_NUMBER_OFFSET);

        if (magicNumber != MAGIC_NUMBER_WITH_CHECKSUM
            && magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM
            && magicNumber != MAGIC_NUMBER_WITH_CHECKSUM_ENCRYPTED
            && magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM_ENCRYPTED) {
          magicNumberIncorrect = true;
          if (commandOutputListener != null) {
            commandOutputListener.onMessage(
                "Error: Magic number for page "
                    + (pos / pageSize)
                    + " in file '"
                    + fileName
                    + "' does not match!\n");
          }
          fileIsCorrect = false;
        }

        if (magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM) {
          final int storedCRC32 =
              OIntegerSerializer.INSTANCE.deserializeNative(data, CHECKSUM_OFFSET);

          final CRC32 crc32 = new CRC32();
          crc32.update(
              data, PAGE_OFFSET_TO_CHECKSUM_FROM, data.length - PAGE_OFFSET_TO_CHECKSUM_FROM);
          final int calculatedCRC32 = (int) crc32.getValue();

          if (storedCRC32 != calculatedCRC32) {
            checkSumIncorrect = true;
            if (commandOutputListener != null) {
              commandOutputListener.onMessage(
                  "Error: Checksum for page "
                      + (pos / pageSize)
                      + " in file '"
                      + fileName
                      + "' is incorrect!\n");
            }
            fileIsCorrect = false;
          }
        }

        if (magicNumberIncorrect || checkSumIncorrect) {
          errors.add(
              new OPageDataVerificationError(
                  magicNumberIncorrect, checkSumIncorrect, pos / pageSize, fileName));
        }

        if (commandOutputListener != null
            && System.currentTimeMillis() - time > notificationTimeOut) {
          time = notificationTimeOut;
          commandOutputListener.onMessage((pos / pageSize) + " pages were processed...\n");
        }
      }
    } catch (final IOException ioe) {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage(
            "Error: Error during processing of file '"
                + fileName
                + "'. "
                + ioe.getMessage()
                + "\n");
      }

      fileIsCorrect = false;
    } finally {
      files.release(entry);
    }

    if (!fileIsCorrect) {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage(
            "Verification of file '" + fileName + "' is finished with errors.\n");
      }
    } else {
      if (commandOutputListener != null) {
        commandOutputListener.onMessage(
            "Verification of file '" + fileName + "' is successfully finished.\n");
      }
    }
  }

  @Override
  public long[] delete() throws IOException {
    final List<Long> result = new ArrayList<>(1_024);
    filesLock.acquireWriteLock();
    try {
      checkForClose();

      for (final int internalFileId : nameIdMap.values()) {
        if (internalFileId < 0) {
          continue;
        }

        final long externalId = composeFileId(id, internalFileId);

        final ORawPair<String, String> file;
        final Future<ORawPair<String, String>> future =
            commitExecutor.submit(new DeleteFileTask(externalId));
        try {
          file = future.get();
        } catch (final InterruptedException e) {
          throw OException.wrapException(
              new OInterruptedException("File data removal was interrupted"), e);
        } catch (final Exception e) {
          throw OException.wrapException(
              new OWriteCacheException("File data removal was abnormally terminated"), e);
        }

        if (file != null) {
          result.add(externalId);
        }
      }

      if (nameIdMapHolderPath != null) {
        if (Files.exists(nameIdMapHolderPath)) {
          Files.delete(nameIdMapHolderPath);
        }

        nameIdMapHolderPath = null;
      }
    } finally {
      filesLock.releaseWriteLock();
    }

    stopFlush();

    final long[] fIds = new long[result.size()];
    int n = 0;
    for (final Long fid : result) {
      fIds[n] = fid;
      n++;
    }

    doubleWriteLog.close();

    return fIds;
  }

  @Override
  public String fileNameById(final long fileId) {
    final int intId = extractFileId(fileId);

    return idNameMap.get(intId);
  }

  @Override
  public String nativeFileNameById(final long fileId) {
    final OFile fileClassic = files.get(fileId);
    if (fileClassic != null) {
      return fileClassic.getName();
    }

    return null;
  }

  @Override
  public int getId() {
    return id;
  }

  private static void openFile(final OFile fileClassic) {
    if (fileClassic.exists()) {
      if (!fileClassic.isOpen()) {
        fileClassic.open();
      }
    } else {
      throw new OStorageException("File " + fileClassic + " does not exist.");
    }
  }

  private static void createFile(final OFile fileClassic, final boolean callFsync)
      throws IOException {
    if (!fileClassic.exists()) {
      fileClassic.create();
    } else {
      if (!fileClassic.isOpen()) {
        fileClassic.open();
      }
      fileClassic.shrink(0);
    }

    if (callFsync) {
      fileClassic.synch();
    }
  }

  private void initNameIdMapping() throws IOException, InterruptedException {
    if (!Files.exists(storagePath)) {
      Files.createDirectories(storagePath);
    }

    final Path nameIdMapHolderV1 = storagePath.resolve(NAME_ID_MAP_V1);
    final Path nameIdMapHolderV2 = storagePath.resolve(NAME_ID_MAP_V2);
    final Path nameIdMapHolderV3 = storagePath.resolve(NAME_ID_MAP_V3);

    if (Files.exists(nameIdMapHolderV1)) {
      if (Files.exists(nameIdMapHolderV2)) {
        Files.delete(nameIdMapHolderV2);
      }
      if (Files.exists(nameIdMapHolderV3)) {
        Files.delete(nameIdMapHolderV3);
      }

      try (final FileChannel nameIdMapHolder =
          FileChannel.open(nameIdMapHolderV1, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
        readNameIdMapV1(nameIdMapHolder);
      }

      Files.delete(nameIdMapHolderV1);
    } else if (Files.exists(nameIdMapHolderV2)) {
      if (Files.exists(nameIdMapHolderV3)) {
        Files.delete(nameIdMapHolderV3);
      }

      try (final FileChannel nameIdMapHolder =
          FileChannel.open(nameIdMapHolderV2, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
        readNameIdMapV2(nameIdMapHolder);
      }

      Files.delete(nameIdMapHolderV2);
    }

    nameIdMapHolderPath = nameIdMapHolderV3;
    if (Files.exists(nameIdMapHolderPath)) {
      try (final FileChannel nameIdMapHolder =
          FileChannel.open(
              nameIdMapHolderPath, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
        readNameIdMapV3(nameIdMapHolder);
      }
    } else {
      storedNameIdMapToV3();
    }
  }

  private void storedNameIdMapToV3() throws IOException {
    final Path nameIdMapHolderFileV3T = storagePath.resolve(NAME_ID_MAP_V3_T);

    if (Files.exists(nameIdMapHolderFileV3T)) {
      Files.delete(nameIdMapHolderFileV3T);
    }

    final FileChannel v3NameIdMapHolder =
        FileChannel.open(
            nameIdMapHolderFileV3T,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ);

    for (final Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0) {
        final OFile fileClassic = files.get(externalFileId(nameIdEntry.getValue()));
        final String fileSystemName = fileClassic.getName();

        final NameFileIdEntry nameFileIdEntry =
            new NameFileIdEntry(nameIdEntry.getKey(), nameIdEntry.getValue(), fileSystemName);
        writeNameIdEntry(v3NameIdMapHolder, nameFileIdEntry, false);
      } else {
        final NameFileIdEntry nameFileIdEntry =
            new NameFileIdEntry(nameIdEntry.getKey(), nameIdEntry.getValue(), "");
        writeNameIdEntry(v3NameIdMapHolder, nameFileIdEntry, false);
      }
    }

    v3NameIdMapHolder.force(true);
    v3NameIdMapHolder.close();

    try {
      Files.move(
          nameIdMapHolderFileV3T,
          storagePath.resolve(NAME_ID_MAP_V3),
          StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(nameIdMapHolderFileV3T, storagePath.resolve(NAME_ID_MAP_V3));
    }
  }

  private OFile createFileInstance(final String fileName, final int fileId) {
    final String internalFileName = createInternalFileName(fileName, fileId);
    return new AsyncFile(storagePath.resolve(internalFileName), pageSize);
  }

  private static String createInternalFileName(final String fileName, final int fileId) {
    final int extSeparator = fileName.lastIndexOf('.');

    String prefix;
    if (extSeparator < 0) {
      prefix = fileName;
    } else if (extSeparator == 0) {
      prefix = "";
    } else {
      prefix = fileName.substring(0, extSeparator);
    }

    final String suffix;
    if (extSeparator < 0 || extSeparator == fileName.length() - 1) {
      suffix = "";
    } else {
      suffix = fileName.substring(extSeparator + 1);
    }

    prefix = prefix + "_" + fileId;

    if (extSeparator >= 0) {
      return prefix + "." + suffix;
    }

    return prefix;
  }

  /**
   * Read information about files are registered inside of write cache/storage File consist of rows
   * of variable length which contains following entries:
   *
   * <ol>
   *   <li>XX_HASH code of the content of the row excluding first two entries.
   *   <li>Length of the content of the row excluding of two entries above.
   *   <li>Internal file id, may be positive or negative depends on whether file is removed or not
   *   <li>Name of file inside of write cache, this name is case sensitive
   *   <li>Name of file which is used inside file system it can be different from name of file used
   *       inside write cache
   * </ol>
   */
  private void readNameIdMapV3(FileChannel nameIdMapHolder)
      throws IOException, InterruptedException {
    nameIdMap.clear();

    long localFileCounter = -1;

    nameIdMapHolder.position(0);

    NameFileIdEntry nameFileIdEntry;

    final Map<Integer, String> idFileNameMap = new HashMap<>(1_000);

    while ((nameFileIdEntry = readNextNameIdEntryV3(nameIdMapHolder)) != null) {
      final long absFileId = Math.abs(nameFileIdEntry.fileId);

      if (localFileCounter < absFileId) {
        localFileCounter = absFileId;
      }

      if (absFileId != 0) {
        nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
        idNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.name);

        idFileNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.fileSystemName);
      } else {
        nameIdMap.remove(nameFileIdEntry.name);
        idNameMap.remove(nameFileIdEntry.fileId);
        idFileNameMap.remove(nameFileIdEntry.fileId);
      }
    }

    for (final Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      final int fileId = nameIdEntry.getValue();

      if (fileId >= 0) {
        final long externalId = composeFileId(id, nameIdEntry.getValue());

        if (files.get(externalId) == null) {
          final Path path = storagePath.resolve(idFileNameMap.get((nameIdEntry.getValue())));
          final AsyncFile file = new AsyncFile(path, pageSize);

          if (file.exists()) {
            file.open();
            files.add(externalId, file);
          } else {
            idNameMap.remove(fileId);

            nameIdMap.put(nameIdEntry.getKey(), -fileId);
            idNameMap.put(-fileId, nameIdEntry.getKey());
          }
        }
      }
    }
  }

  /**
   * Read information about files are registered inside of write cache/storage File consist of rows
   * of variable length which contains following entries:
   *
   * <ol>
   *   <li>Internal file id, may be positive or negative depends on whether file is removed or not
   *   <li>Name of file inside of write cache, this name is case sensitive
   *   <li>Name of file which is used inside file system it can be different from name of file used
   *       inside write cache
   * </ol>
   */
  private void readNameIdMapV2(FileChannel nameIdMapHolder)
      throws IOException, InterruptedException {
    nameIdMap.clear();

    long localFileCounter = -1;

    nameIdMapHolder.position(0);

    NameFileIdEntry nameFileIdEntry;

    final Map<Integer, String> idFileNameMap = new HashMap<>(1_000);

    while ((nameFileIdEntry = readNextNameIdEntryV2(nameIdMapHolder)) != null) {
      final long absFileId = Math.abs(nameFileIdEntry.fileId);

      if (localFileCounter < absFileId) {
        localFileCounter = absFileId;
      }

      if (absFileId != 0) {
        nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
        idNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.name);

        idFileNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.fileSystemName);
      } else {
        nameIdMap.remove(nameFileIdEntry.name);
        idNameMap.remove(nameFileIdEntry.fileId);

        idFileNameMap.remove(nameFileIdEntry.fileId);
      }
    }

    for (final Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      final int fileId = nameIdEntry.getValue();

      if (fileId > 0) {
        final long externalId = composeFileId(id, nameIdEntry.getValue());

        if (files.get(externalId) == null) {
          final Path path = storagePath.resolve(idFileNameMap.get((nameIdEntry.getValue())));
          final AsyncFile file = new AsyncFile(path, pageSize);

          if (file.exists()) {
            file.open();
            files.add(externalId, file);
          } else {
            idNameMap.remove(fileId);

            nameIdMap.put(nameIdEntry.getKey(), -fileId);
            idNameMap.put(-fileId, nameIdEntry.getKey());
          }
        }
      }
    }
  }

  private void readNameIdMapV1(final FileChannel nameIdMapHolder)
      throws IOException, InterruptedException {
    // older versions of ODB incorrectly logged file deletions
    // some deleted files have the same id
    // because we reuse ids of removed files when we re-create them
    // we need to fix this situation
    final Map<Integer, Set<String>> filesWithNegativeIds = new HashMap<>(1_000);

    nameIdMap.clear();

    long localFileCounter = -1;

    nameIdMapHolder.position(0);

    NameFileIdEntry nameFileIdEntry;
    while ((nameFileIdEntry = readNextNameIdEntryV1(nameIdMapHolder)) != null) {

      final long absFileId = Math.abs(nameFileIdEntry.fileId);
      if (localFileCounter < absFileId) {
        localFileCounter = absFileId;
      }

      final Integer existingId = nameIdMap.get(nameFileIdEntry.name);

      if (existingId != null && existingId < 0) {
        final Set<String> files = filesWithNegativeIds.get(existingId);

        if (files != null) {
          files.remove(nameFileIdEntry.name);

          if (files.isEmpty()) {
            filesWithNegativeIds.remove(existingId);
          }
        }
      }

      if (nameFileIdEntry.fileId < 0) {
        Set<String> files = filesWithNegativeIds.get(nameFileIdEntry.fileId);

        if (files == null) {
          files = new HashSet<>(8);
          files.add(nameFileIdEntry.name);
          filesWithNegativeIds.put(nameFileIdEntry.fileId, files);
        } else {
          files.add(nameFileIdEntry.name);
        }
      }

      nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
      idNameMap.put(nameFileIdEntry.fileId, nameFileIdEntry.name);
    }

    for (final Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0) {
        final long externalId = composeFileId(id, nameIdEntry.getValue());

        if (files.get(externalId) == null) {
          final OFile fileClassic =
              new AsyncFile(storagePath.resolve(nameIdEntry.getKey()), pageSize);

          if (fileClassic.exists()) {
            fileClassic.open();
            files.add(externalId, fileClassic);
          } else {
            final Integer fileId = nameIdMap.get(nameIdEntry.getKey());

            if (fileId != null && fileId > 0) {
              nameIdMap.put(nameIdEntry.getKey(), -fileId);

              idNameMap.remove(fileId);
              idNameMap.put(-fileId, nameIdEntry.getKey());
            }
          }
        }
      }
    }

    final Set<String> fixedFiles = new HashSet<>(8);

    for (final Map.Entry<Integer, Set<String>> entry : filesWithNegativeIds.entrySet()) {
      final Set<String> files = entry.getValue();

      if (files.size() > 1) {
        idNameMap.remove(entry.getKey());

        for (final String fileName : files) {
          int fileId;

          while (true) {
            final int nextId = fileIdGen.nextInt(Integer.MAX_VALUE - 1) + 1;
            if (!idNameMap.containsKey(nextId) && !idNameMap.containsKey(-nextId)) {
              fileId = nextId;
              break;
            }
          }

          nameIdMap.put(fileName, -fileId);
          idNameMap.put(-fileId, fileName);

          fixedFiles.add(fileName);
        }
      }
    }

    if (!fixedFiles.isEmpty()) {
      OLogManager.instance()
          .warn(
              this,
              "Removed files "
                  + fixedFiles
                  + " had duplicated ids. Problem is fixed automatically.");
    }
  }

  private NameFileIdEntry readNextNameIdEntryV1(FileChannel nameIdMapHolder) throws IOException {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final int nameSize = buffer.getInt();
      buffer = ByteBuffer.allocate(nameSize + OLongSerializer.LONG_SIZE);

      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final String name = stringSerializer.deserializeFromByteBufferObject(buffer);
      final int fileId = (int) buffer.getLong();

      return new NameFileIdEntry(name, fileId);
    } catch (final EOFException ignore) {
      return null;
    }
  }

  private NameFileIdEntry readNextNameIdEntryV2(FileChannel nameIdMapHolder) throws IOException {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(2 * OIntegerSerializer.INT_SIZE);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final int fileId = buffer.getInt();
      final int nameSize = buffer.getInt();

      buffer = ByteBuffer.allocate(nameSize);

      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final String name = stringSerializer.deserializeFromByteBufferObject(buffer);

      buffer = ByteBuffer.allocate(OIntegerSerializer.INT_SIZE);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final int fileNameSize = buffer.getInt();

      buffer = ByteBuffer.allocate(fileNameSize);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final String fileName = stringSerializer.deserializeFromByteBufferObject(buffer);

      return new NameFileIdEntry(name, fileId, fileName);

    } catch (final EOFException ignore) {
      return null;
    }
  }

  private NameFileIdEntry readNextNameIdEntryV3(FileChannel nameIdMapHolder) throws IOException {
    try {
      final int xxHashLen = 8;
      final int recordSizeLen = 4;

      ByteBuffer buffer = ByteBuffer.allocate(xxHashLen + recordSizeLen);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final long storedXxHash = buffer.getLong();
      final int recordLen = buffer.getInt();

      if (recordLen > MAX_FILE_RECORD_LEN) {
        OLogManager.instance()
            .errorNoDb(
                this,
                "Maximum record length in file registry can not exceed %d bytes. "
                    + "But actual record length %d.  Storage name : %s",
                null,
                MAX_FILE_RECORD_LEN,
                storageName,
                recordLen);
        return null;
      }

      buffer = ByteBuffer.allocate(recordLen);
      OIOUtils.readByteBuffer(buffer, nameIdMapHolder);
      buffer.rewind();

      final long xxHash = XX_HASH_64.hash(buffer, 0, recordLen, XX_HASH_SEED);
      if (xxHash != storedXxHash) {
        OLogManager.instance()
            .errorNoDb(
                this, "Hash of the file registry is broken. Storage name : %s", null, storageName);
        return null;
      }

      final int fileId = buffer.getInt();
      final String name = stringSerializer.deserializeFromByteBufferObject(buffer);
      final String fileName = stringSerializer.deserializeFromByteBufferObject(buffer);

      return new NameFileIdEntry(name, fileId, fileName);

    } catch (final EOFException ignore) {
      return null;
    }
  }

  private void writeNameIdEntry(final NameFileIdEntry nameFileIdEntry, final boolean sync)
      throws IOException {
    try (final FileChannel nameIdMapHolder =
        FileChannel.open(nameIdMapHolderPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      writeNameIdEntry(nameIdMapHolder, nameFileIdEntry, sync);
    }
  }

  private void writeNameIdEntry(
      final FileChannel nameIdMapHolder, final NameFileIdEntry nameFileIdEntry, final boolean sync)
      throws IOException {
    final int xxHashSize = 8;
    final int recordLenSize = 4;

    final int nameSize = stringSerializer.getObjectSize(nameFileIdEntry.name);
    final int fileNameSize = stringSerializer.getObjectSize(nameFileIdEntry.fileSystemName);

    // file id size + file name + file system name + xx_hash size + record_size size
    final ByteBuffer serializedRecord =
        ByteBuffer.allocate(
            OIntegerSerializer.INT_SIZE + nameSize + fileNameSize + xxHashSize + recordLenSize);

    serializedRecord.position(xxHashSize + recordLenSize);

    // serialize file id
    OIntegerSerializer.INSTANCE.serializeInByteBufferObject(
        nameFileIdEntry.fileId, serializedRecord);

    // serialize file name
    stringSerializer.serializeInByteBufferObject(nameFileIdEntry.name, serializedRecord);

    // serialize file system name
    stringSerializer.serializeInByteBufferObject(nameFileIdEntry.fileSystemName, serializedRecord);

    final int recordLen = serializedRecord.position() - xxHashSize - recordLenSize;
    if (recordLen > MAX_FILE_RECORD_LEN) {
      throw new OStorageException(
          "Maximum record length in file registry can not exceed "
              + MAX_FILE_RECORD_LEN
              + " bytes. But actual record length "
              + recordLen);
    }
    serializedRecord.putInt(xxHashSize, recordLen);

    final long xxHash =
        XX_HASH_64.hash(serializedRecord, xxHashSize + recordLenSize, recordLen, XX_HASH_SEED);
    serializedRecord.putLong(0, xxHash);

    serializedRecord.position(0);

    OIOUtils.writeByteBuffer(serializedRecord, nameIdMapHolder, nameIdMapHolder.size());
    nameIdMapHolder.write(serializedRecord);

    if (sync) {
      nameIdMapHolder.force(true);
    }
  }

  private void removeCachedPages(final int fileId) {
    final Future<Void> future = commitExecutor.submit(new RemoveFilePagesTask(fileId));
    try {
      future.get();
    } catch (final InterruptedException e) {
      throw OException.wrapException(
          new OInterruptedException("File data removal was interrupted"), e);
    } catch (final Exception e) {
      throw OException.wrapException(
          new OWriteCacheException("File data removal was abnormally terminated"), e);
    }
  }

  private OCachePointer loadFileContent(
      final int internalFileId, final long pageIndex, final boolean verifyChecksums)
      throws IOException {
    final long fileId = composeFileId(id, internalFileId);
    try {
      final OClosableEntry<Long, OFile> entry = files.acquire(fileId);
      try {
        final OFile fileClassic = entry.get();
        if (fileClassic == null) {
          throw new IllegalArgumentException(
              "File with id " + internalFileId + " not found in WOW Cache");
        }

        final long pagePosition = pageIndex * pageSize;
        final long pageEndPosition = pagePosition + pageSize;

        // if page is not stored in the file may be page is stored in double write log
        if (fileClassic.getFileSize() >= pageEndPosition) {
          OPointer pointer = bufferPool.acquireDirect(true, Intention.LOAD_PAGE_FROM_DISK);
          ByteBuffer buffer = pointer.getNativeByteBuffer();

          assert buffer.position() == 0;
          assert buffer.order() == ByteOrder.nativeOrder();

          fileClassic.read(pagePosition, buffer, false);

          if (verifyChecksums
              && (checksumMode == OChecksumMode.StoreAndVerify
                  || checksumMode == OChecksumMode.StoreAndThrow
                  || checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode)) {
            // if page is broken inside of data file we check double write log
            if (!verifyMagicChecksumAndDecryptPage(buffer, internalFileId, pageIndex)) {
              final OPointer doubleWritePointer =
                  doubleWriteLog.loadPage(internalFileId, (int) pageIndex, bufferPool);

              if (doubleWritePointer == null) {
                assertPageIsBroken(pageIndex, fileId, pointer);
              } else {
                bufferPool.release(pointer);

                buffer = doubleWritePointer.getNativeByteBuffer();
                assert buffer.position() == 0;
                pointer = doubleWritePointer;

                if (!verifyMagicChecksumAndDecryptPage(buffer, internalFileId, pageIndex)) {
                  assertPageIsBroken(pageIndex, fileId, pointer);
                }
              }
            }
          }

          buffer.position(0);
          return new OCachePointer(pointer, bufferPool, fileId, (int) pageIndex);
        } else {
          final OPointer pointer =
              doubleWriteLog.loadPage(internalFileId, (int) pageIndex, bufferPool);
          if (pointer != null) {
            final ByteBuffer buffer = pointer.getNativeByteBuffer();
            assert buffer.position() == 0;

            if (verifyChecksums
                && (checksumMode == OChecksumMode.StoreAndVerify
                    || checksumMode == OChecksumMode.StoreAndThrow
                    || checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode)) {
              if (!verifyMagicChecksumAndDecryptPage(buffer, internalFileId, pageIndex)) {
                assertPageIsBroken(pageIndex, fileId, pointer);
              }
            }
          }

          return null;
        }
      } finally {
        files.release(entry);
      }
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OStorageException("Data load was interrupted"), e);
    }
  }

  private void assertPageIsBroken(long pageIndex, long fileId, OPointer pointer) {
    final String message =
        "Magic number verification failed for page `"
            + pageIndex
            + "` of `"
            + fileNameById(fileId)
            + "`.";
    OLogManager.instance().error(this, "%s", null, message);

    if (checksumMode == OChecksumMode.StoreAndThrow) {
      bufferPool.release(pointer);
      throw new OStorageException(message);
    } else if (checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode) {
      dumpStackTrace(message);
      callPageIsBrokenListeners(fileNameById(fileId), pageIndex);
    }
  }

  private void addMagicChecksumAndEncryption(
      final int intId, final int pageIndex, final ByteBuffer buffer) {
    assert buffer.order() == ByteOrder.nativeOrder();

    if (checksumMode != OChecksumMode.Off) {
      buffer.position(PAGE_OFFSET_TO_CHECKSUM_FROM);
      final CRC32 crc32 = new CRC32();
      crc32.update(buffer);
      final int computedChecksum = (int) crc32.getValue();

      buffer.position(CHECKSUM_OFFSET);
      buffer.putInt(computedChecksum);
    }

    if (aesKey != null) {
      long magicNumber = buffer.getLong(MAGIC_NUMBER_OFFSET);

      long updateCounter = magicNumber >>> 8;
      updateCounter++;

      if (checksumMode == OChecksumMode.Off) {
        magicNumber = (updateCounter << 8) | MAGIC_NUMBER_WITHOUT_CHECKSUM_ENCRYPTED;
      } else {
        magicNumber = (updateCounter << 8) | MAGIC_NUMBER_WITH_CHECKSUM_ENCRYPTED;
      }

      buffer.putLong(MAGIC_NUMBER_OFFSET, magicNumber);
      doEncryptionDecryption(intId, pageIndex, Cipher.ENCRYPT_MODE, buffer, updateCounter);
    } else {
      buffer.putLong(
          MAGIC_NUMBER_OFFSET,
          checksumMode == OChecksumMode.Off
              ? MAGIC_NUMBER_WITHOUT_CHECKSUM
              : MAGIC_NUMBER_WITH_CHECKSUM);
    }
  }

  private void doEncryptionDecryption(
      final int intId,
      final int pageIndex,
      final int mode,
      final ByteBuffer buffer,
      final long updateCounter) {
    try {
      final Cipher cipher = CIPHER.get();
      final SecretKey aesKey = new SecretKeySpec(this.aesKey, ALGORITHM_NAME);

      final byte[] updatedIv = new byte[iv.length];

      for (int i = 0; i < OIntegerSerializer.INT_SIZE; i++) {
        updatedIv[i] = (byte) (iv[i] ^ ((pageIndex >>> i) & 0xFF));
      }

      for (int i = 0; i < OIntegerSerializer.INT_SIZE; i++) {
        updatedIv[i + OIntegerSerializer.INT_SIZE] =
            (byte) (iv[i + OIntegerSerializer.INT_SIZE] ^ ((intId >>> i) & 0xFF));
      }

      for (int i = 0; i < OLongSerializer.LONG_SIZE - 1; i++) {
        updatedIv[i + 2 * OIntegerSerializer.INT_SIZE] =
            (byte) (iv[i + 2 * OIntegerSerializer.INT_SIZE] ^ ((updateCounter >>> i) & 0xFF));
      }

      updatedIv[updatedIv.length - 1] = iv[iv.length - 1];

      cipher.init(mode, aesKey, new IvParameterSpec(updatedIv));

      final ByteBuffer outBuffer =
          ByteBuffer.allocate(buffer.capacity() - CHECKSUM_OFFSET).order(ByteOrder.nativeOrder());

      buffer.position(CHECKSUM_OFFSET);
      cipher.doFinal(buffer, outBuffer);

      buffer.position(CHECKSUM_OFFSET);
      outBuffer.position(0);
      buffer.put(outBuffer);

    } catch (InvalidKeyException e) {
      throw OException.wrapException(new OInvalidStorageEncryptionKeyException(e.getMessage()), e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalArgumentException("Invalid IV.", e);
    } catch (IllegalBlockSizeException | BadPaddingException | ShortBufferException e) {
      throw new IllegalStateException("Unexpected exception during CRT encryption.", e);
    }
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private boolean verifyMagicChecksumAndDecryptPage(
      final ByteBuffer buffer, final int intId, final long pageIndex) {
    assert buffer.order() == ByteOrder.nativeOrder();

    buffer.position(MAGIC_NUMBER_OFFSET);
    final long magicNumber = OLongSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    if ((aesKey == null && magicNumber != MAGIC_NUMBER_WITH_CHECKSUM)
        || (magicNumber != MAGIC_NUMBER_WITH_CHECKSUM
            && (magicNumber & 0xFF) != MAGIC_NUMBER_WITH_CHECKSUM_ENCRYPTED)) {
      if ((aesKey == null && magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM)
          || (magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM
              && (magicNumber & 0xFF) != MAGIC_NUMBER_WITHOUT_CHECKSUM_ENCRYPTED)) {
        return false;
      }

      if (aesKey != null && (magicNumber & 0xFF) == MAGIC_NUMBER_WITHOUT_CHECKSUM_ENCRYPTED) {
        doEncryptionDecryption(
            intId, (int) pageIndex, Cipher.DECRYPT_MODE, buffer, magicNumber >>> 8);
      }

      return true;
    }

    if (aesKey != null && (magicNumber & 0xFF) == MAGIC_NUMBER_WITH_CHECKSUM_ENCRYPTED) {
      doEncryptionDecryption(
          intId, (int) pageIndex, Cipher.DECRYPT_MODE, buffer, magicNumber >>> 8);
    }

    buffer.position(CHECKSUM_OFFSET);
    final int storedChecksum = OIntegerSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    buffer.position(PAGE_OFFSET_TO_CHECKSUM_FROM);
    final CRC32 crc32 = new CRC32();
    crc32.update(buffer);
    final int computedChecksum = (int) crc32.getValue();

    return computedChecksum == storedChecksum;
  }

  private void dumpStackTrace(final String message) {
    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);

    printWriter.println(message);
    final Exception exception = new Exception();
    exception.printStackTrace(printWriter);
    printWriter.flush();

    OLogManager.instance().error(this, stringWriter.toString(), null);
  }

  private void fsyncFiles() throws InterruptedException, IOException {
    for (int intFileId : idNameMap.keySet()) {
      if (intFileId >= 0) {
        final long extFileId = externalFileId(intFileId);

        final OClosableEntry<Long, OFile> fileEntry = files.acquire(extFileId);
        try {
          final OFile fileClassic = fileEntry.get();
          fileClassic.synch();
        } finally {
          files.release(fileEntry);
        }
      }
    }

    doubleWriteLog.truncate();
  }

  private void doRemoveCachePages(int internalFileId) {
    final Iterator<Map.Entry<PageKey, OCachePointer>> entryIterator =
        writeCachePages.entrySet().iterator();
    while (entryIterator.hasNext()) {
      final Map.Entry<PageKey, OCachePointer> entry = entryIterator.next();
      final PageKey pageKey = entry.getKey();

      if (pageKey.fileId == internalFileId) {
        final OCachePointer pagePointer = entry.getValue();
        final Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
        try {
          pagePointer.acquireExclusiveLock();
          try {
            pagePointer.decrementWritersReferrer();
            pagePointer.setWritersListener(null);
            writeCacheSize.decrementAndGet();

            removeFromDirtyPages(pageKey);
          } finally {
            pagePointer.releaseExclusiveLock();
          }

          entryIterator.remove();
        } finally {
          groupLock.unlock();
        }
      }
    }
  }

  public void setChecksumMode(final OChecksumMode checksumMode) { // for testing purposes only
    this.checksumMode = checksumMode;
  }

  private static final class NameFileIdEntry {
    private final String name;
    private final int fileId;
    private final String fileSystemName;

    private NameFileIdEntry(final String name, final int fileId) {
      this.name = name;
      this.fileId = fileId;
      this.fileSystemName = name;
    }

    private NameFileIdEntry(final String name, final int fileId, final String fileSystemName) {
      this.name = name;
      this.fileId = fileId;
      this.fileSystemName = fileSystemName;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final NameFileIdEntry that = (NameFileIdEntry) o;

      if (fileId != that.fileId) {
        return false;
      }
      if (!name.equals(that.name)) {
        return false;
      }
      return fileSystemName.equals(that.fileSystemName);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + fileId;
      result = 31 * result + fileSystemName.hashCode();
      return result;
    }
  }

  private static final class PageKey implements Comparable<PageKey> {
    private final int fileId;
    private final long pageIndex;

    private PageKey(final int fileId, final long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public int compareTo(final PageKey other) {
      if (fileId > other.fileId) {
        return 1;
      }
      if (fileId < other.fileId) {
        return -1;
      }

      return Long.compare(pageIndex, other.pageIndex);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final PageKey pageKey = (PageKey) o;

      if (fileId != pageKey.fileId) {
        return false;
      }
      return pageIndex == pageKey.pageIndex;
    }

    @Override
    public int hashCode() {
      int result = fileId;
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "PageKey{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }

    public PageKey previous() {
      return pageIndex == -1 ? this : new PageKey(fileId, pageIndex - 1);
    }
  }

  private final class FlushTillSegmentTask implements Callable<Void> {
    private final long segmentId;

    private FlushTillSegmentTask(final long segmentId) {
      this.segmentId = segmentId;
    }

    @Override
    public Void call() throws Exception {
      if (flushError != null) {
        OLogManager.instance()
            .errorNoDb(
                this,
                "Can not flush data till provided segment because of issue during data write, %s",
                null,
                flushError.getMessage());
        return null;
      }

      convertSharedDirtyPagesToLocal();
      Map.Entry<Long, TreeSet<PageKey>> firstEntry = localDirtyPagesBySegment.firstEntry();

      if (firstEntry == null) {
        return null;
      }

      long minDirtySegment = firstEntry.getKey();
      while (minDirtySegment < segmentId) {
        flushExclusivePagesIfNeeded();

        flushWriteCacheFromMinLSN(writeAheadLog.begin().getSegment(), segmentId, chunkSize);

        firstEntry = localDirtyPagesBySegment.firstEntry();

        if (firstEntry == null) {
          return null;
        }

        minDirtySegment = firstEntry.getKey();
      }

      return null;
    }

    private void flushExclusivePagesIfNeeded() throws InterruptedException, IOException {
      final long ewcSize = exclusiveWriteCacheSize.get();
      assert ewcSize >= 0;

      if (ewcSize >= 0.8 * exclusiveWriteCacheMaxSize) {
        flushExclusiveWriteCache(null, ewcSize);
      }
    }
  }

  private final class ExclusiveFlushTask implements Runnable {
    private final CountDownLatch cacheBoundaryLatch;
    private final CountDownLatch completionLatch;

    private ExclusiveFlushTask(
        final CountDownLatch cacheBoundaryLatch, final CountDownLatch completionLatch) {
      this.cacheBoundaryLatch = cacheBoundaryLatch;
      this.completionLatch = completionLatch;
    }

    @Override
    public void run() {
      if (stopFlush) {
        return;
      }

      try {
        if (flushError != null) {
          OLogManager.instance()
              .errorNoDb(
                  this,
                  "Can not flush data because of issue during data write, %s",
                  null,
                  flushError.getMessage());
          return;
        }

        if (writeCachePages.isEmpty()) {
          return;
        }

        final long ewcSize = exclusiveWriteCacheSize.get();

        assert ewcSize >= 0;

        if (cacheBoundaryLatch != null && ewcSize <= exclusiveWriteCacheMaxSize) {
          cacheBoundaryLatch.countDown();
        }

        if (ewcSize > exclusiveWriteCacheMaxSize) {
          flushExclusiveWriteCache(cacheBoundaryLatch, chunkSize);
        }

      } catch (final Error | Exception t) {
        OLogManager.instance().error(this, "Exception during data flush", t);
        OWOWCache.this.fireBackgroundDataFlushExceptionEvent(t);
        flushError = t;
      } finally {
        if (cacheBoundaryLatch != null) {
          cacheBoundaryLatch.countDown();
        }

        if (completionLatch != null) {
          completionLatch.countDown();
        }
      }
    }
  }

  private final class PeriodicFlushTask implements Runnable {

    @Override
    public void run() {
      if (stopFlush) {
        return;
      }

      long flushInterval = pagesFlushInterval;

      try {
        if (flushError != null) {
          OLogManager.instance()
              .errorNoDb(
                  this,
                  "Can not flush data because of issue during data write, %s",
                  null,
                  flushError.getMessage());
          return;
        }

        try {
          if (writeCachePages.isEmpty()) {
            return;
          }

          long ewcSize = exclusiveWriteCacheSize.get();
          if (ewcSize >= 0) {
            flushExclusiveWriteCache(null, Math.min(ewcSize, 4L * chunkSize));

            if (exclusiveWriteCacheSize.get() > 0) {
              flushInterval = 1;
            }
          }

          final OLogSequenceNumber begin = writeAheadLog.begin();
          final OLogSequenceNumber end = writeAheadLog.end();
          final long segments = end.getSegment() - begin.getSegment() + 1;

          if (segments > 1) {
            convertSharedDirtyPagesToLocal();

            Map.Entry<Long, TreeSet<PageKey>> firstSegment = localDirtyPagesBySegment.firstEntry();
            if (firstSegment != null) {
              final long firstSegmentIndex = firstSegment.getKey();
              if (firstSegmentIndex < end.getSegment()) {
                flushWriteCacheFromMinLSN(firstSegmentIndex, firstSegmentIndex + 1, chunkSize);
              }
            }

            firstSegment = localDirtyPagesBySegment.firstEntry();
            if (firstSegment != null && firstSegment.getKey() < end.getSegment()) {
              flushInterval = 1;
            }
          }
        } catch (final Error | Exception t) {
          OLogManager.instance().error(this, "Exception during data flush", t);
          OWOWCache.this.fireBackgroundDataFlushExceptionEvent(t);
          flushError = t;
        }
      } finally {
        if (flushInterval > 0 && !stopFlush) {
          flushFuture = commitExecutor.schedule(this, flushInterval, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  final class FindMinDirtySegment implements Callable<Long> {
    @Override
    public Long call() {
      if (flushError != null) {
        OLogManager.instance()
            .errorNoDb(
                this,
                "Can not calculate minimum LSN because of issue during data write, %s",
                null,
                flushError.getMessage());
        return null;
      }

      convertSharedDirtyPagesToLocal();

      if (localDirtyPagesBySegment.isEmpty()) {
        return null;
      }

      return localDirtyPagesBySegment.firstKey();
    }
  }

  private void convertSharedDirtyPagesToLocal() {
    for (final Map.Entry<PageKey, OLogSequenceNumber> entry : dirtyPages.entrySet()) {
      final OLogSequenceNumber localLSN = localDirtyPages.get(entry.getKey());

      if (localLSN == null || localLSN.compareTo(entry.getValue()) > 0) {
        localDirtyPages.put(entry.getKey(), entry.getValue());

        final long segment = entry.getValue().getSegment();
        TreeSet<PageKey> pages = localDirtyPagesBySegment.get(segment);
        if (pages == null) {
          pages = new TreeSet<>();
          pages.add(entry.getKey());

          localDirtyPagesBySegment.put(segment, pages);
        } else {
          pages.add(entry.getKey());
        }
      }
    }

    for (final Map.Entry<PageKey, OLogSequenceNumber> entry : localDirtyPages.entrySet()) {
      dirtyPages.remove(entry.getKey(), entry.getValue());
    }
  }

  private void removeFromDirtyPages(final PageKey pageKey) {
    dirtyPages.remove(pageKey);

    final OLogSequenceNumber lsn = localDirtyPages.remove(pageKey);
    if (lsn != null) {
      final long segment = lsn.getSegment();
      final TreeSet<PageKey> pages = localDirtyPagesBySegment.get(segment);
      assert pages != null;

      final boolean removed = pages.remove(pageKey);
      if (pages.isEmpty()) {
        localDirtyPagesBySegment.remove(segment);
      }

      assert removed;
    }
  }

  private void flushWriteCacheFromMinLSN(
      final long segStart, final long segEnd, final int pagesFlushLimit)
      throws InterruptedException, IOException {
    // first we try to find page which contains the oldest not flushed changes
    // that is needed to allow to compact WAL as earlier as possible
    convertSharedDirtyPagesToLocal();

    int copiedPages = 0;

    List<List<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>>> chunks = new ArrayList<>(16);
    ArrayList<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>> chunk = new ArrayList<>(16);

    long currentSegment = segStart;

    int chunksSize = 0;

    OLogSequenceNumber maxFullLogLSN = null;
    flushCycle:
    while (chunksSize < pagesFlushLimit) {
      final TreeSet<PageKey> segmentPages = localDirtyPagesBySegment.get(currentSegment);

      if (segmentPages == null) {
        currentSegment++;

        if (currentSegment >= segEnd) {
          break;
        }

        continue;
      }

      final Iterator<PageKey> lsnPagesIterator = segmentPages.iterator();

      final List<PageKey> pageKeysToFlush = new ArrayList<>(pagesFlushLimit);

      while (lsnPagesIterator.hasNext() && pageKeysToFlush.size() < pagesFlushLimit - chunksSize) {
        final PageKey pageKey = lsnPagesIterator.next();
        pageKeysToFlush.add(pageKey);
      }

      long lastPageIndex = -1;
      long lastFileId = -1;

      for (final PageKey pageKey : pageKeysToFlush) {
        if (lastFileId == -1) {
          if (!chunk.isEmpty()) {
            throw new IllegalStateException("Chunk is not empty !");
          }
        } else {
          if (lastPageIndex == -1) {
            throw new IllegalStateException("Last page index is -1");
          }

          if (lastFileId != pageKey.fileId || lastPageIndex != pageKey.pageIndex - 1) {
            if (!chunk.isEmpty()) {
              chunks.add(chunk);
              chunksSize += chunk.size();
              chunk = new ArrayList<>();
            }
          }
        }

        final OCachePointer pointer = writeCachePages.get(pageKey);

        if (pointer == null) {
          // we marked page as dirty but did not put it in cache yet
          if (!chunk.isEmpty()) {
            chunks.add(chunk);
          }

          break flushCycle;
        }

        if (pointer.tryAcquireSharedLock()) {
          final long version;
          final OLogSequenceNumber fullLogLSN;

          final OPointer directPointer =
              bufferPool.acquireDirect(false, Intention.COPY_PAGE_DURING_FLUSH);
          final ByteBuffer copy = directPointer.getNativeByteBuffer();
          assert copy.position() == 0;
          try {
            version = pointer.getVersion();
            final ByteBuffer buffer = pointer.getBufferDuplicate();

            fullLogLSN = pointer.getEndLSN();

            assert buffer != null;
            buffer.position(0);
            copy.position(0);

            copy.put(buffer);

            removeFromDirtyPages(pageKey);

            copiedPages++;
          } finally {
            pointer.releaseSharedLock();
          }

          if (fullLogLSN != null
              && (maxFullLogLSN == null || fullLogLSN.compareTo(maxFullLogLSN) > 0)) {
            maxFullLogLSN = fullLogLSN;
          }

          copy.position(0);

          chunk.add(new OQuarto<>(version, copy, directPointer, pointer));

          if (chunksSize + chunk.size() >= pagesFlushLimit) {
            if (!chunk.isEmpty()) {
              chunks.add(chunk);
              chunksSize += chunk.size();
              chunk = new ArrayList<>();
            }

            lastPageIndex = -1;
            lastFileId = -1;
          } else {
            lastPageIndex = pageKey.pageIndex;
            lastFileId = pageKey.fileId;
          }
        } else {
          if (!chunk.isEmpty()) {
            chunks.add(chunk);
            chunksSize += chunk.size();
            chunk = new ArrayList<>();
          }

          maxFullLogLSN = null;

          lastPageIndex = -1;
          lastFileId = -1;
        }
      }

      if (!chunk.isEmpty()) {
        chunks.add(chunk);
        chunksSize += chunk.size();
        chunk = new ArrayList<>();
      }
    }

    final int flushedPages = flushPages(chunks, maxFullLogLSN);
    if (copiedPages != flushedPages) {
      throw new IllegalStateException(
          "Copied pages (" + copiedPages + " ) != flushed pages (" + flushedPages + ")");
    }
  }

  private int flushPages(
      final List<List<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>>> chunks,
      final OLogSequenceNumber fullLogLSN)
      throws InterruptedException, IOException {

    if (chunks.isEmpty()) {
      return 0;
    }

    if (fullLogLSN != null) {
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLsn();

      while (flushedLSN == null || flushedLSN.compareTo(fullLogLSN) < 0) {
        writeAheadLog.flush();
        flushedLSN = writeAheadLog.getFlushedLsn();
      }
    }

    final boolean fsyncFiles;

    int flushedPages = 0;

    final OPointer[] containerPointers = new OPointer[chunks.size()];
    final ByteBuffer[] containerBuffers = new ByteBuffer[chunks.size()];
    final int[] chunkPositions = new int[chunks.size()];
    final int[] chunkFileIds = new int[chunks.size()];

    final Map<Long, List<ORawPair<Long, ByteBuffer>>> buffersByFileId = new HashMap<>();
    try {
      for (int i = 0; i < chunks.size(); i++) {
        final List<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>> chunk = chunks.get(i);

        flushedPages += chunk.size();

        final OPointer containerPointer =
            ODirectMemoryAllocator.instance()
                .allocate(
                    chunk.size() * pageSize,
                    false,
                    Intention.ALLOCATE_CHUNK_TO_WRITE_DATA_IN_BATCH);
        final ByteBuffer containerBuffer = containerPointer.getNativeByteBuffer();

        containerPointers[i] = containerPointer;
        containerBuffers[i] = containerBuffer;
        assert containerBuffer.position() == 0;

        for (final OQuarto<Long, ByteBuffer, OPointer, OCachePointer> quarto : chunk) {
          final ByteBuffer buffer = quarto.two;

          final OCachePointer pointer = quarto.four;

          addMagicChecksumAndEncryption(
              extractFileId(pointer.getFileId()), pointer.getPageIndex(), buffer);

          buffer.position(0);
          containerBuffer.put(buffer);
        }

        final OQuarto<Long, ByteBuffer, OPointer, OCachePointer> firstPage = chunk.get(0);
        final OCachePointer firstCachePointer = firstPage.four;

        final long fileId = firstCachePointer.getFileId();
        final int pageIndex = firstCachePointer.getPageIndex();

        final List<ORawPair<Long, ByteBuffer>> fileBuffers =
            buffersByFileId.computeIfAbsent(fileId, (id) -> new ArrayList<>());
        fileBuffers.add(new ORawPair<>(((long) pageIndex) * pageSize, containerBuffer));

        chunkPositions[i] = pageIndex;
        chunkFileIds[i] = internalFileId(fileId);
      }

      fsyncFiles = doubleWriteLog.write(containerBuffers, chunkFileIds, chunkPositions);

      final List<OClosableEntry<Long, OFile>> acquiredFiles =
          new ArrayList<>(buffersByFileId.size());
      final List<IOResult> ioResults = new ArrayList<>(buffersByFileId.size());

      final Iterator<Map.Entry<Long, List<ORawPair<Long, ByteBuffer>>>> filesIterator =
          buffersByFileId.entrySet().iterator();
      Map.Entry<Long, List<ORawPair<Long, ByteBuffer>>> entry = null;
      // acquire as much files as possible and flush data
      while (true) {
        if (entry == null) {
          if (filesIterator.hasNext()) {
            entry = filesIterator.next();
          } else {
            break;
          }
        }

        final OClosableEntry<Long, OFile> fileEntry = files.tryAcquire(entry.getKey());
        if (fileEntry != null) {
          final OFile file = fileEntry.get();

          final List<ORawPair<Long, ByteBuffer>> bufferList = entry.getValue();

          ioResults.add(file.write(bufferList));
          acquiredFiles.add(fileEntry);

          entry = null;
        } else {
          assert ioResults.size() == acquiredFiles.size();

          if (!ioResults.isEmpty()) {
            for (final IOResult ioResult : ioResults) {
              ioResult.await();
            }

            for (final OClosableEntry<Long, OFile> closableEntry : acquiredFiles) {
              files.release(closableEntry);
            }

            ioResults.clear();
            acquiredFiles.clear();
          } else {
            Thread.yield();
          }
        }
      }

      assert ioResults.size() == acquiredFiles.size();

      if (!ioResults.isEmpty()) {
        for (final IOResult ioResult : ioResults) {
          ioResult.await();
        }

        for (final OClosableEntry<Long, OFile> closableEntry : acquiredFiles) {
          files.release(closableEntry);
        }
      }

    } finally {
      for (final OPointer containerPointer : containerPointers) {
        if (containerPointer != null) {
          ODirectMemoryAllocator.instance().deallocate(containerPointer);
        }
      }
    }

    if (fsyncFiles) {
      fsyncFiles();
    }

    for (final List<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>> chunk : chunks) {
      for (final OQuarto<Long, ByteBuffer, OPointer, OCachePointer> chunkPage : chunk) {
        final OCachePointer pointer = chunkPage.four;

        final PageKey pageKey =
            new PageKey(internalFileId(pointer.getFileId()), pointer.getPageIndex());
        final long version = chunkPage.one;

        final Lock lock = lockManager.acquireExclusiveLock(pageKey);
        try {
          if (!pointer.tryAcquireSharedLock()) {
            continue;
          }

          try {
            if (version == pointer.getVersion()) {
              writeCachePages.remove(pageKey);
              writeCacheSize.decrementAndGet();

              pointer.decrementWritersReferrer();
              pointer.setWritersListener(null);
            }
          } finally {
            pointer.releaseSharedLock();
          }
        } finally {
          lock.unlock();
        }

        bufferPool.release(chunkPage.three);
      }
    }

    return flushedPages;
  }

  private void flushExclusiveWriteCache(final CountDownLatch latch, long pagesToFlush)
      throws InterruptedException, IOException {
    final Iterator<PageKey> iterator = exclusiveWritePages.iterator();

    int flushedPages = 0;
    int copiedPages = 0;

    final long ewcSize = exclusiveWriteCacheSize.get();
    pagesToFlush = Math.min(Math.max(pagesToFlush, chunkSize), ewcSize);

    List<List<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>>> chunks = new ArrayList<>(16);
    List<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>> chunk = new ArrayList<>(16);

    if (latch != null && ewcSize <= exclusiveWriteCacheMaxSize) {
      latch.countDown();
    }

    OLogSequenceNumber maxFullLogLSN = null;

    int chunksSize = 0;
    flushCycle:
    while (chunksSize < pagesToFlush) {
      long lastFileId = -1;
      long lastPageIndex = -1;

      while (chunksSize + chunk.size() < pagesToFlush) {
        if (!iterator.hasNext()) {
          if (!chunk.isEmpty()) {
            chunks.add(chunk);
          }

          break flushCycle;
        }

        final PageKey pageKey = iterator.next();

        final OCachePointer pointer = writeCachePages.get(pageKey);
        final long version;

        if (pointer == null) {
          iterator.remove();
        } else {
          if (pointer.tryAcquireSharedLock()) {
            final OLogSequenceNumber fullLSN;

            final OPointer directPointer =
                bufferPool.acquireDirect(false, Intention.COPY_PAGE_DURING_EXCLUSIVE_PAGE_FLUSH);
            final ByteBuffer copy = directPointer.getNativeByteBuffer();
            assert copy.position() == 0;
            try {
              version = pointer.getVersion();
              final ByteBuffer buffer = pointer.getBufferDuplicate();

              fullLSN = pointer.getEndLSN();

              assert buffer != null;
              buffer.position(0);
              copy.position(0);

              copy.put(buffer);

              removeFromDirtyPages(pageKey);

              copiedPages++;
            } finally {
              pointer.releaseSharedLock();
            }

            if (fullLSN != null
                && (maxFullLogLSN == null || maxFullLogLSN.compareTo(fullLSN) < 0)) {
              maxFullLogLSN = fullLSN;
            }

            copy.position(0);

            if (!chunk.isEmpty()) {
              if (lastFileId != pointer.getFileId()
                  || lastPageIndex != pointer.getPageIndex() - 1) {
                chunks.add(chunk);
                chunksSize += chunk.size();
                chunk = new ArrayList<>();

                if (chunksSize - flushedPages >= this.chunkSize) {
                  flushedPages += flushPages(chunks, maxFullLogLSN);
                  chunks.clear();

                  if (latch != null
                      && exclusiveWriteCacheSize.get() <= exclusiveWriteCacheMaxSize) {
                    latch.countDown();
                  }

                  maxFullLogLSN = null;
                }
              }
            }

            chunk.add(new OQuarto<>(version, copy, directPointer, pointer));

            lastFileId = pointer.getFileId();
            lastPageIndex = pointer.getPageIndex();
          } else {
            if (!chunk.isEmpty()) {
              chunks.add(chunk);
              chunksSize += chunk.size();
              chunk = new ArrayList<>();
            }

            if (chunksSize - flushedPages >= this.chunkSize) {
              flushedPages += flushPages(chunks, maxFullLogLSN);
              chunks.clear();

              if (latch != null && exclusiveWriteCacheSize.get() <= exclusiveWriteCacheMaxSize) {
                latch.countDown();
              }

              maxFullLogLSN = null;
            }

            lastFileId = -1;
            lastPageIndex = -1;
          }
        }
      }

      if (!chunk.isEmpty()) {
        chunks.add(chunk);
        chunksSize += chunk.size();
        chunk = new ArrayList<>();

        if (chunksSize - flushedPages >= this.chunkSize) {
          flushedPages += flushPages(chunks, maxFullLogLSN);
          chunks.clear();

          maxFullLogLSN = null;

          if (latch != null && exclusiveWriteCacheSize.get() <= exclusiveWriteCacheMaxSize) {
            latch.countDown();
          }
        }
      }
    }

    flushedPages += flushPages(chunks, maxFullLogLSN);

    if (copiedPages != flushedPages) {
      throw new IllegalStateException(
          "Copied pages (" + copiedPages + " ) != flushed pages (" + flushedPages + ")");
    }
  }

  private final class FileFlushTask implements Callable<Void> {
    private final Set<Integer> fileIdSet;

    private FileFlushTask(final Collection<Integer> fileIds) {
      this.fileIdSet = new HashSet<>(fileIds);
    }

    @Override
    public Void call() throws Exception {
      if (flushError != null) {
        OLogManager.instance()
            .errorNoDb(
                this,
                "Can not flush file data because of issue during data write, %s",
                null,
                flushError.getMessage());
        return null;
      }

      writeAheadLog.flush();

      final TreeSet<PageKey> pagesToFlush = new TreeSet<>();
      for (final Map.Entry<PageKey, OCachePointer> entry : writeCachePages.entrySet()) {
        final PageKey pageKey = entry.getKey();
        if (fileIdSet.contains(pageKey.fileId)) {
          pagesToFlush.add(pageKey);
        }
      }

      OLogSequenceNumber maxLSN = null;

      final List<List<OQuarto<Long, ByteBuffer, OPointer, OCachePointer>>> chunks =
          new ArrayList<>(chunkSize);
      for (final PageKey pageKey : pagesToFlush) {
        if (fileIdSet.contains(pageKey.fileId)) {
          final OCachePointer pagePointer = writeCachePages.get(pageKey);
          final Lock pageLock = lockManager.acquireExclusiveLock(pageKey);
          try {
            if (!pagePointer.tryAcquireSharedLock()) {
              continue;
            }
            try {
              final ByteBuffer buffer = pagePointer.getBufferDuplicate();

              final OPointer directPointer = bufferPool.acquireDirect(false, Intention.FILE_FLUSH);
              final ByteBuffer copy = directPointer.getNativeByteBuffer();
              assert copy.position() == 0;

              assert buffer != null;
              buffer.position(0);
              copy.put(buffer);

              final OLogSequenceNumber endLSN = pagePointer.getEndLSN();

              if (endLSN != null && (maxLSN == null || endLSN.compareTo(maxLSN) > 0)) {
                maxLSN = endLSN;
              }

              chunks.add(
                  Collections.singletonList(
                      new OQuarto<>(pagePointer.getVersion(), copy, directPointer, pagePointer)));
              removeFromDirtyPages(pageKey);
            } finally {
              pagePointer.releaseSharedLock();
            }
          } finally {
            pageLock.unlock();
          }

          if (chunks.size() >= 4 * chunkSize) {
            flushPages(chunks, maxLSN);
            chunks.clear();
          }
        }
      }

      flushPages(chunks, maxLSN);

      if (callFsync) {
        for (final int iFileId : fileIdSet) {
          final long finalId = composeFileId(id, iFileId);
          final OClosableEntry<Long, OFile> entry = files.acquire(finalId);
          if (entry != null) {
            try {
              entry.get().synch();
            } finally {
              files.release(entry);
            }
          }
        }
      }

      return null;
    }
  }

  private static Cipher getCipherInstance() {
    try {
      return Cipher.getInstance(TRANSFORMATION);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw OException.wrapException(
          new OSecurityException("Implementation of encryption " + TRANSFORMATION + " is absent"),
          e);
    }
  }

  private final class RemoveFilePagesTask implements Callable<Void> {
    private final int fileId;

    private RemoveFilePagesTask(final int fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() {
      doRemoveCachePages(fileId);
      return null;
    }
  }

  private final class DeleteFileTask implements Callable<ORawPair<String, String>> {
    private final long externalFileId;

    private DeleteFileTask(long externalFileId) {
      this.externalFileId = externalFileId;
    }

    @Override
    public ORawPair<String, String> call() throws Exception {
      final int internalFileId = extractFileId(externalFileId);
      final long fileId = composeFileId(id, internalFileId);

      doRemoveCachePages(internalFileId);

      final OFile fileClassic = files.remove(fileId);

      if (fileClassic != null) {
        if (fileClassic.exists()) {
          fileClassic.delete();
        }

        final String name = idNameMap.get(internalFileId);

        idNameMap.remove(internalFileId);

        nameIdMap.put(name, -internalFileId);
        idNameMap.put(-internalFileId, name);

        return new ORawPair<>(fileClassic.getName(), name);
      }

      return null;
    }
  }
}
