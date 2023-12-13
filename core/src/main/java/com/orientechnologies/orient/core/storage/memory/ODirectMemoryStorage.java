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

package com.orientechnologies.orient.core.storage.memory;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OQuarto;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OMemoryWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/9/14
 */
public class ODirectMemoryStorage extends OAbstractPaginatedStorage {
  private static final int ONE_KB = 1024;

  private static final String ALGORITHM_NAME = "AES";
  private static final String TRANSFORMATION = "AES/CTR/NoPadding";
  private static final String CONF_ENTRY_NAME = "database.ocf";
  private static final String CONF_UTF_8_ENTRY_NAME = "database_utf8.ocf";
  private static final String ENCRYPTION_IV = "encryption.iv";
  private static final String IV_EXT = ".iv";
  protected static final String IV_NAME = "data" + IV_EXT;

  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(ODirectMemoryStorage::getCipherInstance);

  public ODirectMemoryStorage(
      final String name, final String filePath, final int id, OrientDBInternal context) {
    super(name, filePath, id, context);
  }

  @Override
  protected void initWalAndDiskCache(final OContextConfiguration contextConfiguration) {
    if (writeAheadLog == null) {
      writeAheadLog = new OMemoryWriteAheadLog();
    }

    final ODirectMemoryOnlyDiskCache diskCache =
        new ODirectMemoryOnlyDiskCache(
            contextConfiguration.getValueAsInteger(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE)
                * ONE_KB,
            1);

    if (readCache == null) {
      readCache = diskCache;
    }

    if (writeCache == null) {
      writeCache = diskCache;
    }
  }

  @Override
  protected void postCloseSteps(
      final boolean onDelete, final boolean internalError, final long lastTxId) {}

  @Override
  public boolean exists() {
    try {
      return readCache != null && writeCache.exists("default" + OPaginatedCluster.DEF_EXTENSION);
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e, false);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public String getType() {
    return OEngineMemory.NAME;
  }

  @Override
  public String getURL() {
    return OEngineMemory.NAME + ":" + url;
  }

  @Override
  public void flushAllData() {}

  @Override
  protected void readIv() {}

  @Override
  protected byte[] getIv() {
    return new byte[0];
  }

  @Override
  protected void initIv() {}

  @Override
  public List<String> backup(
      final OutputStream out,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iListener,
      final int compressionLevel,
      final int bufferSize) {

    checkOpennessAndMigration();

    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();
      freeze(false);

      try {
        final ZipOutputStream zipOutputStream =
            new ZipOutputStream(
                new BufferedOutputStream(out), Charset.forName(configuration.getCharset()));
        try {

          final byte[] encryptionIv = new byte[16];
          final SecureRandom secureRandom = new SecureRandom();
          secureRandom.nextBytes(encryptionIv);

          final String aesKeyEncoded =
              getConfiguration()
                  .getContextConfiguration()
                  .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
          final byte[] aesKey =
              aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

          if (aesKey != null && aesKey.length != 16 && aesKey.length != 24 && aesKey.length != 32) {
            throw new OInvalidStorageEncryptionKeyException(
                "Invalid length of the encryption key, provided size is " + aesKey.length);
          }

          backupPagesWithChanges(zipOutputStream, encryptionIv, aesKey);

        } catch (IOException e) {
          OException.wrapException(new OStorageException("Error on memory file backup"), e);
        } finally {
          try {
            zipOutputStream.close();
          } catch (IOException e) {
            OLogManager.instance().warn(this, "Failed to flush resource " + zipOutputStream);
          }
        }
      } finally {
        release();
      }
    } finally {
      stateLock.readLock().unlock();
    }
    return new ArrayList<>();
  }

  private void backupPagesWithChanges(
      final ZipOutputStream stream, final byte[] encryptionIv, final byte[] aesKey)
      throws IOException {

    final Map<String, Long> files = writeCache.files();
    final int pageSize = writeCache.pageSize();

    for (Map.Entry<String, Long> entry : files.entrySet()) {
      final String fileName = entry.getKey();

      long fileId = entry.getValue();
      fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

      final long filledUpTo = writeCache.getFilledUpTo(fileId);
      final ZipEntry zipEntry = new ZipEntry(fileName);

      stream.putNextEntry(zipEntry);

      final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serialize(fileId, binaryFileId, 0);
      stream.write(binaryFileId, 0, binaryFileId.length);

      for (int pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        final OCacheEntry cacheEntry =
            readCache.silentLoadForRead(fileId, pageIndex, writeCache, true);
        cacheEntry.acquireSharedLock();
        try {
          final byte[] data = new byte[pageSize + OLongSerializer.LONG_SIZE];
          OLongSerializer.INSTANCE.serializeNative(pageIndex, data, 0);
          ODurablePage.getPageData(
              cacheEntry.getCachePointer().getBufferDuplicate(),
              data,
              OLongSerializer.LONG_SIZE,
              pageSize);

          if (aesKey != null) {
            doEncryptionDecryption(
                Cipher.ENCRYPT_MODE, aesKey, fileId, pageIndex, data, encryptionIv);
          }

          stream.write(data);

        } finally {
          cacheEntry.releaseSharedLock();
          readCache.releaseFromRead(cacheEntry);
        }
      }

      stream.closeEntry();
    }
  }

  private static void doEncryptionDecryption(
      final int mode,
      final byte[] aesKey,
      final long pageIndex,
      final long fileId,
      final byte[] backUpPage,
      final byte[] encryptionIv) {
    try {
      final Cipher cipher = CIPHER.get();
      final SecretKey secretKey = new SecretKeySpec(aesKey, ALGORITHM_NAME);

      final byte[] updatedIv = new byte[16];
      for (int i = 0; i < OLongSerializer.LONG_SIZE; i++) {
        updatedIv[i] = (byte) (encryptionIv[i] ^ ((pageIndex >>> i) & 0xFF));
      }

      for (int i = 0; i < OLongSerializer.LONG_SIZE; i++) {
        updatedIv[i + OLongSerializer.LONG_SIZE] =
            (byte) (encryptionIv[i + OLongSerializer.LONG_SIZE] ^ ((fileId >>> i) & 0xFF));
      }

      cipher.init(mode, secretKey, new IvParameterSpec(updatedIv));

      final byte[] data =
          cipher.doFinal(
              backUpPage, OLongSerializer.LONG_SIZE, backUpPage.length - OLongSerializer.LONG_SIZE);
      System.arraycopy(
          data,
          0,
          backUpPage,
          OLongSerializer.LONG_SIZE,
          backUpPage.length - OLongSerializer.LONG_SIZE);
    } catch (InvalidKeyException e) {
      throw OException.wrapException(new OInvalidStorageEncryptionKeyException(e.getMessage()), e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalArgumentException("Invalid IV.", e);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new IllegalStateException("Unexpected exception during CRT encryption.", e);
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

  @Override
  public void restore(
      final InputStream in,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iListener) {
    stateLock.writeLock().lock();
    try {
      final String aesKeyEncoded =
          getConfiguration()
              .getContextConfiguration()
              .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);
      final byte[] aesKey =
          aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

      if (aesKey != null && aesKey.length != 16 && aesKey.length != 24 && aesKey.length != 32) {
        throw new OInvalidStorageEncryptionKeyException(
            "Invalid length of the encryption key, provided size is " + aesKey.length);
      }

      final OQuarto<Locale, OContextConfiguration, String, Locale> quarto =
          preprocessingIncrementalRestore();
      final Locale serverLocale = quarto.one;
      final OContextConfiguration contextConfiguration = quarto.two;
      final String charset = quarto.three;
      final Locale locale = quarto.four;

      restoreFromIncrementalBackup(
          charset, serverLocale, locale, contextConfiguration, aesKey, in, true);

      postProcessIncrementalRestore(contextConfiguration);
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during restore from incremental backup"), e);
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private void postProcessIncrementalRestore(OContextConfiguration contextConfiguration)
      throws IOException {
    if (OClusterBasedStorageConfiguration.exists(writeCache)) {
      configuration = new OClusterBasedStorageConfiguration(this);
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              ((OClusterBasedStorageConfiguration) configuration)
                  .load(contextConfiguration, atomicOperation));
    } else {
      if (configuration == null) {
        configuration = new OClusterBasedStorageConfiguration(this);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                ((OClusterBasedStorageConfiguration) configuration)
                    .load(contextConfiguration, atomicOperation));
      }
    }

    atomicOperationsManager.executeInsideAtomicOperation(null, this::openClusters);
    sbTreeCollectionManager.close();
    sbTreeCollectionManager.load();
    openIndexes();

    flushAllData();

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation2 -> {
          generateDatabaseInstanceId(atomicOperation2);
        });
  }

  private byte[] restoreIv(final ZipInputStream zipInputStream) throws IOException {
    final byte[] iv = new byte[16];
    OIOUtils.readFully(zipInputStream, iv, 0, iv.length);

    return iv;
  }

  private byte[] restoreEncryptionIv(final ZipInputStream zipInputStream) throws IOException {
    final byte[] iv = new byte[16];
    int read = 0;
    while (read < iv.length) {
      final int localRead = zipInputStream.read(iv, read, iv.length - read);

      if (localRead < 0) {
        throw new OStorageException(
            "End of stream is reached but IV data were not completely read");
      }

      read += localRead;
    }

    return iv;
  }

  private void replaceConfiguration(ZipInputStream zipInputStream) throws IOException {
    byte[] buffer = new byte[1024];

    int rb = 0;
    while (true) {
      final int b = zipInputStream.read(buffer, rb, buffer.length - rb);

      if (b == -1) break;

      rb += b;

      if (rb == buffer.length) {
        byte[] oldBuffer = buffer;

        buffer = new byte[buffer.length << 1];
        System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length);
      }
    }
  }

  private void restoreFromIncrementalBackup(
      final String charset,
      final Locale serverLocale,
      final Locale locale,
      final OContextConfiguration contextConfiguration,
      final byte[] aesKey,
      final InputStream inputStream,
      final boolean isFull)
      throws IOException {
    stateLock.writeLock().lock();
    try {

      final List<String> currentFiles = new ArrayList<>(writeCache.files().keySet());

      final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      final ZipInputStream zipInputStream =
          new ZipInputStream(bufferedInputStream, Charset.forName(charset));
      final int pageSize = writeCache.pageSize();

      ZipEntry zipEntry;
      OLogSequenceNumber maxLsn = null;

      List<String> processedFiles = new ArrayList<>();

      if (isFull) {
        final Map<String, Long> files = writeCache.files();
        for (Map.Entry<String, Long> entry : files.entrySet()) {
          final long fileId = writeCache.fileIdByName(entry.getKey());

          // assert entry.getValue().equals(fileId);
          readCache.deleteFile(fileId, writeCache);
        }
      }

      final File walTempDir = createWalTempDirectory();

      byte[] encryptionIv = null;
      byte[] walIv = null;

      entryLoop:
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        if (zipEntry.getName().equals(IV_NAME)) {
          walIv = restoreIv(zipInputStream);
          continue;
        }

        if (zipEntry.getName().equals(ENCRYPTION_IV)) {
          encryptionIv = restoreEncryptionIv(zipInputStream);
          continue;
        }

        if (zipEntry.getName().equals(CONF_ENTRY_NAME)) {
          replaceConfiguration(zipInputStream);

          continue;
        }

        if (zipEntry.getName().equalsIgnoreCase("database_instance.uuid")) {
          continue;
        }

        if (zipEntry.getName().equals(CONF_UTF_8_ENTRY_NAME)) {
          replaceConfiguration(zipInputStream);

          continue;
        }

        if (zipEntry
            .getName()
            .toLowerCase(serverLocale)
            .endsWith(CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION)) {
          final String walName = zipEntry.getName();
          final int segmentIndex =
              walName.lastIndexOf(
                  ".", walName.length() - CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION.length() - 1);
          final String storageName = getName();

          if (segmentIndex < 0) {
            throw new IllegalStateException("Can not find index of WAL segment");
          }

          addFileToDirectory(
              storageName + walName.substring(segmentIndex), zipInputStream, walTempDir);
          continue;
        }

        if (aesKey != null && encryptionIv == null) {
          throw new OSecurityException("IV can not be null if encryption key is provided");
        }

        final byte[] binaryFileId = new byte[OLongSerializer.LONG_SIZE];
        OIOUtils.readFully(zipInputStream, binaryFileId, 0, binaryFileId.length);

        final long expectedFileId = OLongSerializer.INSTANCE.deserialize(binaryFileId, 0);
        long fileId;

        if (!writeCache.exists(zipEntry.getName())) {
          fileId = readCache.addFile(zipEntry.getName(), expectedFileId, writeCache);
        } else {
          fileId = writeCache.fileIdByName(zipEntry.getName());
        }

        if (!writeCache.fileIdsAreEqual(expectedFileId, fileId))
          throw new OStorageException(
              "Can not restore database from backup because expected and actual file ids are not the same");

        while (true) {
          final byte[] data = new byte[pageSize + OLongSerializer.LONG_SIZE];

          int rb = 0;

          while (rb < data.length) {
            final int b = zipInputStream.read(data, rb, data.length - rb);

            if (b == -1) {
              if (rb > 0)
                throw new OStorageException("Can not read data from file " + zipEntry.getName());
              else {
                processedFiles.add(zipEntry.getName());
                continue entryLoop;
              }
            }

            rb += b;
          }

          final long pageIndex = OLongSerializer.INSTANCE.deserializeNative(data, 0);

          if (aesKey != null) {
            doEncryptionDecryption(
                Cipher.DECRYPT_MODE, aesKey, expectedFileId, pageIndex, data, encryptionIv);
          }

          OCacheEntry cacheEntry =
              readCache.loadForWrite(fileId, pageIndex, writeCache, true, null);

          if (cacheEntry == null) {
            do {
              if (cacheEntry != null) readCache.releaseFromWrite(cacheEntry, writeCache, true);

              cacheEntry = readCache.allocateNewPage(fileId, writeCache, null);
            } while (cacheEntry.getPageIndex() != pageIndex);
          }

          try {
            final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
            final OLogSequenceNumber backedUpPageLsn =
                ODurablePage.getLogSequenceNumber(OLongSerializer.LONG_SIZE, data);
            if (isFull) {
              buffer.position(0);
              buffer.put(data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);

              if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                maxLsn = backedUpPageLsn;
              }
            } else {
              final OLogSequenceNumber currentPageLsn =
                  ODurablePage.getLogSequenceNumberFromPage(buffer);
              if (backedUpPageLsn.compareTo(currentPageLsn) > 0) {
                buffer.position(0);
                buffer.put(
                    data, OLongSerializer.LONG_SIZE, data.length - OLongSerializer.LONG_SIZE);

                if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                  maxLsn = backedUpPageLsn;
                }
              }
            }

          } finally {
            readCache.releaseFromWrite(cacheEntry, writeCache, true);
          }
        }
      }

      currentFiles.removeAll(processedFiles);

      for (String file : currentFiles) {
        if (writeCache.exists(file)) {
          final long fileId = writeCache.fileIdByName(file);
          readCache.deleteFile(fileId, writeCache);
        }
      }

    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private OQuarto<Locale, OContextConfiguration, String, Locale> preprocessingIncrementalRestore()
      throws IOException {
    final Locale serverLocale = configuration.getLocaleInstance();
    final OContextConfiguration contextConfiguration = configuration.getContextConfiguration();
    final String charset = configuration.getCharset();
    final Locale locale = configuration.getLocaleInstance();

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation -> {
          closeClusters(false);
          closeIndexes(atomicOperation, false);
          ((OClusterBasedStorageConfiguration) configuration).close(atomicOperation);
        });

    configuration = null;

    return new OQuarto<>(serverLocale, contextConfiguration, charset, locale);
  }

  @Override
  protected OLogSequenceNumber copyWALToIncrementalBackup(
      final ZipOutputStream zipOutputStream, final long startSegment) {
    return null;
  }

  @Override
  protected boolean isWriteAllowedDuringIncrementalBackup() {
    return false;
  }

  @Override
  protected File createWalTempDirectory() {
    return null;
  }

  @Override
  protected void addFileToDirectory(
      final String name, final InputStream stream, final File directory) {}

  @Override
  protected OWriteAheadLog createWalFromIBUFiles(
      final File directory,
      final OContextConfiguration contextConfiguration,
      final Locale locale,
      byte[] iv) {
    return null;
  }

  @Override
  public void shutdown() {
    try {
      delete();
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  protected void checkBackupRunning() {}

  @Override
  public boolean isMemory() {
    return true;
  }
}
