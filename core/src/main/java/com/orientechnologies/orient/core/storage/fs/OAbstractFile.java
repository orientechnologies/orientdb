/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

/**
 * 
 * Header structure:<br/>
 * <br/>
 * +-----------+--------------+---------------+---------------+---------------+<br/>
 * | FILE SIZE | FILLED UP TO | SOFTLY CLOSED | SECURITY CODE | VERSION |<br/>
 * | 8 bytes . | 8 bytes .... | 1 byte ...... | 32 bytes .... | 1 byte....... |<br/>
 * +-----------+--------------+---------------+---------------+---------------+<br/>
 * = 1024 bytes<br/>
 * <br/>
 */
public abstract class OAbstractFile implements OFile {
  private FileLock            fileLock;

  protected File              osFile;
  protected RandomAccessFile  accessFile;
  protected FileChannel       channel;
  protected volatile boolean  dirty                    = false;
  protected volatile boolean  headerDirty              = false;
  protected int               version;

  protected int               incrementSize            = DEFAULT_INCREMENT_SIZE;
  protected long              maxSize;
  protected byte[]            securityCode             = new byte[32];                // PART OF HEADER (32 bytes)
  protected String            mode;
  protected boolean           failCheck                = true;
  protected volatile long     size;                                                   // PART OF HEADER (4 bytes)

  public static final int     HEADER_SIZE              = 1024;
  protected static final int  HEADER_DATA_OFFSET       = 128;
  protected static final int  DEFAULT_SIZE             = 1024000;
  protected static final int  DEFAULT_INCREMENT_SIZE   = -50;                         // NEGATIVE NUMBER MEANS AS PERCENT OF
                                                                                       // CURRENT
                                                                                       // SIZE

  private static final int    OPEN_RETRY_MAX           = 10;
  private static final int    OPEN_DELAY_RETRY         = 100;

  private static final long   LOCK_WAIT_TIME           = 300;
  private static final int    LOCK_MAX_RETRIES         = 10;

  protected static final int  SIZE_OFFSET_V_0          = 0;
  protected static final int  FILLEDUPTO_OFFSET_V_0    = 4;
  protected static final int  SOFTLY_CLOSED_OFFSET_V_0 = 8;

  protected static final int  SIZE_OFFSET              = 0;
  protected static final int  FILLEDUPTO_OFFSET        = 8;
  protected static final int  SOFTLY_CLOSED_OFFSET     = 16;
  protected static final int  VERSION_OFFSET           = 48;

  protected static final int  CURRENT_VERSION          = 1;

  private final ReadWriteLock lock                     = new ReentrantReadWriteLock();
  private boolean             wasSoftlyClosed          = true;

  public abstract long getFileSize();

  public abstract long getFilledUpTo();

  public abstract void setSize(long iSize) throws IOException;

  public abstract void writeHeaderLong(int iPosition, long iValue) throws IOException;

  public abstract long readHeaderLong(int iPosition) throws IOException;

  public abstract boolean synch() throws IOException;

  public abstract void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException;

  public abstract short readShort(long iLogicalPosition) throws IOException;

  public abstract int readInt(long iLogicalPosition) throws IOException;

  public abstract long readLong(long iOffset) throws IOException;

  public abstract byte readByte(long iOffset) throws IOException;

  public abstract void writeInt(long iOffset, int iValue) throws IOException;

  public abstract void writeLong(long iOffset, long iValue) throws IOException;

  public abstract void writeShort(long iOffset, short iValue) throws IOException;

  public abstract void writeByte(long iOffset, byte iValue) throws IOException;

  public abstract void write(long iOffset, byte[] iSourceBuffer) throws IOException;

  protected abstract void init() throws IOException;

  protected abstract void setFilledUpTo(long iHow) throws IOException;

  protected abstract void flushHeader() throws IOException;

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#open()
   */
  public boolean open() throws IOException {
    acquireWriteLock();
    try {
      if (!osFile.exists())
        throw new FileNotFoundException("File: " + osFile.getAbsolutePath());

      openChannel(osFile.length());

      OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getName() + "...");

      init();

      long filledUpTo = getFilledUpTo();

      long fileSize = getFileSize();
      if (fileSize == 0) {
        // CORRUPTED? GET THE OS FILE SIZE
        final long newFileSize = osFile.length() - HEADER_SIZE;
        if (newFileSize != fileSize) {
          OLogManager
              .instance()
              .error(
                  this,
                  "Invalid fileSize=%d for file %s. Resetting it to the os file size: %d. Probably the file was not closed correctly last time. The number of records has been set to the maximum value. It's strongly suggested to export and reimport the database before using it",
                  fileSize, getOsFile().getAbsolutePath(), newFileSize);

          setFilledUpTo(newFileSize, true);
          setSize(newFileSize, true);
          fileSize = newFileSize;
        }
      }

      if (filledUpTo > 0 && filledUpTo > fileSize) {
        OLogManager
            .instance()
            .error(
                this,
                "Invalid filledUp=%d for file %s. Resetting it to the os file size: %d. Probably the file was not closed correctly last time. The number of records has been set to the maximum value. It's strongly suggested to export and reimport the database before using it",
                filledUpTo, getOsFile().getAbsolutePath(), fileSize);
        setSize(fileSize);
        setFilledUpTo(fileSize);
        filledUpTo = getFilledUpTo();
      }

      if (filledUpTo > fileSize || filledUpTo < 0)
        OLogManager.instance().error(this, "Invalid filledUp size (=" + filledUpTo + "). The file could be corrupted", null,
            OStorageException.class);

      if (failCheck) {
        wasSoftlyClosed = isSoftlyClosed();

        if (wasSoftlyClosed)
          setSoftlyClosed(false);
      }

      if (version < CURRENT_VERSION) {
        setSize(fileSize, true);
        setFilledUpTo(filledUpTo, true);
        setVersion(CURRENT_VERSION);
        version = CURRENT_VERSION;
        setSoftlyClosed(!failCheck);
      }

      if (failCheck)
        return wasSoftlyClosed;

      return true;
    } finally {
      releaseWriteLock();
    }
  }

  public boolean wasSoftlyClosed() {
    acquireReadLock();
    try {
      return wasSoftlyClosed;
    } finally {
      releaseReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#create(int)
   */
  public void create(int iStartSize) throws IOException {
    acquireWriteLock();
    try {
      if (iStartSize == -1)
        iStartSize = DEFAULT_SIZE;

      openChannel(iStartSize);

      setFilledUpTo(0, true);
      setSize(maxSize > 0 && iStartSize > maxSize ? maxSize : iStartSize, true);
      setVersion(CURRENT_VERSION);
      version = CURRENT_VERSION;
      setSoftlyClosed(!failCheck);
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
   */
  public void close() throws IOException {
    acquireWriteLock();
    try {
      try {
        setSoftlyClosed(true);

        if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
          unlock();

        if (channel != null && channel.isOpen()) {
          channel.close();
          channel = null;
        }

        if (accessFile != null) {
          accessFile.close();
          accessFile = null;
        }

      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on closing file " + osFile.getAbsolutePath(), e, OIOException.class);
      }
    } finally {
      releaseWriteLock();
    }
  }

  public void close(boolean softlyClosed) throws IOException {
    acquireWriteLock();
    try {
      try {
        setSoftlyClosed(softlyClosed);

        if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
          unlock();

        if (channel != null && channel.isOpen()) {
          channel.close();
          channel = null;
        }

        if (accessFile != null) {
          accessFile.close();
          accessFile = null;
        }

      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on closing file " + osFile.getAbsolutePath(), e, OIOException.class);
      }
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#delete()
   */
  public void delete() throws IOException {
    acquireWriteLock();
    try {
      close();
      if (osFile != null) {
        boolean deleted = osFile.delete();
        while (!deleted) {
          OMemoryWatchDog.freeMemoryForResourceCleanup(100);
          deleted = !osFile.exists() || osFile.delete();
        }
      }
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * Locks a portion of file.
   */
  public FileLock lock(final long iRangeFrom, final long iRangeSize, final boolean iShared) throws IOException {
    acquireWriteLock();
    try {
      return channel.lock(iRangeFrom, iRangeSize, iShared);
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * Unlocks a portion of file.
   */
  public OFile unlock(final FileLock iLock) throws IOException {
    acquireWriteLock();
    try {
      if (iLock != null) {
        try {
          iLock.release();
        } catch (ClosedChannelException e) {
        }
      }
      return this;
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#lock()
   */
  public void lock() throws IOException {
    if (channel == null)
      return;

    acquireWriteLock();
    try {
      for (int i = 0; i < LOCK_MAX_RETRIES; ++i) {
        try {
          fileLock = channel.tryLock(0, 1, true);
          if (fileLock != null)
            break;
        } catch (OverlappingFileLockException e) {
          OLogManager.instance().debug(this,
              "Cannot open file '" + osFile.getAbsolutePath() + "' because it is locked. Waiting %d ms and retrying %d/%d...",
              LOCK_WAIT_TIME, i, LOCK_MAX_RETRIES);

          // FORCE FINALIZATION TO COLLECT ALL THE PENDING BUFFERS
          OMemoryWatchDog.freeMemoryForResourceCleanup(LOCK_WAIT_TIME);
        }

        if (fileLock == null)
          throw new OLockException(
              "File '"
                  + osFile.getPath()
                  + "' is locked by another process, maybe the database is in use by another process. Use the remote mode with a OrientDB server to allow multiple access to the same database.");
      }
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#unlock()
   */
  public void unlock() throws IOException {
    acquireWriteLock();
    try {
      if (fileLock != null) {
        try {
          fileLock.release();
        } catch (ClosedChannelException e) {
        }
        fileLock = null;
      }
    } finally {
      releaseWriteLock();
    }
  }

  protected void checkSize(final long iSize) throws IOException {
    acquireReadLock();
    try {
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Changing file size to " + iSize + " bytes. " + toString());

      final long filledUpTo = getFilledUpTo();
      if (iSize < filledUpTo)
        OLogManager.instance().error(
            this,
            "You cannot resize down the file to " + iSize + " bytes, since it is less than current space used: " + filledUpTo
                + " bytes", OIOException.class);
    } finally {
      releaseReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#removeTail(int)
   */
  public void removeTail(long iSizeToShrink) throws IOException {
    acquireWriteLock();
    try {
      final long filledUpTo = getFilledUpTo();
      if (filledUpTo < iSizeToShrink)
        iSizeToShrink = 0;

      setFilledUpTo(filledUpTo - iSizeToShrink);
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#shrink(int)
   */
  public void shrink(final long iSize) throws IOException {
    acquireWriteLock();
    try {
      final long filledUpTo = getFilledUpTo();
      if (iSize >= filledUpTo)
        return;

      OLogManager.instance().debug(this, "Shrinking filled file from " + filledUpTo + " to " + iSize + " bytes. " + toString());

      setFilledUpTo(iSize);
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#allocateSpace(int)
   */
  public long allocateSpace(final long iSize) throws IOException {
    acquireWriteLock();
    try {
      final long offset = getFilledUpTo();
      final long size = getFileSize();

      if (getFreeSpace() < iSize) {
        if (maxSize > 0 && maxSize - size < iSize)
          throw new IllegalArgumentException("Cannot enlarge file since the configured max size ("
              + OFileUtils.getSizeAsString(maxSize) + ") was reached! " + toString());

        // MAKE ROOM
        long newFileSize = size;

        if (newFileSize == 0)
          // PROBABLY HAS BEEN LOST WITH HARD KILLS
          newFileSize = DEFAULT_SIZE;

        // GET THE STEP SIZE IN BYTES
        long stepSizeInBytes = incrementSize > 0 ? incrementSize : -1 * size / 100 * incrementSize;

        // FIND THE BEST SIZE TO ALLOCATE (BASED ON INCREMENT-SIZE)
        while (newFileSize - offset <= iSize) {
          newFileSize += stepSizeInBytes;

          if (newFileSize == 0)
            // EMPTY FILE: ALLOCATE REQUESTED SIZE ONLY
            newFileSize = iSize;
          if (newFileSize > maxSize && maxSize > 0)
            // TOO BIG: ROUND TO THE MAXIMUM FILE SIZE
            newFileSize = maxSize;
        }

        setSize(newFileSize);
      }

      // THERE IS SPACE IN FILE: RETURN THE UPPER BOUND OFFSET AND UPDATE THE FILLED THRESHOLD
      setFilledUpTo(offset + iSize);

      return offset;
    } finally {
      releaseWriteLock();
    }
  }

  protected long checkRegions(final long iOffset, final long iLength) {
    acquireReadLock();
    try {
      if (iOffset < 0 || iOffset + iLength > getFilledUpTo())
        throw new OIOException("You cannot access outside the file size (" + getFilledUpTo()
            + " bytes). You have requested portion " + iOffset + "-" + (iOffset + iLength) + " bytes. File: " + toString());

      return iOffset;
    } finally {
      releaseReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getFreeSpace()
   */
  public long getFreeSpace() {
    acquireReadLock();
    try {
      return getFileSize() - getFilledUpTo();
    } finally {
      releaseReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#canOversize(int)
   */
  public boolean canOversize(final int iRecordSize) {
    acquireReadLock();
    try {
      return maxSize - getFileSize() > iRecordSize;
    } finally {
      releaseReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("File: ");
    builder.append(osFile.getName());
    if (accessFile != null) {
      builder.append(" os-size=");
      try {
        builder.append(accessFile.length());
      } catch (IOException e) {
        builder.append("?");
      }
    }
    builder.append(", stored=");
    builder.append(getFileSize());
    builder.append(", filled=");
    builder.append(getFilledUpTo());
    builder.append(", max=");
    builder.append(maxSize);
    builder.append("");
    return builder.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getOsFile()
   */
  public File getOsFile() {
    acquireReadLock();
    try {
      return osFile;
    } finally {
      releaseReadLock();
    }

  }

  public OAbstractFile init(final String iFileName, final String iMode) {
    acquireWriteLock();
    try {
      mode = iMode;
      osFile = new File(iFileName);

      return this;
    } finally {
      releaseWriteLock();
    }
  }

  protected void openChannel(final long newSize) throws IOException {
    acquireWriteLock();
    try {
      OLogManager.instance().debug(this, "[OFile.openChannel] opening channel for file '%s' of size: %d", osFile, osFile.length());

      for (int i = 0; i < OPEN_RETRY_MAX; ++i)
        try {
          accessFile = new RandomAccessFile(osFile, mode);
          break;
        } catch (FileNotFoundException e) {
          if (i == OPEN_DELAY_RETRY)
            throw e;

          // TRY TO RE-CREATE THE DIRECTORY (THIS HAPPENS ON WINDOWS AFTER A DELETE IS PENDING, USUALLY WHEN REOPEN THE DB VERY
          // FREQUENTLY)
          osFile.getParentFile().mkdirs();
          try {
            Thread.sleep(OPEN_DELAY_RETRY);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
        }

      if (accessFile == null)
        throw new FileNotFoundException(osFile.getAbsolutePath());

      if (accessFile.length() != newSize)
        accessFile.setLength(newSize);

      accessFile.seek(VERSION_OFFSET);
      version = accessFile.read();

      accessFile.seek(0);
      channel = accessFile.getChannel();
      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
        lock();
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getMaxSize()
   */
  public long getMaxSize() {
    acquireReadLock();
    try {
      return maxSize;
    } finally {
      releaseReadLock();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#setMaxSize(int)
   */
  public void setMaxSize(int maxSize) {
    acquireWriteLock();
    try {
      this.maxSize = maxSize;
    } finally {
      releaseWriteLock();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getIncrementSize()
   */
  public int getIncrementSize() {
    acquireReadLock();
    try {
      return incrementSize;
    } finally {
      releaseReadLock();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#setIncrementSize(int)
   */
  public void setIncrementSize(int incrementSize) {
    acquireWriteLock();
    try {
      this.incrementSize = incrementSize;
    } finally {
      releaseWriteLock();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isOpen()
   */
  public boolean isOpen() {
    acquireReadLock();
    try {
      return accessFile != null;
    } finally {
      releaseReadLock();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#exists()
   */
  public boolean exists() {
    acquireReadLock();
    try {
      return osFile != null && osFile.exists();
    } finally {
      releaseReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isFailCheck()
   */
  public boolean isFailCheck() {
    acquireReadLock();
    try {
      return failCheck;
    } finally {
      releaseReadLock();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#setFailCheck(boolean)
   */
  public void setFailCheck(boolean failCheck) {
    acquireWriteLock();
    try {
      this.failCheck = failCheck;
    } finally {
      releaseWriteLock();
    }

  }

  protected void setDirty() {
    acquireWriteLock();
    try {
      if (!dirty)
        dirty = true;
    } finally {
      releaseWriteLock();
    }
  }

  protected void setHeaderDirty() {
    acquireWriteLock();
    try {
      if (!headerDirty)
        headerDirty = true;
    } finally {
      releaseWriteLock();
    }
  }

  public String getName() {
    acquireReadLock();
    try {
      return osFile.getName();
    } finally {
      releaseReadLock();
    }
  }

  public String getPath() {
    acquireReadLock();
    try {
      return osFile.getPath();
    } finally {
      releaseReadLock();
    }
  }

  public String getAbsolutePath() {
    acquireReadLock();
    try {
      return osFile.getAbsolutePath();
    } finally {
      releaseReadLock();
    }
  }

  public boolean renameTo(final File newFile) {
    acquireWriteLock();
    try {
      return osFile.renameTo(newFile);
    } finally {
      releaseWriteLock();
    }
  }

  protected void acquireWriteLock() {
    lock.writeLock().lock();
  }

  protected void releaseWriteLock() {
    lock.writeLock().unlock();
  }

  protected void acquireReadLock() {
    lock.readLock().lock();
  }

  protected void releaseReadLock() {
    lock.readLock().unlock();
  }

  protected abstract void setVersion(int version) throws IOException;

  protected abstract void setFilledUpTo(final long iHow, boolean force);

  protected abstract void setSize(final long size, final boolean force) throws IOException;
}
