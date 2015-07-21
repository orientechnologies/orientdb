/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.storage.fs;

import java.io.*;
import java.nio.ByteBuffer;
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
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

public class OFileClassic implements OFile {
  private static final boolean trackFileClose           = OGlobalConfiguration.TRACK_FILE_CLOSE.getValueAsBoolean();

  public final static String   NAME                     = "classic";
  public static final int      HEADER_SIZE              = 1024;
  protected static final int   HEADER_DATA_OFFSET       = 128;
  protected static final int   DEFAULT_INCREMENT_SIZE   = -50;                                                      // NEGATIVE
                                                                                                                     // NUMBER MEANS
                                                                                                                     // AS
                                                                                                                     // PERCENT OF
  protected static final int   SOFTLY_CLOSED_OFFSET_V_0 = 8;
  protected static final int   SOFTLY_CLOSED_OFFSET     = 16;
  protected static final int   VERSION_OFFSET           = 48;
  protected static final int   CURRENT_VERSION          = 1;
  private static final int     OPEN_RETRY_MAX           = 10;
  private static final int     OPEN_DELAY_RETRY         = 100;
  private static final long    LOCK_WAIT_TIME           = 300;
  private static final int     LOCK_MAX_RETRIES         = 10;
  private final ReadWriteLock  lock                     = new ReentrantReadWriteLock();
  protected ByteBuffer         internalWriteBuffer      = ByteBuffer.allocate(OBinaryProtocol.SIZE_LONG);
  protected File               osFile;
  protected RandomAccessFile   accessFile;
  protected FileChannel        channel;
  protected volatile boolean   dirty                    = false;
  protected volatile boolean   headerDirty              = false;
  protected int                version;
  protected int                incrementSize            = DEFAULT_INCREMENT_SIZE;
  protected long               maxSize;
  protected String             mode;
  protected boolean            failCheck                = true;
  protected volatile long      size;                                                                                // PART OF
                                                                                                                     // HEADER (4
                                                                                                                     // bytes)
  private FileLock             fileLock;
  private boolean              wasSoftlyClosed          = true;

  @Override
  public long allocateSpace(long size) throws IOException {
    acquireWriteLock();
    try {
      final long currentSize = this.size;
      if (maxSize > 0 && currentSize + size > maxSize)
        throw new IllegalArgumentException("Cannot enlarge file since the configured max size ("
            + OFileUtils.getSizeAsString(maxSize) + ") was reached! " + toString());

      this.size += size;
      return currentSize;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void shrink(long iSize) throws IOException {
    acquireWriteLock();
    try {
      channel.truncate(HEADER_SIZE + iSize);
      size = iSize;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public long getFileSize() {
    return size;
  }

  @Override
  public long getFilledUpTo() {
    return size;
  }

  public void read(long iOffset, byte[] iData, int iLength, int iArrayOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, iLength);

      ByteBuffer buffer = ByteBuffer.wrap(iData, iArrayOffset, iLength);
      channel.read(buffer, iOffset);
    } finally {
      releaseReadLock();
    }
  }

  public void write(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    acquireWriteLock();
    try {
      writeInternal(iOffset, iData, iSize, iArrayOffset);
    } finally {
      releaseWriteLock();
    }
  }

  private void writeInternal(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    if (iData != null) {
      iOffset += HEADER_SIZE;
      ByteBuffer byteBuffer = ByteBuffer.wrap(iData, iArrayOffset, iSize);
      channel.write(byteBuffer, iOffset);
      setDirty();
    }
  }

  @Override
  public void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException {
    read(iOffset, iDestBuffer, iLenght, 0);
  }

  @Override
  public int readInt(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
      return readData(iOffset, OBinaryProtocol.SIZE_INT).getInt();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public long readLong(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
      return readData(iOffset, OBinaryProtocol.SIZE_LONG).getLong();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public short readShort(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
      return readData(iOffset, OBinaryProtocol.SIZE_SHORT).getShort();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public byte readByte(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
      return readData(iOffset, OBinaryProtocol.SIZE_BYTE).get();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void writeInt(long iOffset, final int iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_INT);
      buffer.putInt(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeLong(long iOffset, final long iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
      buffer.putLong(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeShort(long iOffset, final short iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_SHORT);
      buffer.putShort(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeByte(long iOffset, final byte iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
      buffer.put(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }

  }

  @Override
  public long write(long iOffset, final byte[] iSourceBuffer) throws IOException {
    long allocationDiff = 0;

    acquireWriteLock();
    try {
      if (iSourceBuffer != null) {
        final long start = accessFile.length();
        writeInternal(iOffset, iSourceBuffer, iSourceBuffer.length, 0);
        final long end = accessFile.length();
        allocationDiff = end - start;
      }
    } finally {
      releaseWriteLock();
    }

    return allocationDiff;
  }

  /**
   * Synchronizes the buffered changes to disk.
   * 
   * @throws IOException
   * 
   */
  @Override
  public boolean synch() throws IOException {
    acquireWriteLock();
    try {
      flushHeader();
      return true;
    } finally {
      releaseWriteLock();
    }
  }

  protected void flushHeader() throws IOException {
    acquireWriteLock();
    try {
      if (headerDirty || dirty) {
        headerDirty = dirty = false;
        try {
          channel.force(false);
        } catch (IOException e) {
          OLogManager.instance().warn(this, "Error during flush of file %s. Data may be lost in case of power failure.", getName());
        }

      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void create() throws IOException {
    acquireWriteLock();
    try {
      openChannel(HEADER_SIZE);

      setFilledUpTo(0, true);
      setSize(maxSize > 0 && HEADER_SIZE > maxSize ? maxSize : HEADER_SIZE, true);
      setVersion(OFileClassic.CURRENT_VERSION);
      version = OFileClassic.CURRENT_VERSION;
      setSoftlyClosed(!failCheck);
    } finally {
      releaseWriteLock();
    }
  }

  protected void init() throws IOException {
    acquireWriteLock();
    try {
      size = accessFile.length() - HEADER_SIZE;
    } finally {
      releaseWriteLock();
    }

  }

  protected void setFilledUpTo(final long value) throws IOException {
    setFilledUpTo(value, false);
  }

  protected void setFilledUpTo(long iHow, boolean force) {
    acquireWriteLock();
    try {
      size = iHow;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void setSize(final long iSize) throws IOException {
    setSize(iSize, false);
  }

  protected void setSize(long size, boolean force) throws IOException {
  }

  @Override
  public void writeHeaderLong(final int iPosition, final long iValue) throws IOException {
    acquireWriteLock();
    try {
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
      buffer.putLong(iValue);
      writeBuffer(buffer, HEADER_DATA_OFFSET + iPosition);
      setHeaderDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public long readHeaderLong(final int iPosition) throws IOException {
    acquireReadLock();
    try {
      return readData(HEADER_DATA_OFFSET + iPosition, OBinaryProtocol.SIZE_LONG).getLong();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public boolean isSoftlyClosed() throws IOException {
    acquireReadLock();
    try {
      final ByteBuffer buffer;
      if (version == 0)
        buffer = readData(SOFTLY_CLOSED_OFFSET_V_0, 1);
      else
        buffer = readData(SOFTLY_CLOSED_OFFSET, 1);

      return buffer.get(0) > 0;
    } finally {
      releaseReadLock();
    }
  }

  public void setSoftlyClosed(final boolean value) throws IOException {
    acquireWriteLock();
    try {
      if (channel == null || mode.indexOf('w') < 0)
        return;

      final ByteBuffer buffer = getBuffer(1);
      buffer.put(0, (byte) (value ? 1 : 0));

      writeBuffer(buffer, SOFTLY_CLOSED_OFFSET);

      try {
        channel.force(true);
      } catch (IOException e) {
        OLogManager.instance().warn(this, "Error during flush of file %s. Data may be lost in case of power failure.", getName());
      }
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  protected long checkRegions(final long iOffset, final long iLength) {
    acquireReadLock();
    try {
      if (iOffset < 0 || iOffset + iLength > getFilledUpTo())
        throw new OIOException("You cannot access outside the file size (" + getFilledUpTo()
            + " bytes). You have requested portion " + iOffset + "-" + (iOffset + iLength) + " bytes. File: " + toString());

      return iOffset + HEADER_SIZE;
    } finally {
      releaseReadLock();
    }

  }

  private ByteBuffer readData(final long iOffset, final int iSize) throws IOException {
    ByteBuffer buffer = getBuffer(iSize);
    channel.read(buffer, iOffset);
    buffer.rewind();
    return buffer;
  }

  private void writeBuffer(final ByteBuffer iBuffer, final long iOffset) throws IOException {
    iBuffer.rewind();
    channel.write(iBuffer, iOffset);
  }

  private ByteBuffer getBuffer(final int iLenght) {
    return ByteBuffer.allocate(iLenght);
  }

  private ByteBuffer getWriteBuffer(final int iLenght) {
    setDirty();
    if (iLenght <= OBinaryProtocol.SIZE_LONG)
      // RECYCLE WRITE BYTE BUFFER SINCE WRITES ARE SYNCHRONIZED
      return (ByteBuffer) internalWriteBuffer.rewind();

    return getBuffer(iLenght);
  }

  protected void setVersion(int version) throws IOException {
    acquireWriteLock();
    try {
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
      buffer.put((byte) version);
      writeBuffer(buffer, VERSION_OFFSET);
      setHeaderDirty();
    } finally {
      releaseWriteLock();
    }
  }

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

      openChannel(-1);

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
        OLogManager.instance().error(this,
            "Invalid filledUp size (=" + filledUpTo + "). The file '" + getName() + "' could be corrupted", null,
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
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
   */
  public void close() throws IOException {
    acquireWriteLock();
    try {
      if (trackFileClose) {
        final Exception exception = new Exception();
        final StringWriter writer = new StringWriter();
        writer.append("File ").append(getName()).append(" was closed at : \r\n");

        final PrintWriter printWriter = new PrintWriter(writer);
        exception.printStackTrace(printWriter);
        printWriter.flush();

        OLogManager.instance().warn(this, writer.toString());
      }

      if (accessFile != null && (accessFile.length() - HEADER_SIZE) < getFileSize())
        accessFile.setLength(getFileSize() + HEADER_SIZE);

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
        boolean deleted = OFileUtils.delete(osFile);
        int retryCount = 0;

        while (!deleted) {
          deleted = OFileUtils.delete(osFile);
          retryCount++;

          if (retryCount > 10)
            throw new IOException("Can not delete file. Retry limit exceeded.");
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
          fileLock = channel.tryLock();
          if (fileLock != null)
            break;
        } catch (OverlappingFileLockException e) {
          OLogManager.instance().debug(this,
              "Cannot open file '" + osFile.getAbsolutePath() + "' because it is locked. Waiting %d ms and retrying %d/%d...",
              LOCK_WAIT_TIME, i, LOCK_MAX_RETRIES);
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

  public OFileClassic init(final String iFileName, final String iMode) {
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
          if (i == OPEN_RETRY_MAX - 1)
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

      if (newSize > -1 && accessFile.length() != newSize)
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

  public boolean renameTo(final File newFile) throws IOException {
    acquireWriteLock();
    try {
      close();

      final boolean renamed = OFileUtils.renameFile(osFile, newFile);
      if (renamed)
        osFile = new File(newFile.getAbsolutePath());

      open();

      return renamed;
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

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(128);
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
}
