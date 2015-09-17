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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
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
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

public class OFileClassic implements OFile {
  private static final boolean trackFileClose           = OGlobalConfiguration.TRACK_FILE_CLOSE.getValueAsBoolean();

  public final static String   NAME                     = "classic";
  public static final int      HEADER_SIZE              = 1024;
  private static final int     SOFTLY_CLOSED_OFFSET_V_0 = 8;
  private static final int     SOFTLY_CLOSED_OFFSET     = 16;
  private static final int     VERSION_OFFSET           = 48;
  private static final int     CURRENT_VERSION          = 1;
  private static final int     OPEN_RETRY_MAX           = 10;
  private static final int     OPEN_DELAY_RETRY         = 100;
  private static final long    LOCK_WAIT_TIME           = 300;
  private static final int     LOCK_MAX_RETRIES         = 10;
  private final ReadWriteLock  lock                     = new ReentrantReadWriteLock();
  private ByteBuffer           internalWriteBuffer      = ByteBuffer.allocate(OBinaryProtocol.SIZE_LONG);

  private volatile File        osFile;
  private final String         mode;

  private RandomAccessFile     accessFile;
  private FileChannel          channel;
  private volatile boolean     dirty                    = false;
  private volatile boolean     headerDirty              = false;
  private int                  version;

  private boolean              failCheck                = true;
  private volatile long        size;                                                                                // PART OF
                                                                                                                     // HEADER (4
                                                                                                                     // bytes)
  private FileLock             fileLock;
  private boolean              wasSoftlyClosed          = true;

  public OFileClassic(String osFile, String mode) {
    this.mode = mode;
    this.osFile = new File(osFile);
  }

  @Override
  public long allocateSpace(long size) {
    acquireWriteLock();
    try {
      final long currentSize = this.size;
      this.size += size;

      assert this.size >= size;

      return currentSize;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void shrink(long iSize) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          channel.truncate(HEADER_SIZE + iSize);
          size = iSize;

          assert size >= 0;
          break;

        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during file shrink for file " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public long getFileSize() {
    return size;
  }

  public void read(long iOffset, byte[] iData, int iLength, int iArrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, iLength);

          ByteBuffer buffer = ByteBuffer.wrap(iData, iArrayOffset, iLength);
          channel.read(buffer, iOffset);
          break;

        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data read for file " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  public void write(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          writeInternal(iOffset, iData, iSize, iArrayOffset);
          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data write for file  " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
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
    int attempts = 0;
    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
          return readData(iOffset, OBinaryProtocol.SIZE_INT).getInt();
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during read of int data for file " + getName() + " " + attempts + "-th attempt.",
            e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public long readLong(long iOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
          return readData(iOffset, OBinaryProtocol.SIZE_LONG).getLong();
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during read of long data for file " + getName() + " " + attempts + "-th attempt",
            e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public short readShort(long iOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
          return readData(iOffset, OBinaryProtocol.SIZE_SHORT).getShort();
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during read of short data for file " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public byte readByte(long iOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
          return readData(iOffset, OBinaryProtocol.SIZE_BYTE).get();
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during read of byte data for file " + getName() + " " + attempts + "-th attempt",
            e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeInt(long iOffset, final int iValue) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;

          final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_INT);
          buffer.putInt(iValue);
          writeBuffer(buffer, iOffset);
          setDirty();

          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during write of int data for file " + getName() + " " + attempts + "-th attempt",
            e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeLong(long iOffset, final long iValue) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;
          final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
          buffer.putLong(iValue);
          writeBuffer(buffer, iOffset);
          setDirty();
          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during write of long data for file " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeShort(long iOffset, final short iValue) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;
          final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_SHORT);
          buffer.putShort(iValue);
          writeBuffer(buffer, iOffset);
          setDirty();
          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during write of short data for file " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeByte(long iOffset, final byte iValue) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;
          final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
          buffer.put(iValue);
          writeBuffer(buffer, iOffset);
          setDirty();
          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during write of byte data for file " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }

  }

  @Override
  public long write(long iOffset, final byte[] iSourceBuffer) throws IOException {
    int attempts = 0;
    while (true) {
      try {
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
          attempts++;
        }

        return allocationDiff;
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during write of data for file " + getName() + " " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
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
          OLogManager.instance().warn(this, "Error during flush of file %s. Data may be lost in case of power failure.", getName(),
              e);
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
      openChannel();
      init(HEADER_SIZE);

      setVersion(OFileClassic.CURRENT_VERSION);
      version = OFileClassic.CURRENT_VERSION;
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  private long checkRegions(final long iOffset, final long iLength) {
    acquireReadLock();
    try {
      if (iOffset < 0 || iOffset + iLength > size)
        throw new OIOException("You cannot access outside the file size (" + size + " bytes). You have requested portion "
            + iOffset + "-" + (iOffset + iLength) + " bytes. File: " + toString());

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

  private void setVersion(int version) throws IOException {
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

      openChannel();
      init(-1);

      OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getName() + "...");

      if (version < CURRENT_VERSION) {
        setVersion(CURRENT_VERSION);
        version = CURRENT_VERSION;
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
            throw new IOException("Cannot delete file. Retry limit exceeded");
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
                  + "' is locked by another process, maybe the database is in use by another process. Use the remote mode with a OrientDB server to allow multiple access to the same database");
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

  private void openChannel() throws IOException {
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

      channel = accessFile.getChannel();

      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
        lock();
    } finally {
      releaseWriteLock();
    }
  }

  private void init(long newSize) throws IOException {
    if (newSize > -1 && accessFile.length() != newSize)
      accessFile.setLength(newSize);

    size = accessFile.length() - HEADER_SIZE;
    assert size >= 0;

    accessFile.seek(VERSION_OFFSET);
    version = accessFile.read();
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

  private void setDirty() {
    acquireWriteLock();
    try {
      if (!dirty)
        dirty = true;
    } finally {
      releaseWriteLock();
    }
  }

  private void setHeaderDirty() {
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
      if (osFile == null)
        return null;

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

  private void acquireWriteLock() {
    lock.writeLock().lock();
  }

  private void releaseWriteLock() {
    lock.writeLock().unlock();
  }

  private void acquireReadLock() {
    lock.readLock().lock();
  }

  private void releaseReadLock() {
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
    builder.append("");
    return builder.toString();
  }

  private void reopenFile(int attempt, IOException e) throws IOException {
    if (attempt > 1)
      throw e;

    acquireWriteLock();
    try {
      try {
        unlock();
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Error during unlock of file " + osFile.getName() + ", during IO exception handling.",
            ioe);
      }

      try {
        channel.close();
      } catch (IOException ioe) {
        OLogManager.instance().error(this,
            "Error during channel close for file " + osFile.getAbsolutePath() + ", during IO exception handling.", ioe);
      }

      try {
        accessFile.close();
      } catch (IOException ioe) {
        OLogManager.instance().error(this,
            "Error during close of file " + osFile.getAbsolutePath() + ", during IO exception handling.", ioe);
      }

      channel = null;
      accessFile = null;

      openChannel();
    } finally {
      releaseWriteLock();
    }
  }
}
