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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/6/14
 */
public class OPaginatedStorageDirtyFlag {
  private Path        dirtyFilePath;
  private FileChannel channel;
  private FileLock    fileLock;

  private volatile boolean dirtyFlag;

  private final Lock lock = new ReentrantLock();

  public OPaginatedStorageDirtyFlag(Path dirtyFilePath) {
    this.dirtyFilePath = dirtyFilePath;
  }

  public void create() throws IOException {
    lock.lock();
    try {

      if (Files.exists(dirtyFilePath)) {
        Files.delete(dirtyFilePath);
      }

      channel = FileChannel.open(dirtyFilePath, StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean()) {
        lockFile();
      }

      final ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 1);
      buffer.position(0);

      OIOUtils.writeByteBuffer(buffer, channel, 0);

      dirtyFlag = true;
    } finally {
      lock.unlock();
    }
  }

  private void lockFile() throws IOException {
    try {
      fileLock = channel.tryLock();
    } catch (OverlappingFileLockException e) {
      OLogManager.instance().warn(this, "File is already locked by other thread", e);
    }

    if (fileLock == null)
      throw new OStorageException("Database is locked by another process, please shutdown process and try again");
  }

  public boolean exists() {
    lock.lock();
    try {
      return Files.exists(dirtyFilePath);
    } finally {
      lock.unlock();
    }
  }

  public void open() throws IOException {
    lock.lock();
    try {
      if (!Files.exists(dirtyFilePath)) {
        channel = FileChannel.open(dirtyFilePath, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);

        ByteBuffer buffer = ByteBuffer.allocate(1);
        OIOUtils.writeByteBuffer(buffer, channel, 0);
      } else {
        channel = FileChannel.open(dirtyFilePath, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
      }

      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean()) {
        lockFile();
      }

      ByteBuffer buffer = ByteBuffer.allocate(1);
      OIOUtils.readByteBuffer(buffer, channel, 0);

      buffer.position(0);
      dirtyFlag = buffer.get() > 0;
    } finally {
      lock.unlock();
    }
  }

  public void close() throws IOException {
    lock.lock();
    try {
      if (channel == null)
        return;

      if (Files.exists(dirtyFilePath)) {
        if (fileLock != null) {
          fileLock.release();
          fileLock = null;
        }

        channel.close();
        channel = null;
      }

    } finally {
      lock.unlock();
    }
  }

  public void delete() throws IOException {
    lock.lock();
    try {
      if (channel == null)
        return;

      if (Files.exists(dirtyFilePath)) {

        if (fileLock != null) {
          fileLock.release();
          fileLock = null;
        }

        channel.close();
        channel = null;

        Files.delete(dirtyFilePath);
      }
    } finally {
      lock.unlock();
    }
  }

  public void makeDirty() throws IOException {
    if (dirtyFlag)
      return;

    lock.lock();
    try {
      if (dirtyFlag)
        return;

      final ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 1);
      buffer.position(0);

      OIOUtils.writeByteBuffer(buffer, channel, 0);

      dirtyFlag = true;
    } finally {
      lock.unlock();
    }
  }

  public void clearDirty() throws IOException {
    if (!dirtyFlag)
      return;

    lock.lock();
    try {
      if (!dirtyFlag)
        return;

      final ByteBuffer buffer = ByteBuffer.allocate(1);
      OIOUtils.writeByteBuffer(buffer, channel, 0);

      dirtyFlag = false;
    } finally {
      lock.unlock();
    }
  }

  public boolean isDirty() {
    return dirtyFlag;
  }

}
