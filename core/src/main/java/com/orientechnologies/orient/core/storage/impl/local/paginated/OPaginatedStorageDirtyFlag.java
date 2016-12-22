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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 5/6/14
 */
public class OPaginatedStorageDirtyFlag {
  private final String dirtyFilePath;

  private File             dirtyFile;
  private RandomAccessFile dirtyFileData;
  private FileChannel      channel;
  private FileLock         fileLock;

  private volatile boolean dirtyFlag;

  private final Lock lock = new ReentrantLock();

  public OPaginatedStorageDirtyFlag(String dirtyFilePath) {
    this.dirtyFilePath = dirtyFilePath;
  }

  public void create() throws IOException {
    lock.lock();
    try {
      dirtyFile = new File(dirtyFilePath);

      if (dirtyFile.exists()) {
        final boolean fileDeleted = dirtyFile.delete();

        if (!fileDeleted)
          throw new IllegalStateException("Cannot delete file : " + dirtyFilePath);
      }

      final boolean fileCreated = dirtyFile.createNewFile();
      if (!fileCreated)
        throw new IllegalStateException("Cannot create file : " + dirtyFilePath);

      dirtyFileData = new RandomAccessFile(dirtyFile, "rwd");
      channel = dirtyFileData.getChannel();

      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean()) {
        lockFile();
      }

      channel.position(0);

      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.position(0);
      buffer.put((byte) 1);

      buffer.position(0);
      channel.write(buffer);

      dirtyFlag = true;
    } finally {
      lock.unlock();
    }
  }

  private void lockFile() throws IOException {
    StringWriter sw = new StringWriter();

    try {
      Thread thread = Thread.currentThread();
      long now = System.nanoTime();

      sw.append("Thread ").append(thread.getName()).append(" is going to acquire log at ").append(String.valueOf(now)).append("\n");

      fileLock = channel.tryLock();
      if (fileLock != null)
        sw.append("Attempt was successful\n");
      else
        sw.append("Other process holds given lock\n");

    } catch (OverlappingFileLockException e) {
      OLogManager.instance().warn(this, "Database is open by another process");

      sw.append("Database lock is overlapped\n");
    }

    OLogManager.instance().error(this, sw.toString());

    if (fileLock == null)
      throw new OStorageException("Can not open storage it is acquired by other process");
  }

  public boolean exists() {
    lock.lock();
    try {
      return new File(dirtyFilePath).exists();
    } finally {
      lock.unlock();
    }
  }

  public void open() throws IOException {
    lock.lock();
    try {
      dirtyFile = new File(dirtyFilePath);
      if (!dirtyFile.exists()) {
        final boolean fileCreated = dirtyFile.createNewFile();

        if (!fileCreated)
          throw new IllegalStateException("Cannot create file : " + dirtyFilePath);

        dirtyFileData = new RandomAccessFile(dirtyFile, "rwd");
        channel = dirtyFileData.getChannel();

        channel.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(1);
        channel.write(buffer);
      }

      dirtyFileData = new RandomAccessFile(dirtyFile, "rwd");
      channel = dirtyFileData.getChannel();

      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean()) {
        lockFile();
      }

      channel.position(0);
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.position(0);

      channel.read(buffer);
      buffer.position(0);

      dirtyFlag = buffer.get() > 0;
    } finally {
      lock.unlock();
    }
  }

  public void close() throws IOException {
    lock.lock();
    try {
      if (dirtyFile == null) {
        OLogManager.instance().error(this, "Dirty file is null , file lock is not released");
        return;
      }

      if (dirtyFile.exists()) {
        if (fileLock != null) {
          long now = System.nanoTime();
          Thread thread = Thread.currentThread();

          fileLock.release();
          fileLock = null;

          OLogManager.instance().error(this, "File lock is released by thread " + thread.getName() + " at " + now);
        } else {
          OLogManager.instance().error(this, "File lock is null , file lock is not released");
        }

        dirtyFileData.close();
      }

    } finally {
      lock.unlock();
    }
  }

  public void delete() throws IOException {
    lock.lock();
    try {
      if (dirtyFile == null)
        return;

      if (dirtyFile.exists()) {

        if (fileLock != null) {
          fileLock.release();
          fileLock = null;
        }

        channel.close();
        dirtyFileData.close();

        boolean deleted = dirtyFile.delete();
        while (!deleted) {
          deleted = !dirtyFile.exists() || dirtyFile.delete();
        }
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

      channel.position(0);

      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 1);
      buffer.position(0);
      channel.write(buffer);

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

      channel.position(0);
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 0);
      buffer.position(0);
      channel.write(buffer);

      dirtyFlag = false;
    } finally {
      lock.unlock();
    }
  }

  public boolean isDirty() {
    return dirtyFlag;
  }

}
