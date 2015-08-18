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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 5/6/14
 */
public class OPaginatedStorageDirtyFlag {
  private final String     dirtyFilePath;

  private File             dirtyFile;
  private RandomAccessFile dirtyFileData;
  private volatile boolean dirtyFlag;

  private final Lock       lock = new ReentrantLock();

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

      dirtyFileData.seek(0);
      dirtyFileData.writeBoolean(true);
      dirtyFlag = true;

    } finally {
      lock.unlock();
    }
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
      if (!dirtyFile.exists())
        throw new IllegalStateException("File '" + dirtyFilePath + "' does not exist.");

      dirtyFileData = new RandomAccessFile(dirtyFile, "rwd");

      dirtyFileData.seek(0);
      dirtyFlag = dirtyFileData.readBoolean();
    } finally {
      lock.unlock();
    }
  }

  public void close() throws IOException {
    lock.lock();
    try {
      if (dirtyFile == null)
        return;

      if (dirtyFile.exists())
        dirtyFileData.close();

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

      dirtyFileData.seek(0);
      dirtyFileData.writeBoolean(true);
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

      dirtyFileData.seek(0);
      dirtyFileData.writeBoolean(false);
      dirtyFlag = false;
    } finally {
      lock.unlock();
    }
  }

  public boolean isDirty() {
    return dirtyFlag;
  }

}
