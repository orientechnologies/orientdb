package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 5/6/14
 */
public class OPaginatedStorageDirtyFlag {
  private final String        dirtyFilePath;

  private File                dirtyFile;
  private RandomAccessFile    dirtyFileData;
  private boolean             dirtyFlag;

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  public OPaginatedStorageDirtyFlag(String dirtyFilePath) {
    this.dirtyFilePath = dirtyFilePath;
  }

  public void create() throws IOException {
    readWriteLock.writeLock().lock();
    try {
      dirtyFile = new File(dirtyFilePath);

      if (dirtyFile.exists()) {
        final boolean fileDeleted = dirtyFile.delete();

        if (!fileDeleted)
          throw new IllegalStateException("Can not delete file : " + dirtyFilePath);
      }

      final boolean fileCreated = dirtyFile.createNewFile();
      if (!fileCreated)
        throw new IllegalStateException("Can not create file : " + dirtyFilePath);

      dirtyFileData = new RandomAccessFile(dirtyFile, "rwd");

      dirtyFileData.seek(0);
      dirtyFileData.writeBoolean(true);
      dirtyFlag = true;

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public boolean exits() {
    readWriteLock.readLock().lock();
    try {
      return new File(dirtyFilePath).exists();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void open() throws IOException {
    readWriteLock.writeLock().lock();
    try {
      dirtyFile = new File(dirtyFilePath);
      if (!dirtyFile.exists())
        throw new IllegalStateException("File '" + dirtyFilePath + "' does not exist.");

      dirtyFileData = new RandomAccessFile(dirtyFile, "rwd");

      dirtyFileData.seek(0);
      dirtyFlag = dirtyFileData.readBoolean();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void close() throws IOException {
    readWriteLock.writeLock().lock();
    try {
      if (dirtyFile == null)
        return;

      if (dirtyFile.exists())
        dirtyFileData.close();

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void delete() throws IOException {
    readWriteLock.writeLock().lock();
    try {
      if (dirtyFile == null)
        return;

      if (dirtyFile.exists()) {

        dirtyFileData.close();

        boolean deleted = dirtyFile.delete();
        while (!deleted) {
          OMemoryWatchDog.freeMemoryForResourceCleanup(100);
          deleted = !dirtyFile.exists() || dirtyFile.delete();
        }
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void makeDirty() throws IOException {
    readWriteLock.writeLock().lock();
    try {
      if (dirtyFlag)
        return;

      dirtyFileData.seek(0);
      dirtyFileData.writeBoolean(true);
      dirtyFlag = true;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void clearDirty() throws IOException {
    readWriteLock.writeLock().lock();
    try {
      if (!dirtyFlag)
        return;

      dirtyFileData.seek(0);
      dirtyFileData.writeBoolean(false);
      dirtyFlag = false;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public boolean isDirty() {
    readWriteLock.readLock().lock();
    try {
      return dirtyFlag;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

}