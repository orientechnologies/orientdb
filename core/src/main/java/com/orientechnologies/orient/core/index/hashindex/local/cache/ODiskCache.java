package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;

import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;

/**
 * @author Andrey Lomakin
 * @since 14.03.13
 */
public interface ODiskCache {
  long openFile(OStorageSegmentConfiguration fileConfiguration, String fileExtension) throws IOException;

  long loadAndLockForWrite(long fileId, long pageIndex) throws IOException;

  long allocateAndLockForWrite(long fileId, long pageIndex) throws IOException;

  void cacheHit(long fileId, long pageIndex, long dataPointer) throws IOException;

  long getAndLockForWrite(long fileId, long pageIndex) throws IOException;

  void clearExternalManagementFlag(long fileId, long pageIndex) throws IOException;

  long loadAndLockForRead(long fileId, long pageIndex) throws IOException;

  void releaseReadLock(long fileId, long pageIndex);

  void releaseWriteLock(long fileId, long pageIndex);

  long getFilledUpTo(long fileId) throws IOException;

  void flushFile(long fileId) throws IOException;

  void flushFile(long fileId, boolean writeLock) throws IOException;

  void freePage(long fileId, long pageIndex);

  void closeFile(long fileId) throws IOException;

  void deleteFile(long fileId) throws IOException;

  void renameFile(long fileId, String oldFileName, String newFileName) throws IOException;

  void truncateFile(long fileId) throws IOException;

  boolean wasSoftlyClosed(long fileId) throws IOException;

  void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException;

  void flushBuffer() throws IOException;

  void flushBuffer(boolean writeLock) throws IOException;

  void clear() throws IOException;

  void close() throws IOException;

  void flushData(long fileId, long pageIndex, long dataPointer) throws IOException;
}
