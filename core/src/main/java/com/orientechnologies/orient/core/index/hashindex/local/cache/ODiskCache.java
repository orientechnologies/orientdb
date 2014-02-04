package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;

/**
 * @author Andrey Lomakin
 * @since 14.03.13
 */
public interface ODiskCache {
  long openFile(String fileName) throws IOException;

  void openFile(long fileId) throws IOException;

  OCacheEntry load(long fileId, long pageIndex, boolean checkPinnedPages) throws IOException;

  void pinPage(OCacheEntry cacheEntry) throws IOException;

  void loadPinnedPage(OCacheEntry cacheEntry) throws IOException;

  OCacheEntry allocateNewPage(long fileId) throws IOException;

  void release(OCacheEntry cacheEntry);

  long getFilledUpTo(long fileId) throws IOException;

  void flushFile(long fileId) throws IOException;

  void closeFile(long fileId) throws IOException;

  void closeFile(long fileId, boolean flush) throws IOException;

  void deleteFile(long fileId) throws IOException;

  void renameFile(long fileId, String oldFileName, String newFileName) throws IOException;

  void truncateFile(long fileId) throws IOException;

  boolean wasSoftlyClosed(long fileId) throws IOException;

  void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException;

  void setSoftlyClosed(boolean softlyClosed) throws IOException;

  void flushBuffer() throws IOException;

  void clear() throws IOException;

  void close() throws IOException;

  void delete() throws IOException;

  OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener);

  Set<ODirtyPage> logDirtyPagesTable() throws IOException;

  void forceSyncStoredChanges() throws IOException;

  boolean isOpen(long fileId);

  boolean exists(String name);

  String fileNameById(long fileId);
}
