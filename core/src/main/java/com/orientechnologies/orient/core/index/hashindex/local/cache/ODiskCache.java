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

  void markDirty(long fileId, long pageIndex);

  long load(long fileId, long pageIndex) throws IOException;

  void release(long fileId, long pageIndex);

  long getFilledUpTo(long fileId) throws IOException;

  void flushFile(long fileId) throws IOException;

  void closeFile(long fileId) throws IOException;

  void closeFile(long fileId, boolean flush) throws IOException;

  void deleteFile(long fileId) throws IOException;

  void renameFile(long fileId, String oldFileName, String newFileName) throws IOException;

  void truncateFile(long fileId) throws IOException;

  boolean wasSoftlyClosed(long fileId) throws IOException;

  void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException;

  void flushBuffer() throws IOException;

  void clear() throws IOException;

  void close() throws IOException;

  OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener);

  Set<ODirtyPage> logDirtyPagesTable() throws IOException;

  void forceSyncStoredChanges() throws IOException;

  boolean isOpen(long fileId);

}
