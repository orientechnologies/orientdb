package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.io.IOException;
import java.util.Set;

public interface OAtomicOperation {
  OOperationUnitId getOperationUnitId();

  OCacheEntry loadPageForWrite(long fileId, long pageIndex, boolean checkPinnedPages, int pageCount, boolean verifyChecksum) throws
      IOException;

  void addComponentOperation(OComponentOperationRecord componentOperation) throws IOException;

  OCacheEntry loadPageForRead(long fileId, long pageIndex, boolean checkPinnedPages, int pageCount)
      throws IOException;

  void addMetadata(OAtomicOperationMetadata<?> metadata);

  OAtomicOperationMetadata<?> getMetadata(String key);

  void addDeletedRidBag(OBonsaiBucketPointer rootPointer);

  Set<OBonsaiBucketPointer> getDeletedBonsaiPointers();

  OCacheEntry addPage(long fileId);

  void releasePageFromRead(OCacheEntry cacheEntry);

  void releasePageFromWrite(OCacheEntry cacheEntry);

  long filledUpTo(long fileId);

  long addFile(String fileName);

  long loadFile(String fileName) throws IOException;

  void deleteFile(long fileId);

  boolean isFileExists(String fileName);

  String fileNameById(long fileId);

  void truncateFile(long fileId);

  int getCounter();
}
