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

package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.storage.cache.local.OBackgroundExceptionListener;
import com.orientechnologies.orient.core.storage.impl.local.OPageIsBrokenListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public interface OWriteCache {
  /** Adds listener which is called by cache if corruption of file page is detected. */
  void addPageIsBrokenListener(OPageIsBrokenListener listener);

  /** Removes listener which is called by cache if corruption of file page is detected. */
  @SuppressWarnings("unused")
  void removePageIsBrokenListener(OPageIsBrokenListener listener);

  long bookFileId(String fileName);

  /**
   * Registers new file in write cache and returns file id assigned to this file.
   *
   * <p>File id consist of two parts:
   *
   * <ol>
   *   <li>Internal id is permanent and can not be changed during life of storage {@link
   *       #internalFileId(long)}
   *   <li>Write cache id which is changed between storage open/close cycles
   * </ol>
   *
   * <p>If file with the same name is deleted and then new file is created this file with have the
   * same internal id.
   *
   * @param fileName Name of file to register inside storage.
   * @return Id of registered file
   */
  long loadFile(String fileName) throws IOException;

  long addFile(String fileName) throws IOException;

  long addFile(String fileName, long fileId) throws IOException;

  /**
   * Returns id associated with given file or value &lt; 0 if such file does not exist.
   *
   * @param fileName File name id of which has to be returned.
   * @return id associated with given file or value &lt; 0 if such file does not exist.
   */
  long fileIdByName(String fileName);

  boolean checkLowDiskSpace() throws IOException;

  void syncDataFiles(long segmentId, byte[] lastMetadata) throws IOException;

  void flushTillSegment(long segmentId);

  boolean exists(String fileName);

  boolean exists(long fileId);

  void restoreModeOn() throws IOException;

  void restoreModeOff();

  void store(long fileId, long pageIndex, OCachePointer dataPointer);

  void checkCacheOverflow() throws InterruptedException;

  int allocateNewPage(final long fileId) throws IOException;

  OCachePointer load(
      long fileId, long startPageIndex, OModifiableBoolean cacheHit, boolean verifyChecksums)
      throws IOException;

  void flush(long fileId);

  void flush();

  long getFilledUpTo(long fileId);

  long getExclusiveWriteCachePagesSize();

  void deleteFile(long fileId) throws IOException;

  void truncateFile(long fileId) throws IOException;

  void renameFile(long fileId, String newFileName) throws IOException;

  long[] close() throws IOException;

  void close(long fileId, boolean flush);

  OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener);

  long[] delete() throws IOException;

  String fileNameById(long fileId);

  /**
   * Obtains native file name by the given file id.
   *
   * <p>Native file name is a file name of a "physical" on-disk file, it may differ from the
   * "virtual" logical file name.
   *
   * @param fileId the file id to obtain the native file name of.
   * @return the obtained native file name or {@code null} if the passed file id doesn't correspond
   *     to any file.
   */
  String nativeFileNameById(long fileId);

  int getId();

  Map<String, Long> files();

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   *
   * @return Size of page inside of cache.
   */
  int pageSize();

  /**
   * Finds if there was file in write cache with given id which is deleted right now. If such file
   * exists it creates new file with the same name at it was in deleted file.
   *
   * @param fileId If of file which should be restored
   * @return Name of restored file or <code>null</code> if such name does not exist
   */
  String restoreFileById(long fileId) throws IOException;

  /**
   * Adds listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to trigger
   */
  void addBackgroundExceptionListener(OBackgroundExceptionListener listener);

  /**
   * Removes listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to remove
   */
  void removeBackgroundExceptionListener(OBackgroundExceptionListener listener);

  /**
   * Directory which contains all files managed by write cache.
   *
   * @return Directory which contains all files managed by write cache or <code>null</code> in case
   *     of in memory database.
   */
  Path getRootDirectory();

  /**
   * Returns internal file id which is unique and always the same for given file in contrary to
   * external id which changes over close/open cycle of cache.
   *
   * @param fileId External file id.
   * @return Internal file id.
   */
  int internalFileId(long fileId);

  /**
   * Converts unique internal file id to external one. External id is combination of internal id and
   * write cache id, which changes every time when cache is closed and opened again.
   *
   * @param fileId Internal file id.
   * @return External file id.
   * @see #internalFileId(long)
   * @see #getId()
   */
  long externalFileId(int fileId);

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   *
   * <p>Takes two ids and checks whether they are equal from point of view of write cache. In other
   * words methods checks whether two ids in reality contain the same internal ids.
   */
  boolean fileIdsAreEqual(long firsId, long secondId);

  Long getMinimalNotFlushedSegment();

  void updateDirtyPagesTable(OCachePointer pointer, OLogSequenceNumber startLSN);

  void create() throws IOException;

  void open() throws IOException;

  void replaceFileId(long fileId, long newFileId) throws IOException;
}
