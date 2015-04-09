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
package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;

import java.io.IOException;

/**
 * This class is heart of OrientDB storage model it presents disk backed data cache which works with direct memory.
 * 
 * Model of this cache is based on page model. All direct memory area is mapped to disk files and each file is split on pages. Page
 * is smallest unit of work. The amount of RAM which can be used for data manipulation is limited so only a subset of data will be
 * really loaded into RAM on demand, if there is not enough RAM to store all data, part of them will by flushed to the disk. If disk
 * cache is closed all changes will be flushed to the disk.
 * 
 * Typical steps if you work with disk cache are following:
 * <ol>
 * <li>Open file using {@link #openFile(String)} method</li>
 * <li>Remember id of opened file</li>
 * <li>Load page which you want to use to write data using method {@link #load(long, long, boolean)}</li>
 * <li>Get pointer to the memory page {@link OCacheEntry#getCachePointer()}</li>
 * <li>Lock allocated page for writes {@link OCachePointer#acquireExclusiveLock()}</li>
 * <li>Get pointer to the direct memory which is allocated to hold page data {@link OCachePointer#getDataPointer()}</li>
 * <li>Change page content as you wish.</li>
 * <li>Release page write lock {@link OCachePointer#releaseExclusiveLock()}</li>
 * <li>Mark page as dirty so it will be flushed eventually to the disk {@link OCacheEntry#markDirty()}</li>
 * <li>Put page back to the cache {@link #release(OCacheEntry)}</li>
 * </ol>
 * 
 * If you wish to read data, not change them, you use the same steps but:
 * <ol>
 * <li>Acquire read lock instead of write lock using {@link OCachePointer#acquireSharedLock()}</li> method.
 * <li>Do not mark page as dirty</li>
 * </ol>
 * 
 * If you want to add new data but not to change existing one and you do not have enough space to add new data use method
 * {@link #allocateNewPage(long)} instead of {@link #load(long, long, boolean)}.
 * 
 * {@link #load(long, long, boolean)} method has checkPinnedPages parameter. Pinned pages are pages which are kept always loaded in
 * RAM ,this class of pages is needed for some data structures usually this attribute should be set to <code>false</code> and it is
 * set to <code>true</code> when storage goes through data restore procedure after system crash.
 * 
 * @author Andrey Lomakin
 * @since 14.03.13
 */
public interface ODiskCache {
  long addFile(String fileName) throws IOException;

  void addFile(String fileName, long fileId) throws IOException;

  long openFile(String fileName) throws IOException;

  void openFile(long fileId) throws IOException;

  void openFile(String fileName, long fileId) throws IOException;

  long bookFileId(String fileName) throws IOException;

  OCacheEntry load(long fileId, long pageIndex, boolean checkPinnedPages) throws IOException;

  void pinPage(OCacheEntry cacheEntry) throws IOException;

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

  void close() throws IOException;

  void delete() throws IOException;

  OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener);

  boolean isOpen(long fileId);

  boolean exists(String name);

  boolean exists(long fileId);

  String fileNameById(long fileId);

  void lock() throws IOException;

  void unlock() throws IOException;

  void addLowDiskSpaceListener(OLowDiskSpaceListener listener);

  void removeLowDiskSpaceListener(OLowDiskSpaceListener listener);

  long getUsedMemory();

  void startFuzzyCheckpoints();

  boolean checkLowDiskSpace();

  void makeFuzzyCheckpoint();
}
