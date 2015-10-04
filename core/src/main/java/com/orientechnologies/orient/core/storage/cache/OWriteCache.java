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

package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;

import java.io.IOException;
import java.util.concurrent.Future;

public interface OWriteCache {
  void startFuzzyCheckpoints();

  void addLowDiskSpaceListener(OLowDiskSpaceListener listener);

  void removeLowDiskSpaceListener(OLowDiskSpaceListener listener);

  long bookFileId(String fileName) throws IOException;

  long openFile(String fileName) throws IOException;

  void openFile(long fileId) throws IOException;

  void openFile(String fileName, long fileId) throws IOException;

  long addFile(String fileName) throws IOException;

  void addFile(String fileName, long fileId) throws IOException;

  boolean checkLowDiskSpace();

  void makeFuzzyCheckpoint();

  void lock() throws IOException;

  void unlock() throws IOException;

  boolean exists(String fileName);

  boolean exists(long fileId);

  Future store(long fileId, long pageIndex, OCachePointer dataPointer);

  OCachePointer load(long fileId, long pageIndex, boolean addNewPages) throws IOException;

  void flush(long fileId);

  void flush();

  long getFilledUpTo(long fileId) throws IOException;

  long getExclusiveWriteCachePagesSize();

  boolean isOpen(long fileId);

  Long isOpen(String fileName) throws IOException;

  void deleteFile(long fileId) throws IOException;

  void truncateFile(long fileId) throws IOException;

  void renameFile(long fileId, String oldFileName, String newFileName) throws IOException;

  long[] close() throws IOException;

  void close(long fileId, boolean flush) throws IOException;

  OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener);

  long[] delete() throws IOException;

  String fileNameById(long fileId);

  int getId();
}
