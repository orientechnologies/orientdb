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
package com.orientechnologies.orient.core.storage.fs;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;

/**
 * Interface to represent low-level File access.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OFile {
  /**
   * Opens the file.
   * 
   * @return
   * @throws IOException
   */
  boolean open() throws IOException;

  /**
   * Creates the file.
   * 
   * @throws IOException
   */
  void create() throws IOException;

  /**
   * Closes the file.
   * 
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Deletes the file.
   * 
   * @throws IOException
   */
  void delete() throws IOException;

  boolean synch() throws IOException;

  void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException;

  short readShort(long iLogicalPosition) throws IOException;

  int readInt(long iLogicalPosition) throws IOException;

  long readLong(long iOffset) throws IOException;

  byte readByte(long iOffset) throws IOException;

  void writeInt(long iOffset, int iValue) throws IOException;

  void writeLong(long iOffset, long iValue) throws IOException;

  void writeShort(long iOffset, short iValue) throws IOException;

  void writeByte(long iOffset, byte iValue) throws IOException;

  long write(long iOffset, byte[] iSourceBuffer) throws IOException;

  void lock() throws IOException;

  FileLock lock(final long iRangeFrom, final long iRangeSize, final boolean iShared) throws IOException;

  OFile unlock(final FileLock iLock) throws IOException;

  void unlock() throws IOException;


  /**
   * Shrink the file content (filledUpTo attribute only)
   * 
   * 
   * @param iSize
   * @throws IOException
   */
  void shrink(final long iSize) throws IOException;

  String getName();

  String getPath();

  String getAbsolutePath();

  boolean renameTo(File newFile) throws IOException;

  long allocateSpace(final long iSize) throws IOException;

  long getFileSize();

  String toString();

  boolean isOpen();

  boolean exists();

  void read(long iOffset, byte[] iData, int iLength, int iArrayOffset) throws IOException;

  void write(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException;
}
