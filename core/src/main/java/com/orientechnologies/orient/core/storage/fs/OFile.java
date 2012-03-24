/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.fs;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;

/**
 * Interface to represent low-level File access. To use 3rd party implementations register them to the {@link OFileFactory}
 * singleton instance.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OFile {

	/**
	 * Initializes the file passing name and open mode.
	 * 
	 * @param iFileName
	 *          File name
	 * @param iOpenMode
	 *          Opening mode between "r" = read-only and "rw" for read write
	 * @return
	 */
	public abstract OFile init(String iFileName, String iOpenMode);

	/**
	 * Opens the file.
	 * 
	 * @return
	 * @throws IOException
	 */
	public abstract boolean open() throws IOException;

	/**
	 * Creates the file.
	 * 
	 * @param iStartSize
	 * @throws IOException
	 */
	public abstract void create(int iStartSize) throws IOException;

	/**
	 * Closes the file.
	 * 
	 * @throws IOException
	 */
	public abstract void close() throws IOException;

	/**
	 * Deletes the file.
	 * 
	 * @throws IOException
	 */
	public abstract void delete() throws IOException;

	public abstract void setSize(int iSize) throws IOException;

	public abstract void writeHeaderLong(int iPosition, long iValue) throws IOException;

	public abstract long readHeaderLong(int iPosition) throws IOException;

	public abstract void synch() throws IOException;

	public abstract void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException;

	public abstract short readShort(long iLogicalPosition) throws IOException;

	public abstract int readInt(long iLogicalPosition) throws IOException;

	public abstract long readLong(long iOffset) throws IOException;

	public abstract byte readByte(long iOffset) throws IOException;

	public abstract void writeInt(long iOffset, int iValue) throws IOException;

	public abstract void writeLong(long iOffset, long iValue) throws IOException;

	public abstract void writeShort(long iOffset, short iValue) throws IOException;

	public abstract void writeByte(long iOffset, byte iValue) throws IOException;

	public abstract void write(long iOffset, byte[] iSourceBuffer) throws IOException;

	public abstract void lock() throws IOException;

	public FileLock lock(final long iRangeFrom, final long iRangeSize, final boolean iShared) throws IOException;

	public OFile unlock(final FileLock iLock) throws IOException;

	public abstract void unlock() throws IOException;

	/**
	 * Cuts bytes from the tail of the file reducing the filledUpTo size.
	 * 
	 * @param iSizeToShrink
	 * @throws IOException
	 */
	public abstract void removeTail(int iSizeToShrink) throws IOException;

	/**
	 * Shrink the file content (filledUpTo attribute only)
	 * 
	 * @param iSize
	 * @throws IOException
	 */
	public abstract void shrink(final int iSize) throws IOException;

	public abstract String getName();

	public abstract String getPath();

	public abstract String getAbsolutePath();

	public abstract boolean renameTo(File newFile);

	public abstract int allocateSpace(final int iSize) throws IOException;

	public abstract int getFreeSpace();

	public abstract int getFileSize();

	public abstract int getFilledUpTo();

	public abstract boolean canOversize(final int iRecordSize);

	public abstract String toString();

	public abstract int getMaxSize();

	public abstract void setMaxSize(int maxSize);

	public abstract int getIncrementSize();

	public abstract void setIncrementSize(int incrementSize);

	public abstract boolean isOpen();

	public abstract boolean exists();

	public abstract boolean isFailCheck();

	public abstract void setFailCheck(boolean failCheck);
}