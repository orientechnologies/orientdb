/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

/**
 * 
 * Header structure:<br/>
 * <br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * | FILE SIZE | FILLED UP TO | SOFTLY CLOSED | SECURITY CODE |<br/>
 * | 4 bytes . | 4 bytes .... | 1 byte ...... | 32 bytes .... |<br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * = 1024 bytes<br/>
 * <br/>
 */
public abstract class OAbstractFile implements OFile {
	private FileLock						fileLock;

	protected File							osFile;
	protected RandomAccessFile	accessFile;
	protected FileChannel				channel;
	protected volatile boolean	dirty										= false;
	protected volatile boolean	headerDirty							= false;

	protected int								incrementSize						= DEFAULT_INCREMENT_SIZE;
	protected int								maxSize;
	protected int								size;																						// PART OF HEADER (4 bytes)
	protected int								filledUpTo;																			// PART OF HEADER (4 bytes)
	protected byte[]						securityCode						= new byte[32];					// PART OF HEADER (32 bytes)
	protected String						mode;
	protected boolean						failCheck								= true;

	protected static final int	HEADER_SIZE							= 1024;
	protected static final int	HEADER_DATA_OFFSET			= 128;
	protected static final int	DEFAULT_SIZE						= 1024000;
	protected static final int	DEFAULT_INCREMENT_SIZE	= -50;										// NEGATIVE NUMBER MEANS AS PERCENT OF CURRENT SIZE

	private static final int		OPEN_RETRY_MAX					= 10;
	private static final int		OPEN_DELAY_RETRY				= 100;

	private static final long		LOCK_WAIT_TIME					= 300;
	private static final int		LOCK_MAX_RETRIES				= 10;

	protected static final int	SIZE_OFFSET							= 0;
	protected static final int	FILLEDUPTO_OFFSET				= 4;
	protected static final int	SOFTLY_CLOSED_OFFSET		= 8;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#setSize(int)
	 */
	public abstract void setSize(int iSize) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#writeHeaderLong(int, long)
	 */
	public abstract void writeHeaderLong(int iPosition, long iValue) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#readHeaderLong(int)
	 */
	public abstract long readHeaderLong(int iPosition) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#synch()
	 */
	public abstract void synch() throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#read(long, byte[], int)
	 */
	public abstract void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#readShort(long)
	 */
	public abstract short readShort(long iLogicalPosition) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#readInt(long)
	 */
	public abstract int readInt(long iLogicalPosition) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#readLong(long)
	 */
	public abstract long readLong(long iOffset) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#readByte(long)
	 */
	public abstract byte readByte(long iOffset) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#writeInt(long, int)
	 */
	public abstract void writeInt(long iOffset, int iValue) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#writeLong(long, long)
	 */
	public abstract void writeLong(long iOffset, long iValue) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#writeShort(long, short)
	 */
	public abstract void writeShort(long iOffset, short iValue) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#writeByte(long, byte)
	 */
	public abstract void writeByte(long iOffset, byte iValue) throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#write(long, byte[])
	 */
	public abstract void write(long iOffset, byte[] iSourceBuffer) throws IOException;

	protected abstract void setSoftlyClosed(boolean b) throws IOException;

	protected abstract boolean isSoftlyClosed() throws IOException;

	protected abstract void init() throws IOException;

	protected abstract void setFilledUpTo(int iHow) throws IOException;

	protected abstract void flushHeader() throws IOException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#open()
	 */
	public boolean open() throws IOException {
		if (!osFile.exists() || osFile.length() == 0)
			throw new FileNotFoundException("File: " + osFile.getAbsolutePath());

		openChannel((int) osFile.length());

		OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getName() + "...");

		final int fileSize = size;
		init();

		if (filledUpTo > 0 && filledUpTo > size) {
			OLogManager
					.instance()
					.warn(
							this,
							"Invalid filledUp value (%d) for file %s. Resetting the file size %d to the os file size: %d. Probably the file was not closed correctly last time",
							filledUpTo, getOsFile().getAbsolutePath(), size, fileSize);
			setSize(fileSize);
		}

		if (filledUpTo > size || filledUpTo < 0)
			OLogManager.instance().error(this, "Invalid filledUp size (=" + filledUpTo + "). The file could be corrupted", null,
					OStorageException.class);

		if (failCheck) {
			boolean softlyClosed = isSoftlyClosed();

			if (softlyClosed)
				setSoftlyClosed(false);

			return softlyClosed;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#create(int)
	 */
	public void create(int iStartSize) throws IOException {
		if (iStartSize == -1)
			iStartSize = DEFAULT_SIZE;

		openChannel(iStartSize);

		setFilledUpTo(0);
		setSize(maxSize > 0 && iStartSize > maxSize ? maxSize : iStartSize);
		setSoftlyClosed(!failCheck);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
	 */
	public void close() throws IOException {
		try {
			if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
				unlock();
			if (channel != null && channel.isOpen()) {
				channel.close();
				channel = null;
			}

			if (accessFile != null) {
				accessFile.close();
				accessFile = null;
			}

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on closing file " + osFile.getAbsolutePath(), e, OIOException.class);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#delete()
	 */
	public void delete() throws IOException {
		close();
		if (osFile != null) {
			boolean deleted = osFile.delete();
			while (!deleted) {
				OMemoryWatchDog.freeMemory(100);
				deleted = osFile.delete();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#lock()
	 */
	public void lock() throws IOException {
		for (int i = 0; i < LOCK_MAX_RETRIES; ++i) {
			try {
				fileLock = channel.tryLock(0, 1, false);
				break;
			} catch (OverlappingFileLockException e) {
				OLogManager.instance().debug(this,
						"Cannot open file '" + osFile.getAbsolutePath() + "' because it is locked. Waiting %d ms and retrying %d/%d...",
						LOCK_WAIT_TIME, i, LOCK_MAX_RETRIES);

				// FORCE FINALIZATION TO COLLECT ALL THE PENDING BUFFERS
				OMemoryWatchDog.freeMemory(LOCK_WAIT_TIME);
			}
		}

		if (fileLock == null)
			throw new OLockException(
					"File '"
							+ osFile.getPath()
							+ "' is locked by another process, maybe the database is in use by another process. Use the remote mode with a OrientDB server to allow multiple access to the same database.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#unlock()
	 */
	public void unlock() throws IOException {
		if (fileLock != null) {
			try {
				fileLock.release();
			} catch (ClosedChannelException e) {
			}
			fileLock = null;
		}
	}

	protected void checkSize(final int iSize) throws IOException {
		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this, "Changing file size to " + iSize + " bytes. " + toString());

		if (iSize < filledUpTo)
			OLogManager.instance().error(
					this,
					"You cannot resize down the file to " + iSize + " bytes, since it is less than current space used: " + filledUpTo
							+ " bytes", OIOException.class);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#removeTail(int)
	 */
	public void removeTail(int iSizeToShrink) throws IOException {
		if (filledUpTo < iSizeToShrink)
			iSizeToShrink = 0;

		setFilledUpTo(filledUpTo - iSizeToShrink);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#shrink(int)
	 */
	public void shrink(final int iSize) throws IOException {
		if (iSize >= filledUpTo)
			return;

		OLogManager.instance().debug(this, "Shrinking filled file from " + filledUpTo + " to " + iSize + " bytes. " + toString());

		setFilledUpTo(iSize);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#allocateSpace(int)
	 */
	public int allocateSpace(final int iSize) throws IOException {
		final int offset = filledUpTo;

		if (getFreeSpace() < iSize) {
			if (maxSize > 0 && maxSize - size < iSize)
				throw new IllegalArgumentException("Cannot enlarge file since the configured max size ("
						+ OFileUtils.getSizeAsString(maxSize) + ") was reached! " + toString());

			// MAKE ROOM
			int newFileSize = size;

			if (newFileSize == 0)
				// PROBABLY HAS BEEN LOST WITH HARD KILLS
				newFileSize = DEFAULT_SIZE;

			// GET THE STEP SIZE IN BYTES
			int stepSizeInBytes = incrementSize > 0 ? incrementSize : -1 * size / 100 * incrementSize;

			// FIND THE BEST SIZE TO ALLOCATE (BASED ON INCREMENT-SIZE)
			while (newFileSize - filledUpTo <= iSize) {
				newFileSize += stepSizeInBytes;

				if (newFileSize == 0)
					// EMPTY FILE: ALLOCATE REQUESTED SIZE ONLY
					newFileSize = iSize;
				if (newFileSize > maxSize && maxSize > 0)
					// TOO BIG: ROUND TO THE MAXIMUM FILE SIZE
					newFileSize = maxSize;
			}

			setSize(newFileSize);
		}

		// THERE IS SPACE IN FILE: RETURN THE UPPER BOUND OFFSET AND UPDATE THE FILLED THRESHOLD
		setFilledUpTo(filledUpTo + iSize);

		return offset;
	}

	protected long checkRegions(final long iOffset, final int iLength) {
		if (iOffset + iLength > filledUpTo)
			throw new OIOException("You cannot access outside the file size (" + filledUpTo + " bytes). You have requested portion "
					+ iOffset + "-" + (iOffset + iLength) + " bytes. File: " + toString());

		return iOffset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getFreeSpace()
	 */
	public int getFreeSpace() {
		return size - filledUpTo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getFileSize()
	 */
	public int getFileSize() {
		return size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getFilledUpTo()
	 */
	public int getFilledUpTo() {
		return filledUpTo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#canOversize(int)
	 */
	public boolean canOversize(final int iRecordSize) {
		return maxSize - size > iRecordSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("File: ");
		builder.append(osFile.getName());
		if (accessFile != null) {
			builder.append(" os-size=");
			try {
				builder.append(accessFile.length());
			} catch (IOException e) {
				builder.append("?");
			}
		}
		builder.append(", stored=");
		builder.append(size);
		builder.append(", filled=");
		builder.append(filledUpTo);
		builder.append(", max=");
		builder.append(maxSize);
		builder.append("");
		return builder.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getOsFile()
	 */
	public File getOsFile() {
		return osFile;
	}

	public OAbstractFile init(final String iFileName, final String iMode) {
		mode = iMode;
		osFile = new File(iFileName);
		return this;
	}

	protected void openChannel(final int iNewSize) throws IOException {
		OLogManager.instance().debug(this, "[OFile.openChannel] Opening channel for file: " + osFile);

		for (int i = 0; i < OPEN_RETRY_MAX; ++i)
			try {
				accessFile = new RandomAccessFile(osFile, mode);
				break;
			} catch (FileNotFoundException e) {
				if (i == OPEN_DELAY_RETRY)
					throw e;

				// TRY TO RE-CREATE THE DIRECTORY (THIS HAPPENS ON WINDOWS AFTER A DELETE IS PENDING, USUALLY WHEN REOPEN THE DB VERY
				// FREQUENTLY)
				osFile.getParentFile().mkdirs();
				try {
					Thread.sleep(OPEN_DELAY_RETRY);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
				}
			}

		accessFile.setLength(iNewSize);
		accessFile.seek(0);
		channel = accessFile.getChannel();

		if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
			lock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getMaxSize()
	 */
	public int getMaxSize() {
		return maxSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#setMaxSize(int)
	 */
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#getIncrementSize()
	 */
	public int getIncrementSize() {
		return incrementSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#setIncrementSize(int)
	 */
	public void setIncrementSize(int incrementSize) {
		this.incrementSize = incrementSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isOpen()
	 */
	public boolean isOpen() {
		return accessFile != null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#exists()
	 */
	public boolean exists() {
		return osFile != null && osFile.exists();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isFailCheck()
	 */
	public boolean isFailCheck() {
		return failCheck;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#setFailCheck(boolean)
	 */
	public void setFailCheck(boolean failCheck) {
		this.failCheck = failCheck;
	}

	protected void setDirty() {
		if (!dirty)
			dirty = true;
	}

	protected void setHeaderDirty() {
		if (!headerDirty)
			headerDirty = true;
	}

	public String getName() {
		return osFile.getName();
	}

	public String getPath() {
		return osFile.getPath();
	}

	public String getAbsolutePath() {
		return osFile.getAbsolutePath();
	}

	public boolean renameTo(final File newFile) {
		return osFile.renameTo(newFile);
	}
}
