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
public abstract class OFile {
	protected static final int	SOFTLY_CLOSED_OFFSET		= 8;

	private FileLock						fileLock;

	protected File							osFile;
	protected RandomAccessFile	accessFile;
	protected FileChannel				channel;

	protected int								incrementSize						= DEFAULT_INCREMENT_SIZE;
	protected int								maxSize;
	protected int								size;																						// PART OF HEADER (4 bytes)
	protected int								filledUpTo;																			// PART OF HEADER (4 bytes)
	protected byte[]						securityCode						= new byte[32];					// PART OF HEADER (32 bytes)
	protected String						mode;
	protected boolean						failCheck								= true;

	protected static final int	HEADER_SIZE							= 1024;
	protected static final int	HEADER_DATA_OFFSET			= 128;
	protected static final int	DEFAULT_SIZE						= 15000000;
	protected static final int	DEFAULT_INCREMENT_SIZE	= -50;										// NEGATIVE NUMBER MEANS AS PERCENT OF CURRENT SIZE

	private static final int		OPEN_RETRY_MAX					= 10;
	private static final int		OPEN_DELAY_RETRY				= 100;

	private static final long		LOCK_WAIT_TIME					= 300;
	private static final int		LOCK_MAX_RETRIES				= 10;

	public OFile(final String iFileName, final String iMode) throws IOException {
		init(iFileName, iMode);
	}

	protected abstract void writeHeader() throws IOException;

	protected abstract void readHeader() throws IOException;

	public abstract void writeHeaderLong(int iPosition, long iValue) throws IOException;

	public abstract long readHeaderLong(int iPosition) throws IOException;

	protected abstract void setSoftlyClosed(boolean b) throws IOException;

	protected abstract boolean isSoftlyClosed() throws IOException;

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

	public boolean open() throws IOException {
		if (!osFile.exists() || osFile.length() == 0)
			throw new FileNotFoundException("File: " + osFile.getAbsolutePath());

		openChannel((int) osFile.length());

		OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getName() + "...");

		final int fileSize = size;
		readHeader();

		if (filledUpTo > 0 && filledUpTo > size) {
			OLogManager
					.instance()
					.warn(
							this,
							"Invalid OFile.filledUp value (%d). Reset the file size %d to the os file size: %d. Probably the file was not closed correctly last time",
							filledUpTo, size, fileSize);
			size = fileSize;
			writeHeader();
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

	public void create(int iStartSize) throws IOException {
		if (iStartSize == -1)
			iStartSize = DEFAULT_SIZE;

		openChannel(iStartSize);

		filledUpTo = 0;
		writeHeader();
		setSoftlyClosed(!failCheck);
	}

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

	public void unlock() throws IOException {
		if (fileLock != null) {
			try {
				fileLock.release();
			} catch (ClosedChannelException e) {
			}
			fileLock = null;
		}
	}

	public void changeSize(final int iSize) {

		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this, "Changing file size to " + iSize + " bytes. " + toString());

		if (iSize < filledUpTo)
			OLogManager.instance().error(
					this,
					"You cannot resize down the file to " + iSize + " bytes, since it is less than current space used: " + filledUpTo
							+ " bytes", OIOException.class);
	}

	/**
	 * Cut bytes from the tail of the file reducing the filledUpTo size.
	 * 
	 * @param iSize
	 * @throws IOException
	 */
	public void removeTail(int iSize) throws IOException {
		if (filledUpTo < iSize)
			iSize = 0;

		filledUpTo -= iSize;
		writeHeader();
	}

	/**
	 * Shrink the file content (filledUpTo attribute only)
	 * 
	 * @param iSize
	 * @throws IOException
	 */
	public void shrink(final int iSize) throws IOException {
		if (iSize >= filledUpTo)
			return;

		OLogManager.instance().debug(this, "Shrinking filled file from " + filledUpTo + " to " + iSize + " bytes. " + toString());

		filledUpTo = iSize;
		writeHeader();
	}

	public int allocateSpace(final int iSize) throws IOException {
		final int offset = filledUpTo;

		if (getFreeSpace() < iSize) {
			if (maxSize > 0 && maxSize - size < iSize)
				throw new IllegalArgumentException("Cannot enlarge file since the configured max size ("
						+ OFileUtils.getSizeAsString(maxSize) + ") was reached! " + toString());

			// MAKE ROOM
			int newFileSize = size;
			int stepSizeInBytes = incrementSize > 0 ? incrementSize : -1 * size / 100 * incrementSize;

			// FIND THE BEST SIZE TO ALLOCATE (BASED ON INCREMENT-SIZE)
			while (newFileSize - filledUpTo <= iSize) {
				if (stepSizeInBytes > 0 && (maxSize == 0 || newFileSize + stepSizeInBytes < maxSize))
					newFileSize += stepSizeInBytes;
				else
					newFileSize = maxSize;
			}

			changeSize(newFileSize);
		}

		// THERE IS SPACE IN FILE: RETURN THE UPPER BOUND OFFSET AND UPDATE THE FILLED THRESHOLD
		filledUpTo += iSize;
		writeHeader();

		return offset;
	}

	protected long checkRegions(final long iOffset, final int iLength) {
		if (iOffset + iLength > filledUpTo)
			throw new OIOException("You cannot access outside the file size (" + filledUpTo + " bytes). You have requested portion "
					+ iOffset + "-" + (iOffset + iLength) + " bytes. File: " + toString());

		return iOffset;
	}

	public int getFreeSpace() {
		return size - filledUpTo;
	}

	public int getFileSize() {
		return size;
	}

	public int getFilledUpTo() {
		return filledUpTo;
	}

	public boolean canOversize(final int iRecordSize) {
		return maxSize - size > iRecordSize;
	}

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

	public File getOsFile() {
		return osFile;
	}

	protected void init(final String iFileName, final String iMode) throws IOException {
		mode = iMode;
		osFile = new File(iFileName);
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
				}
			}

		accessFile.setLength(iNewSize);
		accessFile.seek(0);
		channel = accessFile.getChannel();

		if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
			lock();
		size = maxSize > 0 && iNewSize > maxSize ? maxSize : iNewSize;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public int getIncrementSize() {
		return incrementSize;
	}

	public void setIncrementSize(int incrementSize) {
		this.incrementSize = incrementSize;
	}

	public boolean isOpen() {
		return accessFile != null;
	}

	public boolean exists() {
		return osFile != null && osFile.exists();
	}

	public boolean isFailCheck() {
		return failCheck;
	}

	public void setFailCheck(boolean failCheck) {
		this.failCheck = failCheck;
	}
}
