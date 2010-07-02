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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;

public class OMultiFileSegment extends OSegment {
	protected OStorageSegmentConfiguration	config;
	protected OFile[]												files	= new OFile[0];
	private String													fileExtension;
	private String													type;
	private long														maxSize;
	@SuppressWarnings("unused")
	private String													defrag;
	private int															fileStartSize;
	private int															fileMaxSize;
	private int															fileIncrementSize;

	public OMultiFileSegment(final OStorageLocal iStorage, final OStorageSegmentConfiguration iConfig, final String iFileExtension,
			final int iRoundMaxSize) throws IOException {
		super(iStorage, iConfig.name);

		config = iConfig;
		fileExtension = iFileExtension;
		type = iConfig.fileType;
		defrag = iConfig.defrag;
		maxSize = OFileUtils.getSizeAsNumber(iConfig.maxSize);
		fileStartSize = (int) OFileUtils.getSizeAsNumber(iConfig.fileStartSize);
		fileMaxSize = (int) OFileUtils.getSizeAsNumber(iConfig.fileMaxSize);
		fileIncrementSize = (int) OFileUtils.getSizeAsNumber(iConfig.fileIncrementSize);

		if (iRoundMaxSize > 0)
			// ROUND THE FILE SIZE TO AVOID ERRORS ON ROUNDING BY DIVIDING FOR FIXED RECORD SIZE
			fileMaxSize = (fileMaxSize / iRoundMaxSize) * iRoundMaxSize;

		// INSTANTIATE ALL THE FILES
		int perFileMaxSize;

		if (iConfig.infoFiles.length == 0) {
			// EMPTY FILE: CREATE THE FIRST FILE BY DEFAULT
			files = new OFile[1];
			files[0] = OFileFactory.create(type,
					iStorage.getVariableParser().resolveVariables(storage.getStoragePath() + "/" + name + "." + 0 + fileExtension),
					iStorage.getMode());
			perFileMaxSize = fileMaxSize;
			files[0].setMaxSize(perFileMaxSize);
			files[0].setIncrementSize(fileIncrementSize);

		} else {
			files = new OFile[iConfig.infoFiles.length];
			for (int i = 0; i < files.length; ++i) {
				files[i] = OFileFactory.create(type, iStorage.getVariableParser().resolveVariables(iConfig.infoFiles[i].path),
						iStorage.getMode());
				perFileMaxSize = fileMaxSize;

				files[i].setMaxSize(perFileMaxSize);
				files[i].setIncrementSize(fileIncrementSize);
			}
		}
	}

	public void open() throws IOException {
		for (OFile file : files)
			if (!file.open()) {
				// LAST TIME THE FILE WAS NOT CLOSED IN SOFT WAY
				OLogManager.instance().warn(
						this,
						"Segment file " + OFileUtils.getPath(file.getOsFile().getName())
								+ " was not closed correctly last time. Checking segments...");
				OLogManager.instance().warn(this, "OK");
			}
	}

	/**
	 * Create the first file for current segment
	 * 
	 * @param iStartSize
	 * @throws IOException
	 */
	public void create(final int iStartSize) throws IOException {
		files = new OFile[1];
		createNewFile();
	}

	public void close() throws IOException {
		try {
			acquireExclusiveLock();

			for (OFile file : files) {
				if (file != null)
					file.close();
			}

		} finally {
			releaseExclusiveLock();
		}
	}

	public void delete() throws IOException {
		try {
			acquireExclusiveLock();

			for (OFile file : files) {
				if (file != null)
					file.delete();
			}

		} finally {
			releaseExclusiveLock();
		}
	}

	public void synch() {
		try {
			acquireSharedLock();

			for (OFile file : files) {
				if (file != null)
					file.synch();
			}

		} finally {
			releaseSharedLock();
		}
	}

	public long getFilledUpTo() {
		try {
			acquireSharedLock();

			int filled = 0;
			for (OFile file : files)
				filled += file.getFilledUpTo();

			return filled;

		} finally {
			releaseSharedLock();
		}
	}

	public long getSize() {
		try {
			acquireSharedLock();

			int size = 0;
			for (OFile file : files)
				size += file.getFileSize();

			return size;

		} finally {
			releaseSharedLock();
		}
	}

	/**
	 * Find free space for iRecordSize bytes.
	 * 
	 * @param iRecordSize
	 * @return a pair file-id/file-pos
	 * @throws IOException
	 */
	protected int[] allocateSpace(final int iRecordSize) throws IOException {
		// TODO: RECYCLE THE HOLES IF ANY

		// IT'S PREFEREABLE TO FIND SPACE WITHOUT ENLARGE ANY FILES: FIND THE FIRST FILE WITH FREE SPACE TO USE
		OFile file;
		for (int i = 0; i < files.length; ++i) {
			file = files[i];

			if (file.getFreeSpace() >= iRecordSize)
				// FOUND: RETURN THIS OFFSET
				return new int[] { i, file.allocateSpace(iRecordSize) };
		}

		// NOT FOUND: CHECK IF CAN OVERSIZE SOME FILES
		for (int i = 0; i < files.length; ++i) {
			file = files[i];

			if (file.canOversize(iRecordSize)) {
				// FOUND SPACE: ENLARGE IT
				return new int[] { i, file.allocateSpace(iRecordSize) };
			}
		}

		// TRY TO CREATE A NEW FILE
		if (maxSize > 0 && getSize() >= maxSize)
			// OUT OF MAX SIZE
			throw new OStorageException("Unable to allocate the requested space of " + iRecordSize
					+ " bytes because the segment is full: max-Size=" + maxSize + ", currentSize=" + getFilledUpTo());

		// COPY THE OLD ARRAY TO THE NEW ONE
		OFile[] newFiles = new OFile[files.length + 1];
		for (int i = 0; i < files.length; ++i)
			newFiles[i] = files[i];
		files = newFiles;

		// CREATE THE NEW FILE AND PUT IT AS LAST OF THE ARRAY
		file = createNewFile();
		file.allocateSpace(iRecordSize);

		config.root.update();

		return new int[] { files.length - 1, 0 };
	}

	/**
	 * Return the absolute position receiving the pair file-id/file-pos.
	 * 
	 * @param iFilePosition
	 *          as pair file-id/file-pos
	 * @return
	 */
	protected long getAbsolutePosition(final int[] iFilePosition) {
		long position = 0;
		for (int i = 0; i < iFilePosition[0]; ++i) {
			position += fileMaxSize;
		}
		return position + iFilePosition[1];
	}

	protected int[] getRelativePosition(final long iPosition) {
		if (iPosition < fileMaxSize)
			return new int[] { 0, (int) iPosition };

		final int fileNum = (int) (iPosition / fileMaxSize);

		if (fileNum >= files.length)
			throw new ODatabaseException("Record position #" + iPosition + " was bound to file #" + fileNum
					+ " that is out of limit (current=" + (files.length - 1) + ")");

		final int fileRec = (int) (iPosition % fileMaxSize);

		if (fileRec >= files[fileNum].getFilledUpTo())
			throw new ODatabaseException("Record position #" + iPosition + " was bound to file #" + fileNum + " but the position #"
					+ files[fileNum].getFilledUpTo() + " is out of file size");

		return new int[] { fileNum, fileRec };
	}

	private OFile createNewFile() throws IOException {
		final int num = files.length - 1;

		final OFile file = OFileFactory.create(type, storage.getStoragePath() + "/" + name + "." + num + fileExtension,
				storage.getMode());
		file.setMaxSize((int) OFileUtils.getSizeAsNumber(config.fileMaxSize));
		file.create(fileStartSize);
		files[num] = file;

		addInfoFileConfigEntry(file);

		return file;
	}

	private void addInfoFileConfigEntry(final OFile file) throws IOException {
		OStorageFileConfiguration[] newConfigFiles = new OStorageFileConfiguration[config.infoFiles.length + 1];
		for (int i = 0; i < config.infoFiles.length; ++i)
			newConfigFiles[i] = config.infoFiles[i];
		config.infoFiles = newConfigFiles;

		// CREATE A NEW ENTRY FOR THE NEW FILE
		String fileNameToStore = storage.getVariableParser().convertPathToRelative(OFileUtils.getPath(file.getOsFile().getPath()));

		config.infoFiles[config.infoFiles.length - 1] = new OStorageFileConfiguration(config, fileNameToStore, config.fileType,
				config.fileMaxSize, config.fileIncrementSize);
	}

	public OStorageSegmentConfiguration getConfig() {
		return config;
	}
}
