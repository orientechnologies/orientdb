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
	private final String										fileExtension;
	private final String										type;
	private final long											maxSize;
	@SuppressWarnings("unused")
	private final String										defrag;
	private int															fileStartSize;
	private int															fileMaxSize;
	private final int												fileIncrementSize;

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
		// @TODO: LAZY OPEN FILES
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
		fileStartSize = iStartSize;
		createNewFile();
	}

	public void close() throws IOException {
		acquireExclusiveLock();
		try {
			for (OFile file : files) {
				if (file != null)
					file.close();
			}

		} finally {
			releaseExclusiveLock();
		}
	}

	public void delete() throws IOException {
		acquireExclusiveLock();
		try {
			for (OFile file : files) {
				if (file != null)
					file.delete();
			}
		} finally {
			releaseExclusiveLock();
		}
	}

	public void truncate() throws IOException {
		acquireExclusiveLock();
		try {
			// SHRINK TO 0
			files[0].shrink(0);

			if (files.length > 1) {
				// LEAVE JUST ONE FILE
				for (int i = 1; i < files.length; ++i) {
					if (files[i] != null)
						files[i].delete();
				}

				// UPDATE FILE STRUCTURE
				final OFile f = files[0];
				files = new OFile[1];
				files[0] = f;

				// UPDATE CONFIGURATION
				final OStorageFileConfiguration fileConfig = config.infoFiles[0];
				config.infoFiles = new OStorageFileConfiguration[1];
				config.infoFiles[0] = fileConfig;
				config.root.update();
			}
		} finally {
			releaseExclusiveLock();
		}
	}

	public void synch() throws IOException {
		acquireSharedLock();
		try {
			for (OFile file : files) {
				if (file != null && file.isOpen())
					file.synch();
			}

		} finally {
			releaseSharedLock();
		}
	}

	public long getFilledUpTo() {
		acquireSharedLock();
		try {
			long filled = 0;
			for (OFile file : files)
				filled += file.getFilledUpTo();

			return filled;

		} finally {
			releaseSharedLock();
		}
	}

	public long getSize() {
		acquireSharedLock();
		try {
			long size = 0;
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
	protected long[] allocateSpace(final int iRecordSize) throws IOException {
		// IT'S PREFEREABLE TO FIND SPACE WITHOUT ENLARGE ANY FILES: FIND THE FIRST FILE WITH FREE SPACE TO USE
		OFile file;
		for (int i = 0; i < files.length; ++i) {
			file = files[i];

			if (file.getFreeSpace() >= iRecordSize)
				// FOUND: RETURN THIS OFFSET
				return new long[] { i, file.allocateSpace(iRecordSize) };
		}

		// NOT FOUND: CHECK IF CAN OVERSIZE SOME FILES
		for (int i = 0; i < files.length; ++i) {
			file = files[i];

			if (file.canOversize(iRecordSize)) {
				// FOUND SPACE: ENLARGE IT
				return new long[] { i, file.allocateSpace(iRecordSize) };
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

		return new long[] { files.length - 1, 0 };
	}

	/**
	 * Return the absolute position receiving the pair file-id/file-pos.
	 * 
	 * @param iFilePosition
	 *          as pair file-id/file-pos
	 * @return
	 */
	protected long getAbsolutePosition(final long[] iFilePosition) {
		long position = 0;
		for (int i = 0; i < iFilePosition[0]; ++i) {
			position += fileMaxSize;
		}
		return position + iFilePosition[1];
	}

	protected long[] getRelativePosition(final long iPosition) {
		if (iPosition < fileMaxSize)
			return new long[] { 0l, iPosition };

		final int fileNum = (int) (iPosition / fileMaxSize);

		if (fileNum >= files.length)
			throw new ODatabaseException("Record position #" + iPosition + " was bound to file #" + fileNum
					+ " that is out of limit (files range 0-" + (files.length - 1) + ")");

		final int fileRec = (int) (iPosition % fileMaxSize);

		if (fileRec >= files[fileNum].getFilledUpTo())
			throw new ODatabaseException("Record position #" + iPosition + " was bound to file #" + fileNum + " but the position #"
					+ files[fileNum].getFilledUpTo() + " is out of file size");

		return new long[] { fileNum, fileRec };
	}

	private OFile createNewFile() throws IOException {
		final int num = files.length - 1;

		final OFile file = OFileFactory.create(type, storage.getStoragePath() + "/" + name + "." + num + fileExtension,
				storage.getMode());
		file.setMaxSize((int) OFileUtils.getSizeAsNumber(config.root.fileTemplate.fileMaxSize));
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

		final OStorageSegmentConfiguration template = config.root.fileTemplate;

		config.infoFiles[config.infoFiles.length - 1] = new OStorageFileConfiguration(config, fileNameToStore, template.fileType,
				template.fileMaxSize, template.fileIncrementSize);
	}

	public OStorageSegmentConfiguration getConfig() {
		return config;
	}
}
