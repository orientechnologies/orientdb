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

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/**
 * Handles the database configuration in one big record.
 */
@SuppressWarnings("serial")
public class OStorageConfigurationSegment extends OStorageConfiguration {
	private static final int		START_SIZE	= 10000;
	private OSingleFileSegment	segment;

	public OStorageConfigurationSegment(final OStorageLocal iStorage, final String iPath) throws IOException {
		super(iStorage);
		segment = new OSingleFileSegment((OStorageLocal) storage, new OStorageFileConfiguration(null, iPath + "/database.ocf",
				fileTemplate.fileType, fileTemplate.maxSize, fileTemplate.fileIncrementSize));
	}

	@Override
	public void close() throws IOException {
		segment.close();
	}

	public void create() throws IOException {
		segment.create(START_SIZE);
		super.create();
	}

	@Override
	public OStorageConfiguration load() throws OSerializationException {
		try {
			if (segment.getFile().exists())
				segment.open();
			else {
				segment.create(START_SIZE);

				// @COMPATIBILITY0.9.25
				// CHECK FOR OLD VERSION OF DATABASE
				final ORawBuffer rawRecord = storage.readRecord(null, CONFIG_RID, null, null);
				if (rawRecord != null)
					fromStream(rawRecord.buffer);

				update();
				return this;
			}

			final int size = segment.getFile().readInt(0);
			byte[] buffer = new byte[size];
			segment.getFile().read(OBinaryProtocol.SIZE_INT, buffer, size);

			fromStream(buffer);
		} catch (Exception e) {
			throw new OSerializationException("Can't load database's configuration. The database seems to be corrupted.");
		}
		return this;
	}

	@Override
	public void update() throws OSerializationException {
		try {
			if (!segment.getFile().isOpen())
				return;

			final byte[] buffer = toStream();

			final int requiredSpace = buffer.length + OBinaryProtocol.SIZE_INT;
			if (segment.getFile().getFileSize() < requiredSpace)
				segment.getFile().changeSize(requiredSpace);

			segment.getFile().allocateSpace(buffer.length + OBinaryProtocol.SIZE_INT);
			segment.getFile().writeInt(0, buffer.length);
			segment.getFile().write(OBinaryProtocol.SIZE_INT, buffer);
		} catch (Exception e) {
			throw new OSerializationException("Error on update storage configuration", e);
		}
	}
}
