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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Handles the database configuration in one big record.
 */
@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED")
public class OStorageConfigurationSegment extends OStorageConfiguration {
  private static final long                  serialVersionUID = 638874446554389034L;

  private static final int                   START_SIZE       = 10000;
  private final transient OSingleFileSegment segment;

  public OStorageConfigurationSegment(final OLocalPaginatedStorage iStorage) throws IOException {
    super(iStorage);
    segment = new OSingleFileSegment((OLocalPaginatedStorage) storage, new OStorageFileConfiguration(null,
        getDirectory() + "/database.ocf", "classic", fileTemplate.maxSize, fileTemplate.fileIncrementSize));
  }

  public void close() throws IOException {
    super.close();
    segment.close();
  }

  public void delete() throws IOException {
    super.delete();
    segment.delete();
  }

  public void create() throws IOException {
    segment.create(START_SIZE);
    super.create();

    final OFile f = segment.getFile();
    if ( OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean())
      f.synch();
  }

  @Override
  public OStorageConfiguration load(final OContextConfiguration configuration) throws OSerializationException {
    try {
      initConfiguration(configuration);

      if (segment.getFile().exists())
        segment.open();
      else {
        segment.create(START_SIZE);

        // @COMPATIBILITY0.9.25
        // CHECK FOR OLD VERSION OF DATABASE
        final ORawBuffer rawRecord = storage.readRecord(CONFIG_RID, null, false, false, null).getResult();
        if (rawRecord != null)
          fromStream(rawRecord.buffer);

        update();
        return this;
      }

      final int size = segment.getFile().readInt(0);
      byte[] buffer = new byte[size];
      segment.getFile().read(OBinaryProtocol.SIZE_INT, buffer, size);

      fromStream(buffer);
    } catch (IOException e) {
      throw OException
          .wrapException(new OSerializationException("Cannot load database configuration. The database seems corrupted"), e);
    }
    return this;
  }

  @Override
  public void lock() throws IOException {
  }

  @Override
  public void unlock() throws IOException {
  }

  @Override
  public void update() throws OSerializationException {
    try {
      final OFile f = segment.getFile();

      if (!f.isOpen())
        return;

      final byte[] buffer = toStream();

      final int len = buffer.length + OBinaryProtocol.SIZE_INT;

      if (len > f.getFileSize())
        f.allocateSpace(len - f.getFileSize());

      f.writeInt(0, buffer.length);
      f.write(OBinaryProtocol.SIZE_INT, buffer);
      if (OGlobalConfiguration.STORAGE_CONFIGURATION_SYNC_ON_UPDATE.getValueAsBoolean())
        f.synch();

    } catch (Exception e) {
      throw OException.wrapException(new OSerializationException("Error on update storage configuration"), e);
    }
  }

  public void synch() throws IOException {
    segment.getFile().synch();
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  public String getFileName() {
    return segment.getFile().getName();
  }
}
