/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.impl.local.eh;

import java.io.IOException;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 06.02.13
 */
public class OEHMetadataStore extends OSingleFileSegment {
  public OEHMetadataStore(String iPath, String iType) throws IOException {
    super(iPath, iType);
  }

  public OEHMetadataStore(OStorageLocal iStorage, OStorageFileConfiguration iConfig) throws IOException {
    super(iStorage, iConfig);
  }

  public OEHMetadataStore(OStorageLocal iStorage, OStorageFileConfiguration iConfig, String iType) throws IOException {
    super(iStorage, iConfig, iType);
  }

  public void storeMetadata(OEHFileMetadata[] filesMetadata) throws IOException {
    int size = 0;

    for (OEHFileMetadata bucketFile : filesMetadata) {
      final OStorageFileConfiguration fileConfiguration = bucketFile.getFile().getConfig();

      size += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.incrementSize);
      size += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.path);
      size += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.type);

      size += 2 * OLongSerializer.LONG_SIZE;
    }

    if (file.getFilledUpTo() < size + 2 * OIntegerSerializer.INT_SIZE)
      file.allocateSpace(size - file.getFilledUpTo());

    byte[] buffer = new byte[size];
    int offset = 0;

    for (OEHFileMetadata bucketFile : filesMetadata) {
      final OStorageFileConfiguration fileConfiguration = bucketFile.getFile().getConfig();

      OStringSerializer.INSTANCE.serializeNative(fileConfiguration.incrementSize, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSizeNative(buffer, offset);

      OStringSerializer.INSTANCE.serializeNative(fileConfiguration.path, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSizeNative(buffer, offset);

      OStringSerializer.INSTANCE.serializeNative(fileConfiguration.type, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSizeNative(buffer, offset);

      OLongSerializer.INSTANCE.serializeNative(bucketFile.geBucketsCount(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeNative(bucketFile.getTombstonePosition(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    file.writeInt(0, filesMetadata.length);
    file.writeInt(OIntegerSerializer.INT_SIZE, buffer.length);
    file.write(2 * OIntegerSerializer.INT_SIZE, buffer);
  }

  public OEHFileMetadata[] loadMetadata() throws IOException {
    final int len = file.readInt(0);
    final OEHFileMetadata[] metadata = new OEHFileMetadata[len];

    final int bufferSize = file.readInt(OIntegerSerializer.INT_SIZE);
    final byte[] buffer = new byte[bufferSize];

    int offset = 0;
    for (int i = 0; i < len; i++) {
      final String incrementSize = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(incrementSize);

      final String path = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(path);

      final String type = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(type);

      final long bucketsCount = OIntegerSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final long tombstone = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);

      final OStorageFileConfiguration fileConfiguration = new OStorageFileConfiguration(null, path, type, "0", incrementSize);

      final OSingleFileSegment singleFileSegment = new OSingleFileSegment(storage, fileConfiguration);
      final OEHFileMetadata bucketFile = new OEHFileMetadata();
      bucketFile.setFile(singleFileSegment);
      bucketFile.setBucketsCount(bucketsCount);
      bucketFile.setTombstonePosition(tombstone);

      metadata[i] = bucketFile;
    }

    return metadata;
  }
}
