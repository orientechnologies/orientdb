package com.orientechnologies.orient.core.index.hashindex.local;

import java.io.IOException;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 04.03.13
 */
public class OHashIndexBufferMetadataStore extends OSingleFileSegment {
  public OHashIndexBufferMetadataStore(String iPath, String iType) throws IOException {
    super(iPath, iType);
  }

  public OHashIndexBufferMetadataStore(OStorageLocal iStorage, OStorageFileConfiguration iConfig) throws IOException {
    super(iStorage, iConfig);
  }

  public OHashIndexBufferMetadataStore(OStorageLocal iStorage, OStorageFileConfiguration iConfig, String iType) throws IOException {
    super(iStorage, iConfig, iType);
  }

  public void setRecordsCount(final long recordsCount) throws IOException {
    file.writeHeaderLong(0, recordsCount);
  }

  public long getRecordsCount() throws IOException {
    return file.readHeaderLong(0);
  }

  public void setTombstonesCount(long tombstonesCount) throws IOException {
    file.writeHeaderLong(OLongSerializer.LONG_SIZE, tombstonesCount);
  }

  public long getTombstonesCount() throws IOException {
    return file.readHeaderLong(OLongSerializer.LONG_SIZE);
  }

  public void storeMetadata(OHashIndexFileLevelMetadata[] filesMetadata) throws IOException {
    int bufferSize = 0;

    for (OHashIndexFileLevelMetadata metadata : filesMetadata) {
      if (metadata == null)
        break;

      final OStorageSegmentConfiguration fileConfiguration = metadata.getFileConfiguration();

      bufferSize += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.name);
      bufferSize += OIntegerSerializer.INT_SIZE;

      bufferSize += 2 * OLongSerializer.LONG_SIZE;
    }

    final int totalSize = bufferSize + 3 * OIntegerSerializer.INT_SIZE;
    if (file.getFilledUpTo() < totalSize)
      file.allocateSpace(totalSize - file.getFilledUpTo());

    byte[] buffer = new byte[bufferSize];
    int offset = 0;

    for (OHashIndexFileLevelMetadata fileMetadata : filesMetadata) {
      if (fileMetadata == null)
        break;

      final OStorageSegmentConfiguration fileConfiguration = fileMetadata.getFileConfiguration();

      OStringSerializer.INSTANCE.serializeNative(fileConfiguration.name, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSizeNative(buffer, offset);

      OIntegerSerializer.INSTANCE.serializeNative(fileConfiguration.id, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSizeNative(buffer, offset);

      OLongSerializer.INSTANCE.serializeNative(fileMetadata.getBucketsCount(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeNative(fileMetadata.getTombstoneIndex(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    file.writeInt(0, filesMetadata.length);
    file.writeInt(OIntegerSerializer.INT_SIZE, buffer.length);
    file.write(2 * OIntegerSerializer.INT_SIZE, buffer);
  }

  public OHashIndexFileLevelMetadata[] loadMetadata() throws IOException {
    final int len = file.readInt(0);
    final OHashIndexFileLevelMetadata[] metadatas = new OHashIndexFileLevelMetadata[len];

    final int bufferSize = file.readInt(OIntegerSerializer.INT_SIZE);
    final byte[] buffer = new byte[bufferSize];
    file.read(2 * OIntegerSerializer.INT_SIZE, buffer, buffer.length);

    int offset = 0;
    int i = 0;
    while (offset < bufferSize) {
      final String name = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(name);

      final int id = OIntegerSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final long bucketsCount = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      final long tombstone = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      final OStorageSegmentConfiguration fileConfiguration = new OStorageSegmentConfiguration(storage.getConfiguration(), name, id);

      final OHashIndexFileLevelMetadata metadata = new OHashIndexFileLevelMetadata(fileConfiguration, bucketsCount, tombstone);
      metadatas[i] = metadata;
      i++;
    }

    return metadatas;
  }

}
