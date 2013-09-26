package com.orientechnologies.orient.core.index.hashindex.local;

import java.io.IOException;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 04.03.13
 */
public class OHashIndexBufferStore extends OSingleFileSegment {
  public OHashIndexBufferStore(String iPath, String iType) throws IOException {
    super(iPath, iType);
  }

  public OHashIndexBufferStore(OStorageLocalAbstract iStorage, OStorageFileConfiguration iConfig) throws IOException {
    super(iStorage, iConfig);
  }

  public OHashIndexBufferStore(OStorageLocalAbstract iStorage, OStorageFileConfiguration iConfig, String iType) throws IOException {
    super(iStorage, iConfig, iType);
  }

  public void setRecordsCount(final long recordsCount) throws IOException {
    file.writeHeaderLong(0, recordsCount);
  }

  public long getRecordsCount() throws IOException {
    return file.readHeaderLong(0);
  }

  public void setKeySerializerId(byte keySerializerId) throws IOException {
    file.writeHeaderLong(OLongSerializer.LONG_SIZE, keySerializerId);
  }

  public byte getKeySerializerId() throws IOException {
    return (byte) file.readHeaderLong(OLongSerializer.LONG_SIZE);
  }

  public void setValueSerializerId(byte valueSerializerId) throws IOException {
    file.writeHeaderLong(OLongSerializer.LONG_SIZE * 2, valueSerializerId);
  }

  public byte getValuerSerializerId() throws IOException {
    return (byte) file.readHeaderLong(OLongSerializer.LONG_SIZE * 2);
  }

  public void storeMetadata(OHashIndexFileLevelMetadata[] filesMetadata) throws IOException {
    int bufferSize = 0;
    int counter = 0;

    for (OHashIndexFileLevelMetadata metadata : filesMetadata) {
      if (metadata == null)
        break;

      counter++;

      final String fileName = metadata.getFileName();
      bufferSize += OStringSerializer.INSTANCE.getObjectSize(fileName);

      bufferSize += 2 * OLongSerializer.LONG_SIZE;
    }

    final int totalSize = bufferSize + 2 * OIntegerSerializer.INT_SIZE;
    if (file.getFilledUpTo() < totalSize)
      file.allocateSpace(totalSize - file.getFilledUpTo());

    byte[] buffer = new byte[bufferSize];
    int offset = 0;

    for (OHashIndexFileLevelMetadata fileMetadata : filesMetadata) {
      if (fileMetadata == null)
        break;

      OStringSerializer.INSTANCE.serializeNative(fileMetadata.getFileName(), buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileMetadata.getFileName());

      OLongSerializer.INSTANCE.serializeNative(fileMetadata.getBucketsCount(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeNative(fileMetadata.getTombstoneIndex(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    file.writeInt(0, counter);
    file.writeInt(OIntegerSerializer.INT_SIZE, buffer.length);
    file.write(2 * OIntegerSerializer.INT_SIZE, buffer);
  }

  public OHashIndexFileLevelMetadata[] loadMetadata() throws IOException {
    final int len = file.readInt(0);
    final OHashIndexFileLevelMetadata[] metadatas = new OHashIndexFileLevelMetadata[64];

    final int bufferSize = file.readInt(OIntegerSerializer.INT_SIZE);
    final byte[] buffer = new byte[bufferSize];
    file.read(2 * OIntegerSerializer.INT_SIZE, buffer, buffer.length);

    int offset = 0;
    for (int i = 0; i < len; i++) {
      final String name = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(name);

      final long bucketsCount = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      final long tombstone = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      final OHashIndexFileLevelMetadata metadata = new OHashIndexFileLevelMetadata(name, bucketsCount, tombstone);

      metadatas[i] = metadata;
    }

    return metadatas;
  }

}
