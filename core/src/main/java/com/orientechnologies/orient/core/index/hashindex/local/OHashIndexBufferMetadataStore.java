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
      final OStorageSegmentConfiguration fileConfiguration = metadata.getFileConfiguration();

      bufferSize += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.name);
      bufferSize += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.fileType);
      bufferSize += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.fileMaxSize);
      bufferSize += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.fileIncrementSize);

      bufferSize += OIntegerSerializer.INT_SIZE;

      for (OStorageFileConfiguration storageFileConfiguration : fileConfiguration.infoFiles) {
        bufferSize += OStringSerializer.INSTANCE.getObjectSize(storageFileConfiguration.incrementSize);
        bufferSize += OStringSerializer.INSTANCE.getObjectSize(storageFileConfiguration.path == null ? ""
            : storageFileConfiguration.path);
        bufferSize += OStringSerializer.INSTANCE.getObjectSize(storageFileConfiguration.type);
        bufferSize += OStringSerializer.INSTANCE.getObjectSize(storageFileConfiguration.maxSize == null ? ""
            : storageFileConfiguration.maxSize);
      }

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
      offset += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.name);

      OStringSerializer.INSTANCE.serializeNative(fileConfiguration.fileType, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.fileType);

      OStringSerializer.INSTANCE.serializeNative(fileConfiguration.fileMaxSize, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.fileMaxSize);

      OStringSerializer.INSTANCE.serializeNative(fileConfiguration.fileIncrementSize, buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileConfiguration.fileIncrementSize);

      OIntegerSerializer.INSTANCE.serializeNative(fileConfiguration.id, buffer, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(fileConfiguration.infoFiles.length, buffer, offset);
      offset += OIntegerSerializer.INT_SIZE;

      for (OStorageFileConfiguration storageFileConfiguration : fileConfiguration.infoFiles) {
        OStringSerializer.INSTANCE.serializeNative(storageFileConfiguration.incrementSize, buffer, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(storageFileConfiguration.incrementSize);

        OStringSerializer.INSTANCE.serializeNative(storageFileConfiguration.path == null ? "" : storageFileConfiguration.path,
            buffer, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(storageFileConfiguration.path == null ? ""
            : storageFileConfiguration.path);

        OStringSerializer.INSTANCE.serializeNative(
            storageFileConfiguration.maxSize == null ? "" : storageFileConfiguration.maxSize, buffer, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(storageFileConfiguration.maxSize == null ? ""
            : storageFileConfiguration.maxSize);
      }

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

      final String fileType = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileType);

      final String fileMaxSize = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileMaxSize);

      final String fileIncrementSize = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileIncrementSize);

      final int id = OIntegerSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final int infoFilesLength = OIntegerSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final OStorageFileConfiguration[] infoFiles = new OStorageFileConfiguration[infoFilesLength];
      for (int n = 0; n < infoFiles.length; n++) {
        final String incrementSize = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(incrementSize);

        String path = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(path);

        if (path.isEmpty())
          path = null;

        String maxSize = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(maxSize);
        if (maxSize.isEmpty())
          maxSize = null;

        final OStorageFileConfiguration storageFileConfiguration = new OStorageFileConfiguration();
        storageFileConfiguration.incrementSize = incrementSize;
        storageFileConfiguration.path = path;
        storageFileConfiguration.type = fileType;
        storageFileConfiguration.maxSize = maxSize;

        infoFiles[n] = storageFileConfiguration;
      }

      final long bucketsCount = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      final long tombstone = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      final OStorageSegmentConfiguration fileConfiguration = new OStorageSegmentConfiguration(storage.getConfiguration(), name, id);
      fileConfiguration.fileType = fileType;
      fileConfiguration.infoFiles = infoFiles;
      fileConfiguration.fileMaxSize = fileMaxSize;
      fileConfiguration.fileIncrementSize = fileIncrementSize;

      final OHashIndexFileLevelMetadata metadata = new OHashIndexFileLevelMetadata(fileConfiguration, bucketsCount, tombstone);

      metadatas[i] = metadata;
    }

    return metadatas;
  }

}
