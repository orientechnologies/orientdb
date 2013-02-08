package com.orientechnologies.orient.core.storage.impl.local.eh;

import java.io.IOException;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFileMMap;
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

  public void storeMetadata(OEHFileMetadata[] files) throws IOException {
    int size = 0;

    for (OEHFileMetadata extendibleHashingFile : files) {
      final String name;
      if (extendibleHashingFile.getFile() != null)
        name = extendibleHashingFile.getFile().getName();
      else
        name = "";

      size += OStringSerializer.INSTANCE.getObjectSize(name);
      size += 2 * OLongSerializer.LONG_SIZE;
    }

    if (file.getFilledUpTo() < size + 2 * OIntegerSerializer.INT_SIZE)
      file.allocateSpace(size - file.getFilledUpTo());

    byte[] buffer = new byte[size];
    int offset = 0;

    for (OEHFileMetadata extendibleHashingFile : files) {
      OStringSerializer.INSTANCE.serializeNative(extendibleHashingFile.getFile().getName(), buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSizeNative(buffer, offset);

      OLongSerializer.INSTANCE.serializeNative(extendibleHashingFile.geBucketsCount(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeNative(extendibleHashingFile.getTombstonePosition(), buffer, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    file.writeInt(0, files.length);
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
      final String name = OStringSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(name);

      final long bucketsCount = OIntegerSerializer.INSTANCE.deserializeNative(buffer, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final long tombstone = OLongSerializer.INSTANCE.deserializeNative(buffer, offset);

      final OFileMMap fileMMap;
      if (name.length() > 0) {
        fileMMap = new OFileMMap();
        fileMMap.init(name, "rw");
      } else
        fileMMap = null;

      final OEHFileMetadata extendibleHashingFile = new OEHFileMetadata();
      extendibleHashingFile.setFile(fileMMap);
      extendibleHashingFile.setBucketsCount(bucketsCount);
      extendibleHashingFile.setTombstonePosition(tombstone);

      metadata[i] = extendibleHashingFile;
    }

    return metadata;
  }
}
