package com.orientechnologies.orient.core.storage.impl.local;

import java.io.File;
import java.io.IOException;

import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
public class OClusterLocalLHPEOverflow extends OSingleFileSegment {
  private final OClusterLocalLHPEPS clusterLocal;

  public OClusterLocalLHPEOverflow(OStorageLocal iStorage, OStorageFileConfiguration iConfig, OClusterLocalLHPEPS clusterLocal)
      throws IOException {
    super(iStorage, iConfig);
    this.clusterLocal = clusterLocal;
  }

  public OClusterLocalLHPEBucket createBucket() throws IOException {
    final long position = file.allocateSpace(OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES);

    return new OClusterLocalLHPEBucket(clusterLocal, position, true);
  }

  public void rename(String iOldName, String iNewName) {
    final String osFileName = file.getName();
    if (osFileName.startsWith(iOldName)) {
      final File newFile = new File(storage.getStoragePath() + '/' + iNewName
          + osFileName.substring(osFileName.lastIndexOf(iOldName) + iOldName.length()));
      boolean renamed = file.renameTo(newFile);
      while (!renamed) {
        OMemoryWatchDog.freeMemory(100);
        renamed = file.renameTo(newFile);
      }
    }
  }

  public void updateBucket(OClusterLocalLHPEBucket bucket) throws IOException {
    file.write(bucket.getFilePosition(), bucket.getBuffer());
  }

  public OClusterLocalLHPEBucket loadBucket(long position) throws IOException {
    final byte[] buffer = new byte[OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES];
    file.read(position, buffer, OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES);

    return new OClusterLocalLHPEBucket(buffer, clusterLocal, position, true);
  }

  public long getBucketsSize() {
    return getFilledUpTo() / OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES;
  }

  public void updateDataSegmentPosition(OClusterLocalLHPEBucket bucket, int index, int iDataSegmentId, long iDataPosition)
      throws IOException {
    final int idOffset = OClusterLocalLHPEBucket.getDataSegmentIdOffset(index);

    final long filePos = bucket.getFilePosition() + idOffset;

    final byte[] serializedDataSegment = OClusterLocalLHPEBucket.serializeDataSegmentId(iDataSegmentId);
    final byte[] serializedDataPosition = OClusterLocalLHPEBucket.serializeDataPosition(iDataPosition);

    file.write(filePos, serializedDataSegment);
    file.write(filePos + serializedDataSegment.length, serializedDataPosition);
  }

  public void updateRecordType(OClusterLocalLHPEBucket bucket, int index, byte iRecordType) throws IOException {
    final int recordTypeOffset = OClusterLocalLHPEBucket.getRecordTypeOffset(index);

    final long filePos = bucket.getFilePosition() + recordTypeOffset;

    file.writeByte(filePos, iRecordType);
  }

  public void updateVersion(OClusterLocalLHPEBucket bucket, int index, int iVersion) throws IOException {
    final int versionOffset = OClusterLocalLHPEBucket.getVersionOffset(index);
    final byte[] serializedVersion = OClusterLocalLHPEBucket.serializeVersion(iVersion);

    final long filePos = bucket.getFilePosition() + versionOffset;

    file.write(filePos, serializedVersion);
  }
}
