package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;

import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
public class OClusterLocalLHPEOverflow extends OMultiFileSegment {
  private static final String       OVERFLOW_EXTENSION = ".oco";

  private final OClusterLocalLHPEPS clusterLocal;

  public OClusterLocalLHPEOverflow(OStorageLocal iStorage, OStorageSegmentConfiguration iConfig, OClusterLocalLHPEPS clusterLocal,
      int iRoundMaxSize) throws IOException {
    super(iStorage, iConfig, OVERFLOW_EXTENSION, iRoundMaxSize);
    this.clusterLocal = clusterLocal;
  }

  public OClusterLocalLHPEBucket createBucket() throws IOException {
    final long position = this.allocateSpaceContinuously(OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES);

    return new OClusterLocalLHPEBucket(clusterLocal, position, true);
  }

  public void updateBucket(OClusterLocalLHPEBucket bucket) throws IOException {
    this.writeContinuously(bucket.getFilePosition(), bucket.getBuffer());
  }

  public OClusterLocalLHPEBucket loadBucket(long position) throws IOException {
    final byte[] buffer = new byte[OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES];
    this.readContinuously(position, buffer, OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES);

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

    this.writeContinuously(filePos, serializedDataSegment);
    this.writeContinuously(filePos + serializedDataSegment.length, serializedDataPosition);
  }

  public void updateRecordType(OClusterLocalLHPEBucket bucket, int index, byte iRecordType) throws IOException {
    final int recordTypeOffset = OClusterLocalLHPEBucket.getRecordTypeOffset(index);

    final long filePos = bucket.getFilePosition() + recordTypeOffset;

    long[] position = getRelativePosition(filePos);

    int pos = (int) position[0];
    int offset = (int) position[1];

    files[pos].writeByte(offset, iRecordType);
  }

  public void updateVersion(OClusterLocalLHPEBucket bucket, int index, ORecordVersion iVersion) throws IOException {
    final int versionOffset = OClusterLocalLHPEBucket.getVersionOffset(index);
    final byte[] serializedVersion = OClusterLocalLHPEBucket.serializeVersion(iVersion);

    final long filePos = bucket.getFilePosition() + versionOffset;

    this.writeContinuously(filePos, serializedVersion);
  }
}
