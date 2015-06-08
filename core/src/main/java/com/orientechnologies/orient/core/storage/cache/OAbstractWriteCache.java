package com.orientechnologies.orient.core.storage.cache;

/**
 * Created by lomak_000 on 06.06.2015.
 */
public class OAbstractWriteCache {
  public static long composeFileId(int id, int fileId) {
    return (((long) id) << 32) | fileId;
  }

  public static int extractFileId(long fileId) {
    return (int) (fileId & 0xFFFFFFFFL);
  }

  public static int extractStorageId(long fileId) {
    return (int) (fileId >>> 32);
  }

  public static long checkFileIdCompatibility(long fileId, int storageId) {
    if (extractStorageId(fileId) == 0) {
      return composeFileId((int) fileId, storageId);
    }

    return fileId;
  }

}
