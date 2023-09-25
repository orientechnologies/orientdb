package com.orientechnologies.orient.core.storage.cache.local;

final class NameFileIdEntry {
  private final String name;
  private final int fileId;
  private final String fileSystemName;

  public NameFileIdEntry(final String name, final int fileId) {
    this.name = name;
    this.fileId = fileId;
    this.fileSystemName = name;
  }

  public NameFileIdEntry(final String name, final int fileId, final String fileSystemName) {
    this.name = name;
    this.fileId = fileId;
    this.fileSystemName = fileSystemName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final NameFileIdEntry that = (NameFileIdEntry) o;

    if (getFileId() != that.getFileId()) {
      return false;
    }
    if (!getName().equals(that.getName())) {
      return false;
    }
    return getFileSystemName().equals(that.getFileSystemName());
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = 31 * result + getFileId();
    result = 31 * result + getFileSystemName().hashCode();
    return result;
  }

  String getName() {
    return name;
  }

  int getFileId() {
    return fileId;
  }

  String getFileSystemName() {
    return fileSystemName;
  }
}
