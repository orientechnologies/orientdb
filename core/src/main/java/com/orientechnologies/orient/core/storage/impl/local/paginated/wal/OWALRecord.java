package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public interface OWALRecord {
  OLogSequenceNumber getLsn();

  void setLsn(OLogSequenceNumber lsn);

  boolean casLSN(OLogSequenceNumber currentLSN, OLogSequenceNumber newLSN);

  void setDistance(int distance);

  void setDiskSize(int diskSize);

  int getDistance();

  int getDiskSize();
}
