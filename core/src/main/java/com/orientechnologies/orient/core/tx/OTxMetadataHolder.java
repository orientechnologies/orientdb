package com.orientechnologies.orient.core.tx;

public interface OTxMetadataHolder {

  byte[] metadata();

  void notifyMetadataRead();

  OTransactionId getId();

  OTransactionSequenceStatus getStatus();

}
