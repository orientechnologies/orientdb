package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.transaction.OTransactionId;

public interface OTxMetadataHolder {

  byte[] metadata();

  void notifyMetadataRead();

  OTransactionId getId();

  OTransactionSequenceStatus getStatus();
}
