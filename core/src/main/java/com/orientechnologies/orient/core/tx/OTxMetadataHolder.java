package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTxMetadataHolderLocal;

public interface OTxMetadataHolder {

  byte[] metadata();

  void notifyMetadataRead();

  OTransactionId getId();

  OTransactionSequenceStatus getStatus();

  static OTxMetadataHolder read(byte[] data) {
    return OTxMetadataHolderLocal.read(data);
  }
}
