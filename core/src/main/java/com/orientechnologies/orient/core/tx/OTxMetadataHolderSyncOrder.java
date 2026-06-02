package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTxMetadataHolderLocal;
import java.util.concurrent.CountDownLatch;

public class OTxMetadataHolderSyncOrder extends OTxMetadataHolderLocal {
  private final CountDownLatch request;

  public OTxMetadataHolderSyncOrder(
      CountDownLatch request, OTransactionId id, OTransactionSequenceStatus status) {
    super(id, status);
    this.request = request;
  }

  @Override
  public void notifyMetadataRead() {
    request.countDown();
  }
}
