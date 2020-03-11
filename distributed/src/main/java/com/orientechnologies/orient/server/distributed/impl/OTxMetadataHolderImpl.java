package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.tx.OTxMetadataHolder;

import java.util.concurrent.CountDownLatch;

public class OTxMetadataHolderImpl implements OTxMetadataHolder {
  private final CountDownLatch request;
  private final byte[]         status;

  public OTxMetadataHolderImpl(CountDownLatch request, byte[] status) {
    this.request = request;
    this.status = status;
  }

  @Override
  public byte[] metadata() {
    return status;
  }

  @Override
  public void notifyMetadataRead() {
    request.countDown();
  }
}
