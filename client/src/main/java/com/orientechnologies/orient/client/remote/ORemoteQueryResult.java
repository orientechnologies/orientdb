package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

/** Created by tglman on 02/01/17. */
public class ORemoteQueryResult {
  private OResultSet result;
  private boolean transactionUpdated;
  private boolean reloadMetadata;

  public ORemoteQueryResult(OResultSet result, boolean transactionUpdated, boolean reloadMetadata) {
    this.result = result;
    this.transactionUpdated = transactionUpdated;
    this.reloadMetadata = reloadMetadata;
  }

  public OResultSet getResult() {
    return result;
  }

  public boolean isTransactionUpdated() {
    return transactionUpdated;
  }

  public boolean isReloadMetadata() {
    return reloadMetadata;
  }
}
