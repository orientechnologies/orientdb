package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;

/**
 * Created by tglman on 02/01/17.
 */
public class ORemoteQueryResult {
  private OTodoResultSet result;
  private boolean        transactionUpdated;

  public ORemoteQueryResult(OTodoResultSet result, boolean transactionUpdated) {
    this.result = result;
    this.transactionUpdated = transactionUpdated;
  }

  public OTodoResultSet getResult() {
    return result;
  }

  public boolean isTransactionUpdated() {
    return transactionUpdated;
  }
}
