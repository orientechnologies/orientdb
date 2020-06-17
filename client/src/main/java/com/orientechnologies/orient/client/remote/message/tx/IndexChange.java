package com.orientechnologies.orient.client.remote.message.tx;

import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;

/** Created by tglman on 30/12/16. */
public class IndexChange {

  public IndexChange(String name, OTransactionIndexChanges keyChanges) {
    this.name = name;
    this.keyChanges = keyChanges;
  }

  private String name;
  private OTransactionIndexChanges keyChanges;

  public String getName() {
    return name;
  }

  public OTransactionIndexChanges getKeyChanges() {
    return keyChanges;
  }
}
