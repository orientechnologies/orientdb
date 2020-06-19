package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

public class OrientNoTransaction extends OrientTransaction {

  public OrientNoTransaction(OrientGraph g) {
    super(g);
  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  protected void doOpen() {
    // do nothing, no transaction
  }

  @Override
  protected void doCommit() throws TransactionException {
    // do nothing, no transaction
  }

  @Override
  protected void doRollback() throws TransactionException {
    // do nothing, no transaction
  }
}
