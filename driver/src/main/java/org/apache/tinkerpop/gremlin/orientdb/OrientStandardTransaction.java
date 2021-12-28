package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentAbstract;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

/** Created by Enrico Risa on 30/08/2017. */
public class OrientStandardTransaction extends AbstractTransaction {

  private OrientStandardGraph g;

  public OrientStandardTransaction(OrientStandardGraph graph) {
    super(graph);
    this.g = graph;
  }

  @Override
  protected void doOpen() {
    tx().doOpen();
  }

  @Override
  protected void doCommit() throws TransactionException {
    tx().doCommit();
    ODatabaseDocumentAbstract db = (ODatabaseDocumentAbstract) g.graph().getRawDatabase();
    db.internalClose(true);
    db.activateOnCurrentThread();
    db.setStatus(ODatabase.STATUS.OPEN);
  }

  @Override
  protected void doRollback() throws TransactionException {
    tx().doRollback();
    ODatabaseDocumentAbstract db = (ODatabaseDocumentAbstract) g.graph().getRawDatabase();
    db.internalClose(true);
    db.activateOnCurrentThread();
    db.setStatus(ODatabase.STATUS.OPEN);
  }

  @Override
  protected void fireOnCommit() {
    tx().fireOnCommit();
  }

  @Override
  protected void fireOnRollback() {
    tx().fireOnRollback();
  }

  @Override
  protected void doReadWrite() {
    tx().doReadWrite();
  }

  @Override
  protected void doClose() {
    tx().doClose();
  }

  @Override
  public Transaction onReadWrite(Consumer<Transaction> consumer) {
    return tx().onReadWrite(consumer);
  }

  @Override
  public Transaction onClose(Consumer<Transaction> consumer) {
    return tx().onClose(consumer);
  }

  @Override
  public void addTransactionListener(Consumer<Status> listener) {
    tx().addTransactionListener(listener);
  }

  @Override
  public void removeTransactionListener(Consumer<Status> listener) {
    tx().removeTransactionListener(listener);
  }

  @Override
  public void clearTransactionListeners() {
    tx().clearTransactionListeners();
  }

  @Override
  public boolean isOpen() {
    if (g.isOpen()) {
      return tx().isOpen();
    } else {
      return false;
    }
  }

  private OrientTransaction tx() {
    return g.graph().tx();
  }
}
