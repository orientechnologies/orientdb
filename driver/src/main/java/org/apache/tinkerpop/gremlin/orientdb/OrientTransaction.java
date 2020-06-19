package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.tx.OTransaction;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

public class OrientTransaction extends AbstractTransaction {

  protected OrientGraph graph;

  protected Consumer<Transaction> readWriteConsumerInternal = READ_WRITE_BEHAVIOR.AUTO;
  protected Consumer<Transaction> closeConsumerInternal = CLOSE_BEHAVIOR.ROLLBACK;
  protected final List<Consumer<Status>> transactionListeners = new CopyOnWriteArrayList<>();

  public OrientTransaction(OrientGraph graph) {
    super(graph);
    this.graph = graph;
  }

  @Override
  public boolean isOpen() {
    return this.tx().isActive();
  }

  @Override
  public Transaction onReadWrite(Consumer<Transaction> consumer) {
    this.readWriteConsumerInternal =
        Optional.ofNullable(consumer)
            .orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull);
    return this;
  }

  @Override
  public Transaction onClose(Consumer<Transaction> consumer) {
    this.closeConsumerInternal =
        Optional.ofNullable(consumer)
            .orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull);
    return this;
  }

  @Override
  public void addTransactionListener(Consumer<Status> listener) {
    transactionListeners.add(listener);
  }

  @Override
  public void removeTransactionListener(Consumer<Status> listener) {
    transactionListeners.remove(listener);
  }

  @Override
  public void clearTransactionListeners() {

    transactionListeners.clear();
  }

  @Override
  protected void doOpen() {
    this.db().begin();
  }

  @Override
  protected void doCommit() throws TransactionException {
    this.db().commit();
  }

  @Override
  protected void doClose() {
    closeConsumerInternal.accept(this);
  }

  @Override
  protected void doReadWrite() {
    readWriteConsumerInternal.accept(this);
  }

  @Override
  protected void fireOnCommit() {
    this.transactionListeners.forEach(c -> c.accept(Status.COMMIT));
  }

  @Override
  protected void fireOnRollback() {
    this.transactionListeners.forEach(c -> c.accept(Status.ROLLBACK));
  }

  @Override
  protected void doRollback() throws TransactionException {
    this.db().rollback();
  }

  protected OTransaction tx() {
    return graph.getRawDatabase().getTransaction();
  }

  protected ODatabaseDocument db() {
    return graph.getRawDatabase();
  }
}
