package com.tinkerpop.blueprints.impls.orient;

import org.apache.commons.configuration.Configuration;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientTransactionalGraph extends OrientBaseGraph implements TransactionalGraph {
  protected boolean autoStartTx        = true;
  protected boolean useLog             = true;

  /**
   * Constructs a new object using an existent database instance.
   *
   * @param iDatabase
   *          Underlying database object to attach
   */
  protected OrientTransactionalGraph(final ODatabaseDocumentTx iDatabase) {
    this(iDatabase, true, null, null);
  }

  protected OrientTransactionalGraph(final ODatabaseDocumentTx iDatabase, final boolean iAutoStartTx, final String iUserName,
      final String iUserPasswd) {
    super(iDatabase, iUserName, iUserPasswd);
    setCurrentGraphInThreadLocal();
    this.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx)
      begin();
  }

  protected OrientTransactionalGraph(final ODatabaseDocumentPool pool) {
    super(pool);
    setCurrentGraphInThreadLocal();

    begin();
  }

  protected OrientTransactionalGraph(final String url) {
    this(url, true);
  }

  protected OrientTransactionalGraph(final String url, final boolean iAutoStartTx) {
    super(url, ADMIN, ADMIN);
    setCurrentGraphInThreadLocal();
    this.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx)
      begin();
  }

  protected OrientTransactionalGraph(final String url, final String username, final String password) {
    this(url, username, password, true);
  }

  protected OrientTransactionalGraph(final String url, final String username, final String password, final boolean iAutoStartTx) {
    super(url, username, password);
    setCurrentGraphInThreadLocal();
    this.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx)
      begin();
  }

  protected OrientTransactionalGraph(final Configuration configuration) {
    super(configuration);

    final Boolean autoStartTx = configuration.getBoolean("blueprints.orientdb.autoStartTx", null);
    if (autoStartTx != null)
      setAutoStartTx(autoStartTx);
  }

  public boolean isUseLog() {
    return useLog;
  }

  public OrientTransactionalGraph setUseLog(final boolean useLog) {
    this.useLog = useLog;
    return this;
  }

  /**
   * Closes a transaction.
   * 
   * @param conclusion
   *          Can be SUCCESS for commit and FAILURE to rollback.
   */
  @SuppressWarnings("deprecation")
  @Override
  public void stopTransaction(final Conclusion conclusion) {
    if (database.isClosed() || database.getTransaction() instanceof OTransactionNoTx
        || database.getTransaction().getStatus() != TXSTATUS.BEGUN)
      return;

    if (Conclusion.SUCCESS == conclusion)
      commit();
    else
      rollback();
  }

  /**
   * Commits the current active transaction.
   */
  public void commit() {
    if (database == null)
      return;

    database.commit();
    if (autoStartTx)
      begin();
  }

  /**
   * Rollbacks the current active transaction. All the pending changes are rollbacked.
   */
  public void rollback() {
    if (database == null)
      return;

    database.rollback();
    if (autoStartTx)
      begin();
  }

  /**
   * Tells if a transaction is started automatically when the graph is changed. This affects only when a transaction hasn't been
   * started. Default is true.
   * 
   * @return
   */
  public boolean isAutoStartTx() {
    return autoStartTx;
  }

  /**
   * If enabled auto starts a new transaction right before the graph is changed. This affects only when a transaction hasn't been
   * started. Default is true.
   * 
   * @param autoStartTx
   */
  public void setAutoStartTx(final boolean autoStartTx) {
    this.autoStartTx = autoStartTx;
  }

  @Override
  protected void autoStartTransaction() {
    final boolean txBegun = database.getTransaction().isActive();

    if (!autoStartTx) {
      if (settings.requireTransaction && !txBegun)
        throw new OTransactionException("Transaction required to change the Graph");

      return;
    }

    if (!txBegun)
      begin();
  }

  public void begin() {
    database.begin();
    database.getTransaction().setUsingLog(useLog);
  }

}
