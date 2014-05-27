package com.tinkerpop.blueprints.impls.orient;

import org.apache.commons.configuration.Configuration;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientTransactionalGraph extends OrientBaseGraph implements TransactionalGraph {
  protected boolean autoStartTx = true;

  /**
   * Constructs a new object using an existent database instance.
   * 
   * @param iDatabase
   *          Underlying database object to attach
   */
  protected OrientTransactionalGraph(final ODatabaseDocumentTx iDatabase) {
    this(iDatabase, true);
  }

  protected OrientTransactionalGraph(final ODatabaseDocumentTx iDatabase, final boolean iAutoStartTx) {
    super(iDatabase);
    setCurrentGraphInThreadLocal();
    this.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx)
      getContext(false).rawGraph.begin();
  }

  protected OrientTransactionalGraph(final ODatabaseDocumentPool pool) {
    super(pool);
    setCurrentGraphInThreadLocal();

    getContext(false).rawGraph.begin();
  }

  protected OrientTransactionalGraph(final String url) {
    this(url, true);
  }

  protected OrientTransactionalGraph(final String url, final boolean iAutoStartTx) {
    super(url, ADMIN, ADMIN);
    setCurrentGraphInThreadLocal();
    this.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx)
      getContext(false).rawGraph.begin();
  }

  protected OrientTransactionalGraph(final String url, final String username, final String password) {
    this(url, username, password, true);
  }

  protected OrientTransactionalGraph(final String url, final String username, final String password, final boolean iAutoStartTx) {
    super(url, username, password);
    setCurrentGraphInThreadLocal();
    this.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx)
      getContext(false).rawGraph.begin();
  }

  protected OrientTransactionalGraph(final Configuration configuration) {
    super(configuration);

    final Boolean autoStartTx = configuration.getBoolean("blueprints.orientdb.autoStartTx", null);
    if (autoStartTx != null)
      setAutoStartTx(autoStartTx);
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
    final OrientGraphContext context = getContext(false);
    if (context == null)
      return;

    if (context.rawGraph.isClosed() || context.rawGraph.getTransaction() instanceof OTransactionNoTx
        || context.rawGraph.getTransaction().getStatus() != TXSTATUS.BEGUN)
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
    final OrientGraphContext context = getContext(false);
    if (context == null)
      return;

    context.rawGraph.commit();
    if (autoStartTx)
      getContext(false).rawGraph.begin();
  }

  /**
   * Rollbacks the current active transaction. All the pending changes are rollbacked.
   */
  public void rollback() {
    final OrientGraphContext context = getContext(false);
    if (context == null)
      return;

    context.rawGraph.rollback();
    if (autoStartTx)
      getContext(false).rawGraph.begin();
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
    if (!autoStartTx)
      return;

    final OrientGraphContext context = getContext(true);
    if (context.rawGraph.getTransaction() instanceof OTransactionNoTx
        && context.rawGraph.getTransaction().getStatus() != TXSTATUS.BEGUN) {
      context.rawGraph.begin();
    }
  }
}
