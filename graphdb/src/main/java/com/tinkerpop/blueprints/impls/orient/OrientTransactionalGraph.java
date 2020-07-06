/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.tinkerpop.blueprints.TransactionalGraph;
import org.apache.commons.configuration.Configuration;

/**
 * A Blueprints implementation of the graph database OrientDB (http://orientdb.com)
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com)
 */
public abstract class OrientTransactionalGraph extends OrientBaseGraph
    implements TransactionalGraph {

  /**
   * Constructs a new object using an existent database instance.
   *
   * @param iDatabase Underlying database object to attach
   */
  protected OrientTransactionalGraph(final ODatabaseDocumentInternal iDatabase) {
    this(iDatabase, true, null, null);
  }

  protected OrientTransactionalGraph(
      final ODatabaseDocumentInternal iDatabase,
      final String iUserName,
      final String iUserPasswd,
      final Settings iConfiguration) {
    super(iDatabase, iUserName, iUserPasswd, iConfiguration);
    setCurrentGraphInThreadLocal();
    super.setAutoStartTx(isAutoStartTx());

    if (isAutoStartTx()) ensureTransaction();
  }

  protected OrientTransactionalGraph(
      final ODatabaseDocumentInternal iDatabase,
      final boolean iAutoStartTx,
      final String iUserName,
      final String iUserPasswd) {
    super(iDatabase, iUserName, iUserPasswd, null);
    setCurrentGraphInThreadLocal();
    super.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx) ensureTransaction();
  }

  protected OrientTransactionalGraph(final OPartitionedDatabasePool pool) {
    super(pool);
    setCurrentGraphInThreadLocal();

    ensureTransaction();
  }

  protected OrientTransactionalGraph(
      final OPartitionedDatabasePool pool, final Settings configuration) {
    super(pool, configuration);
    setCurrentGraphInThreadLocal();

    if (configuration.isAutoStartTx()) ensureTransaction();
  }

  protected OrientTransactionalGraph(final String url) {
    this(url, true);
  }

  protected OrientTransactionalGraph(final String url, final boolean iAutoStartTx) {
    super(url, ADMIN, ADMIN);
    setCurrentGraphInThreadLocal();
    setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx) ensureTransaction();
  }

  protected OrientTransactionalGraph(
      final String url, final String username, final String password) {
    this(url, username, password, true);
  }

  protected OrientTransactionalGraph(
      final String url, final String username, final String password, final boolean iAutoStartTx) {
    super(url, username, password);
    setCurrentGraphInThreadLocal();
    this.setAutoStartTx(iAutoStartTx);

    if (iAutoStartTx) ensureTransaction();
  }

  protected OrientTransactionalGraph(final Configuration configuration) {
    super(configuration);

    final Boolean autoStartTx = configuration.getBoolean("blueprints.orientdb.autoStartTx", null);
    if (autoStartTx != null) setAutoStartTx(autoStartTx);
  }

  public boolean isUseLog() {
    makeActive();

    return settings.isUseLog();
  }

  public OrientTransactionalGraph setUseLog(final boolean useLog) {
    makeActive();

    settings.setUseLog(useLog);
    return this;
  }

  @Override
  public void setAutoStartTx(boolean autoStartTx) {
    makeActive();

    final boolean showWarning;
    if (!autoStartTx
        && isAutoStartTx()
        && getDatabase() != null
        && getDatabase().getTransaction().isActive()) {
      if (getDatabase().getTransaction().getEntryCount() == 0) {
        getDatabase().getTransaction().rollback();
        showWarning = false;
      } else showWarning = true;
    } else showWarning = false;

    super.setAutoStartTx(autoStartTx);

    if (showWarning)
      OLogManager.instance()
          .warn(
              this,
              "Auto Transaction for graphs setting has been turned off, but a transaction was already started."
                  + " Commit it manually or consider disabling auto transactions while creating the graph.");
  }

  /**
   * Closes a transaction.
   *
   * @param conclusion Can be SUCCESS for commit and FAILURE to rollback.
   */
  @SuppressWarnings("deprecation")
  @Override
  public void stopTransaction(final Conclusion conclusion) {
    makeActive();

    if (getDatabase().isClosed()
        || getDatabase().getTransaction() instanceof OTransactionNoTx
        || getDatabase().getTransaction().getStatus() != TXSTATUS.BEGUN) return;

    if (Conclusion.SUCCESS == conclusion) commit();
    else rollback();
  }

  /** Commits the current active transaction. */
  public void commit() {
    makeActive();

    if (getDatabase() == null) return;

    getDatabase().commit();
    if (isAutoStartTx()) ensureTransaction();
  }

  /** Rollbacks the current active transaction. All the pending changes are rollbacked. */
  public void rollback() {
    makeActive();

    if (getDatabase() == null) return;

    getDatabase().rollback();
    if (isAutoStartTx()) ensureTransaction();
  }

  @Override
  public void begin() {
    makeActive();

    // XXX: Under some circumstances, auto started transactions are committed outside of the graph
    // using the
    // underlying database and later restarted using the graph. So we have to check the status of
    // the
    // database transaction to support this behaviour.
    if (isAutoStartTx() && getDatabase().getTransaction().isActive())
      throw new OTransactionException(
          "A mixture of auto started and manually started transactions is not allowed. "
              + "Disable auto transactions for the graph before starting a manual transaction.");

    getDatabase().begin();
    getDatabase().getTransaction().setUsingLog(settings.isUseLog());
  }

  @Override
  protected void autoStartTransaction() {
    final boolean txBegun = getDatabase().getTransaction().isActive();

    if (!isAutoStartTx()) {
      if (isRequireTransaction() && !txBegun)
        throw new OTransactionException("Transaction required to change the Graph");

      return;
    }

    if (!txBegun) {
      getDatabase().begin();
      getDatabase().getTransaction().setUsingLog(settings.isUseLog());
    }
  }

  private void ensureTransaction() {
    final boolean txBegun = getDatabase().getTransaction().isActive();
    if (!txBegun) {
      getDatabase().begin();
      getDatabase().getTransaction().setUsingLog(settings.isUseLog());
    }
  }
}
