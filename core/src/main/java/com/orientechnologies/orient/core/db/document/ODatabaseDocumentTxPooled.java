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
package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.ODatabasePooled;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.security.OToken;

/**
 * Pooled wrapper to the ODatabaseDocumentTx class. Allows to being reused across calls. The close()
 * method does not close the database for real but release it to the owner pool. The database born
 * as opened and will leave open until the pool is closed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see ODatabasePoolBase
 */
@SuppressWarnings("unchecked")
public class ODatabaseDocumentTxPooled extends ODatabaseDocumentTx implements ODatabasePooled {

  private ODatabaseDocumentPool ownerPool;
  private String userName;

  public ODatabaseDocumentTxPooled(
      final ODatabaseDocumentPool iOwnerPool,
      final String iURL,
      final String iUserName,
      final String iUserPassword) {
    super(iURL);
    ownerPool = iOwnerPool;
    userName = iUserName;

    super.open(iUserName, iUserPassword);
  }

  public void reuse(final Object iOwner, final Object[] iAdditionalArgs) {
    ownerPool = (ODatabaseDocumentPool) iOwner;
    getLocalCache().invalidate();
    // getMetadata().reload();
    ODatabaseRecordThreadLocal.instance().set(this);

    try {
      callOnOpenListeners();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on reusing database '%s' in pool", e, getName());
    }
  }

  @Override
  public ODatabaseDocumentTxPooled open(final String iUserName, final String iUserPassword) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a ODatabaseDocumentTx instance if you want to manually open the connection");
  }

  @Override
  public ODatabaseDocumentTxPooled open(final OToken iToken) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a ODatabaseDocumentTx instance if you want to manually open the connection");
  }

  @Override
  public ODatabaseDocumentTxPooled create() {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a ODatabaseDocumentTx instance if you want to manually open the connection");
  }

  @Override
  public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a ODatabaseDocumentTx instance if you want to manually open the connection");
  }

  public boolean isUnderlyingOpen() {
    return !super.isClosed();
  }

  @Override
  public boolean isClosed() {
    return ownerPool == null || super.isClosed();
  }

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code>
   *     otherwise.
   */
  @Override
  public boolean isPooled() {
    return true;
  }

  /** Avoid to close it but rather release itself to the owner pool. */
  @Override
  public void close() {
    if (isClosed()) return;

    checkOpenness();

    if (ownerPool != null && ownerPool.getConnectionsInCurrentThread(getURL(), userName) > 1) {
      ownerPool.release(this);
      return;
    }

    try {
      commit(true);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    try {
      callOnCloseListeners();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    getLocalCache().clear();

    if (ownerPool != null) {
      final ODatabaseDocumentPool localCopy = ownerPool;
      ownerPool = null;
      localCopy.release(this);
    }

    ODatabaseRecordThreadLocal.instance().remove();
  }

  public void forceClose() {
    super.close();
  }

  //  @Override
  protected void checkOpenness() {
    if (ownerPool == null)
      throw new ODatabaseException(
          "Database instance has been released to the pool. Get another database instance from the pool with the right username and password");

    //    super.checkOpenness();
  }
}
