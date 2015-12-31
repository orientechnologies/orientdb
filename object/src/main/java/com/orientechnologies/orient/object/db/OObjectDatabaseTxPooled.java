/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.object.db;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.ODatabasePooled;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.security.OToken;

/**
 * Pooled wrapper to the OObjectDatabaseTx class. Allows to being reused across calls. The close() method does not close the
 * database for real but release it to the owner pool. The database born as opened and will leave open until the pool is closed.
 *
 * @author Luca Garulli
 * @see ODatabasePoolBase
 *
 */
@SuppressWarnings("unchecked")
public class OObjectDatabaseTxPooled extends OObjectDatabaseTx implements ODatabasePooled {

  private OObjectDatabasePool ownerPool;
  private final String        userName;

  public OObjectDatabaseTxPooled(final OObjectDatabasePool iOwnerPool, final String iURL, final String iUserName,
      final String iUserPassword) {
    super(iURL);
    ownerPool = iOwnerPool;
    userName = iUserName;
    super.open(iUserName, iUserPassword);
  }

  public void reuse(final Object iOwner, final Object[] iAdditionalArgs) {
    ownerPool = (OObjectDatabasePool) iOwner;
    if (isClosed())
      open((String) iAdditionalArgs[0], (String) iAdditionalArgs[1]);
    ODatabaseRecordThreadLocal.INSTANCE.set(getUnderlying());
    init();

    try {
      underlying.callOnOpenListeners();

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on reusing database '%s' in pool", e, getName());
    }
  }

  @Override
  public OObjectDatabaseTxPooled open(String iUserName, String iUserPassword) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a OObjectDatabaseTx instance if you want to manually open the connection");
  }


  @Override
  public OObjectDatabaseTxPooled open(OToken iToken) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a OObjectDatabaseTx instance if you want to manually open the connection");
  }

  @Override
  public OObjectDatabaseTxPooled create() {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a OObjectDatabaseTx instance if you want to manually open the connection");
  }

  @Override
  public boolean isClosed() {
    return ownerPool == null || super.isClosed();
  }

  /**
   * Avoid to close it but rather release itself to the owner pool.
   */
  @Override
  public void close() {
    if (isClosed())
      return;

    checkOpeness();
    if (ownerPool.getConnectionsInCurrentThread(getURL(), userName) > 1) {
      ownerPool.release(this);
      return;
    }

    objects2Records.clear();
    records2Objects.clear();
    rid2Records.clear();

    try {
      commit(true);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    try {
      underlying.callOnCloseListeners();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    getLocalCache().clear();

    if (ownerPool != null) {
      final OObjectDatabasePool localCopy = ownerPool;
      ownerPool = null;
      localCopy.release(this);
    }
  }

  public void forceClose() {
    super.close();
  }

  @Override
  protected void checkOpeness() {
    if (ownerPool == null)
      throw new ODatabaseException(
          "Database instance has been released to the pool. Get another database instance from the pool with the right username and password");

    super.checkOpeness();
  }

  public boolean isUnderlyingOpen() {
    return !super.isClosed();
  }
}
