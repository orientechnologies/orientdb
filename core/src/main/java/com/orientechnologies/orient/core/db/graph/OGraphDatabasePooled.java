/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.ODatabasePooled;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * Pooled wrapper to the ODatabaseObjectTx class. Allows to being reused across calls. The close() method does not close the
 * database for real but release it to the owner pool. The database born as opened and will leave open until the pool is closed.
 * 
 * @author Luca Garulli
 * @see ODatabasePoolBase
 * 
 */
@SuppressWarnings("unchecked")
public class OGraphDatabasePooled extends OGraphDatabase implements ODatabasePooled {

  private OGraphDatabasePool ownerPool;

  public OGraphDatabasePooled(final OGraphDatabasePool iOwnerPool, final String iURL, final String iUserName,
      final String iUserPassword) {
    super(iURL);
    ownerPool = iOwnerPool;
    super.open(iUserName, iUserPassword);
  }

  public void reuse(final Object iOwner, final Object[] iAdditionalArgs) {
    ownerPool = (OGraphDatabasePool) iOwner;
    if (isClosed())
      open((String) iAdditionalArgs[0], (String) iAdditionalArgs[1]);
    getLevel1Cache().invalidate();
    // getMetadata().reload();
    ODatabaseRecordThreadLocal.INSTANCE.set(this);
    checkForGraphSchema();

    try {
      ODatabase current = underlying;
      while (!(current instanceof ODatabaseRaw) && ((ODatabaseComplex<?>) current).getUnderlying() != null)
        current = ((ODatabaseComplex<?>) current).getUnderlying();
      ((ODatabaseRaw) current).callOnOpenListeners();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on reusing database '%s' in pool", e, getName());
    }
  }

  @Override
  public OGraphDatabasePooled open(String iUserName, String iUserPassword) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a OGraphDatabase instance if you want to manually open the connection");
  }

  @Override
  public OGraphDatabasePooled create() {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a OGraphDatabase instance if you want to manually open the connection");
  }

  public boolean isUnderlyingOpen() {
    return !super.isClosed();
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

    vertexBaseClass = null;
    edgeBaseClass = null;

    checkOpeness();
    try {
      rollback();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    try {
      ODatabase current = underlying;
      while (!(current instanceof ODatabaseRaw) && ((ODatabaseComplex<?>) current).getUnderlying() != null)
        current = ((ODatabaseComplex<?>) current).getUnderlying();

      ((ODatabaseRaw) current).callOnCloseListeners();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    getLevel1Cache().clear();

    final OGraphDatabasePool pool = ownerPool;
    ownerPool = null;
    pool.release(this);
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

}
