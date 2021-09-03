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
package com.orientechnologies.orient.object.jpa;

import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import javax.persistence.EntityTransaction;

public class OJPAEntityTransaction implements EntityTransaction {
  private final OObjectDatabaseTx database;

  public OJPAEntityTransaction(final OObjectDatabaseTx iDatabase) {
    database = iDatabase;
  }

  @Override
  public void begin() {
    database.getTransaction().begin();
  }

  @Override
  public void commit() {
    database.getTransaction().commit();
  }

  @Override
  public void rollback() {
    database.getTransaction().rollback();
  }

  @Override
  public void setRollbackOnly() {
    throw new UnsupportedOperationException("merge");
  }

  @Override
  public boolean getRollbackOnly() {
    return false;
  }

  @Override
  public boolean isActive() {
    return !(database.getTransaction() instanceof OTransactionNoTx);
  }
}
