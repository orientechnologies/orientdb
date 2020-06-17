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
package com.orientechnologies.orient.stresstest.workload;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.ODatabaseUtils;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OBaseDocumentWorkload extends OBaseWorkload {
  public class OWorkLoadContext extends OBaseWorkLoadContext {
    private ODatabase db;

    @Override
    public void init(final ODatabaseIdentifier dbIdentifier, int operationsPerTransaction) {
      db = getDocumentDatabase(dbIdentifier, connectionStrategy);
    }

    @Override
    public void close() {
      if (getDb() != null) getDb().close();
    }

    public ODatabase getDb() {
      return db;
    }
  }

  @Override
  protected OBaseWorkLoadContext getContext() {
    return new OWorkLoadContext();
  }

  protected ODatabase getDocumentDatabase(
      final ODatabaseIdentifier databaseIdentifier,
      final OStorageRemote.CONNECTION_STRATEGY connectionStrategy) {
    // opens the newly created db and creates an index on the class we're going to use
    final ODatabase database = ODatabaseUtils.openDatabase(databaseIdentifier, connectionStrategy);

    if (database == null)
      throw new IllegalArgumentException(
          "Error on opening database " + databaseIdentifier.getName());

    return database;
  }

  @Override
  protected void beginTransaction(final OBaseWorkLoadContext context) {
    ((OWorkLoadContext) context).db.begin();
  }

  @Override
  protected void commitTransaction(final OBaseWorkLoadContext context) {
    ((OWorkLoadContext) context).db.commit();
  }
}
