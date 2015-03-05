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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.WeakHashMap;

/**
 * Factory to create high-level ODatabase instances. The global instance is managed by Orient class.
 * 
 * @author Luca Garulli
 * 
 */
public class ODatabaseFactory {
  final WeakHashMap<ODatabaseInternal<?>, Thread> instances = new WeakHashMap<ODatabaseInternal<?>, Thread>();

  public synchronized List<ODatabase<?>> getInstances(final String iDatabaseName) {
    final List<ODatabase<?>> result = new ArrayList<ODatabase<?>>();
    for (ODatabase<?> i : instances.keySet()) {
      if (i != null && i.getName().equals(iDatabaseName)) {
          result.add(i);
      }
    }

    return result;
  }

  /**
   * Registers a database.
   * 
   * @param db
   * @return
   */
  public synchronized ODatabase<?> register(final ODatabaseInternal<?> db) {
    instances.put(db, Thread.currentThread());
    return db;
  }

  /**
   * Unregisters a database.
   * 
   * @param db
   */
  public synchronized void unregister(final ODatabaseInternal<?> db) {
    instances.remove(db);
  }

  /**
   * Unregisters all the database instances that share the storage received as argument.
   * 
   * @param iStorage
   */
  public synchronized void unregister(final OStorage iStorage) {
    for (ODatabaseInternal<?> db : new HashSet<ODatabaseInternal<?>>(instances.keySet())) {
      if (db != null && db.getStorage() == iStorage) {
        db.close();
        instances.remove(db);
      }
    }
  }

  /**
   * Closes all open databases.
   */
  public synchronized void shutdown() {
    if (instances.size() > 0) {
      OLogManager.instance().debug(null,
          "Found %d databases opened during OrientDB shutdown. Assure to always close database instances after usage",
          instances.size());

      for (ODatabase<?> db : new HashSet<ODatabase<?>>(instances.keySet())) {
        if (db != null && !db.isClosed()) {
          db.close();
        }
      }
    }
  }

  public ODatabaseDocumentTx createDatabase(final String iType, final String url) {
    if (iType.equals("graph")) {
        return new ODatabaseDocumentTx(url) {
            @Override
            public <THISDB extends ODatabase> THISDB create() {
                final THISDB db = super.create();
                
                checkSchema((ODatabase<?>) db);
                
                return db;
            }
            
        };
    }

    return new ODatabaseDocumentTx(url);
  }

  public void checkSchema(final ODatabase<?> iDatabase) {
    // FORCE NON DISTRIBUTION ON CREATION
    OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);
    try {

      iDatabase.getMetadata().getSchema().getOrCreateClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);

      OClass vertexBaseClass = iDatabase.getMetadata().getSchema().getClass("V");
      OClass edgeBaseClass = iDatabase.getMetadata().getSchema().getClass("E");

      if (vertexBaseClass == null) {
        // CREATE THE META MODEL USING THE ORIENT SCHEMA
        vertexBaseClass = iDatabase.getMetadata().getSchema().createClass("V");
        vertexBaseClass.setOverSize(2);
      }

      if (edgeBaseClass == null) {
          iDatabase.getMetadata().getSchema().createClass("E");
      }
    } finally {
      OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.DEFAULT);
    }
  }
}
