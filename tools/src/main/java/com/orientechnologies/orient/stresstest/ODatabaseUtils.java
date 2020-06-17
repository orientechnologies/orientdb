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
package com.orientechnologies.orient.stresstest;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * A collection of static methods for interacting with OrientDB
 *
 * @author Andrea Iacono
 */
public class ODatabaseUtils {

  public static void createDatabase(final ODatabaseIdentifier databaseIdentifier) throws Exception {
    switch (databaseIdentifier.getMode()) {
      case PLOCAL:
      case MEMORY:
        new ODatabaseDocumentTx(databaseIdentifier.getUrl()).create();
        break;
      case REMOTE:
        new OServerAdmin(databaseIdentifier.getUrl())
            .connect("root", databaseIdentifier.getPassword())
            .createDatabase(databaseIdentifier.getName(), "document", "plocal");
        break;
    }
  }

  public static ODatabase openDatabase(
      final ODatabaseIdentifier databaseIdentifier,
      final OStorageRemote.CONNECTION_STRATEGY connectionStrategy) {
    ODatabaseDocument database = null;

    switch (databaseIdentifier.getMode()) {
      case PLOCAL:
      case MEMORY:
        database = new ODatabaseDocumentTx(databaseIdentifier.getUrl()).open("admin", "admin");
        break;
      case REMOTE:
        database = new ODatabaseDocumentTx(databaseIdentifier.getUrl());
        database.setProperty(
            OStorageRemote.PARAM_CONNECTION_STRATEGY, connectionStrategy.toString());
        database.open("root", databaseIdentifier.getPassword());
        break;
    }

    return database;
  }

  public static void dropDatabase(ODatabaseIdentifier databaseIdentifier) throws Exception {

    switch (databaseIdentifier.getMode()) {
      case PLOCAL:
      case MEMORY:
        openDatabase(databaseIdentifier, OStorageRemote.CONNECTION_STRATEGY.STICKY).drop();
        break;
      case REMOTE:
        new OServerAdmin(databaseIdentifier.getUrl())
            .connect("root", databaseIdentifier.getPassword())
            .dropDatabase(databaseIdentifier.getName(), "plocal");
        break;
    }
  }
}
