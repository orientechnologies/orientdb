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
package com.orientechnologies.orient.stresstest.util;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.List;

/**
 * A collection of static methods for interacting with OrientDB
 *
 * @author Andrea Iacono
 */
public class ODatabaseUtils {

    public static void createDatabase(ODatabaseIdentifier databaseIdentifier) throws Exception {
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

    public static ODatabase openDatabase(ODatabaseIdentifier databaseIdentifier) {
        switch (databaseIdentifier.getMode()) {
            case PLOCAL:
            case MEMORY:
                return new ODatabaseDocumentTx(databaseIdentifier.getUrl()).open("admin", "admin");
            case REMOTE:
                return new ODatabaseDocumentTx(databaseIdentifier.getUrl()).open("root", databaseIdentifier.getPassword());
        }

        return null;
    }

    public static void dropDatabase(ODatabaseIdentifier databaseIdentifier) throws Exception {

        switch (databaseIdentifier.getMode()) {
            case PLOCAL:
            case MEMORY:
                openDatabase(databaseIdentifier).drop();
                break;
            case REMOTE:
                new OServerAdmin(databaseIdentifier.getUrl())
                        .connect("root", databaseIdentifier.getPassword())
                        .dropDatabase(databaseIdentifier.getName(), "plocal");
                break;
        }
    }

    public static ODocument createOperation(int n) {
        ODocument doc = new ODocument(OConstants.CLASS_NAME);
        doc.field("name", getThreadValue(n));
        doc.save();
        return doc;
    }

    public static void readOperation(ODatabase database, int n) throws Exception {
        String query = String.format("SELECT FROM %s WHERE name = ?", OConstants.CLASS_NAME);
        List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(query)).execute(getThreadValue(n));
        if (result.size() != 1) {
            throw new Exception(String.format("The query [%s] result size is %d. Expected size is 1.", query, result.size()));
        }
    }

    public static void deleteOperation(ODocument doc) {
        doc.delete();
    }

    public static void updateOperation(ODocument doc, int n) {
        doc.field("name", getThreadValue(n, "new"));
        doc.save();
    }

    private static String getThreadValue(int n) {
        return getThreadValue(n, "");
    }

    private static String getThreadValue(int n, String prefix) {
        return prefix + "value-" + Thread.currentThread().getId() + "-" + n;
    }
}
