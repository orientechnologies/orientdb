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

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Migration tool. Keeps updated the graph with latest OrientDB release.
 * 
 * @author Luca Garulli
 * 
 */
public class OGraphDatabaseMigration {
  public static void main(final String[] iArgs) {
    if (iArgs.length < 1) {
      System.err.println("Error: wrong parameters. Syntax: <database-url> [<user> <password>]");
      return;
    }

    final String dbURL = iArgs[0];
    final String user = iArgs.length > 1 ? iArgs[1] : null;
    final String password = iArgs.length > 2 ? iArgs[2] : null;

    migrate(dbURL, user, password);
  }

  public static void migrate(final String dbURL, final String user, final String password) {
    migrate((OGraphDatabase) new OGraphDatabase(dbURL).open(user, password));
  }

  public static void migrate(final OGraphDatabase db) {
    System.out.println("Migration of database started...");
    final long start = System.currentTimeMillis();

    try {
      // CONVERT ALL THE VERTICES
      long convertedVertices = 0;
      for (ODocument doc : db.browseVertices()) {
        boolean converted = false;

        if (doc.containsField(OGraphDatabase.VERTEX_FIELD_IN_OLD)) {
          doc.field(OGraphDatabase.VERTEX_FIELD_IN, doc.field(OGraphDatabase.VERTEX_FIELD_IN_OLD));
          doc.removeField(OGraphDatabase.VERTEX_FIELD_IN_OLD);
          converted = true;
        }
        if (doc.containsField(OGraphDatabase.VERTEX_FIELD_OUT_OLD)) {
          doc.field(OGraphDatabase.VERTEX_FIELD_OUT, doc.field(OGraphDatabase.VERTEX_FIELD_OUT_OLD));
          doc.removeField(OGraphDatabase.VERTEX_FIELD_OUT_OLD);
          converted = true;
        }

        if (converted) {
          doc.save();
          convertedVertices++;
        }
      }

      System.out.println(String.format("Migration complete in %d seconds. Vertices converted: %d",
          (System.currentTimeMillis() - start) / 1000, convertedVertices));

    } finally {
      db.close();
    }
  }
}
