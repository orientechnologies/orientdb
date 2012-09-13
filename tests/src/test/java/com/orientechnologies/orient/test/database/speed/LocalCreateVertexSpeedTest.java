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
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class LocalCreateVertexSpeedTest {
  public static void main(String[] args) {
    OGraphDatabase db = new OGraphDatabase("local:C:/temp/databases/graphtest");

    if (db.exists())
      db.open("admin", "admin").drop();

    db.create();

    db.declareIntent(new OIntentMassiveInsert());

    final long begin = System.currentTimeMillis();

    ODocument doc = db.createVertex();
    for (int i = 0; i < 1000000; ++i) {

      doc.reset();
      doc.setClassName("V");

      doc.field("id", i);
      doc.field("name", "Jason");
      doc.save();
    }
    
    db.declareIntent(null);
    db.close();

    System.out.println("Total time: " + (System.currentTimeMillis() - begin));

  }
}
