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

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import junit.framework.Assert;
import org.junit.Test;

/** Start 3 servers and wait for external commands */
public class ServerClusterSchemaIT extends AbstractServerClusterTest {
  static final int SERVERS = 3;

  public String getDatabaseName() {
    return "distributed-schema";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    for (int s = 0; s < SERVERS; ++s) {
      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        System.out.println("Creating vertex class Client" + s + " against server " + g + "...");
        OClass t = g.createVertexClass("Client" + s);
        t.createProperty("name", OType.STRING).setMandatory(true);

        System.out.println("Creating vertex class Knows" + s + " against server " + g + "...");
        g.createEdgeClass("Knows" + s);
      } finally {
        g.close();
      }
    }

    for (int s = 0; s < SERVERS; ++s) {
      System.out.println("Checking vertices classes on server " + s + "...");
      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        for (int i = 0; i < SERVERS; ++i) {
          Assert.assertNotNull(g.getClass("Client" + i));
          Assert.assertNotNull(g.getClass("Knows" + i));
        }
      } finally {
        g.close();
      }
    }

    for (int s = 0; s < SERVERS; ++s) {
      System.out.println("Add vertices on server " + s + "...");

      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        for (int i = 0; i < SERVERS; ++i) {
          try {
            final OVertex v = g.newVertex("Client" + i).save();
            Assert.assertTrue(false);
          } catch (OValidationException e) {
            // EXPECTED
          }
        }
      } finally {
        g.close();
      }
    }

    for (int s = 0; s < SERVERS; ++s) {
      System.out.println("Add vertices in TX on server " + s + "...");

      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      g.begin();

      try {
        for (int i = 0; i < SERVERS; ++i) {
          try {
            final OVertex v = g.newVertex("Client" + i).save();
            g.commit();
            g.begin();

            Assert.assertTrue(false);
          } catch (ONeedRetryException e) {
            // EXPECTED
          } catch (OValidationException e) {
            // EXPECTED
          }
        }
      } finally {
        g.close();
      }
    }
  }
}
