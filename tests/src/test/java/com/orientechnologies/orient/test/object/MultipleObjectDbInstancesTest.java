/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.test.object;

import static org.testng.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.io.IOException;
import javax.persistence.Id;
import javax.persistence.Version;
import org.testng.annotations.Test;

/** Created by luigidellaquila on 01/07/15. */
public class MultipleObjectDbInstancesTest {
  /**
   * Scenario: create database, register Pojos, create another database, register Pojos again. Check
   * in both if Pojos exist in Schema.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testTwiceCreateDBSchemaRegistered() throws IOException {
    OrientDB orientDB = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database MultipleDbInstancesTest_first memory users(admin identified by 'adminpwd' role admin)");
    ODatabaseDocumentInternal db =
        (ODatabaseDocumentInternal)
            orientDB.open("MultipleDbInstancesTest_first", "admin", "adminpwd");
    OObjectDatabaseTx objectDb = new OObjectDatabaseTx(db);
    objectDb.setAutomaticSchemaGeneration(true);
    objectDb.getEntityManager().registerEntityClass(V.class);
    objectDb.getEntityManager().registerEntityClass(X.class);

    assertTrue(objectDb.getMetadata().getSchema().existsClass("V"));
    assertTrue(objectDb.getMetadata().getSchema().existsClass("X"));
    objectDb.close();

    orientDB.execute(
        "create database MultipleDbInstancesTest_second memory users(admin identified by 'adminpwd' role admin)");
    ODatabaseDocumentInternal db1 =
        (ODatabaseDocumentInternal)
            orientDB.open("MultipleDbInstancesTest_second", "admin", "adminpwd");
    OObjectDatabaseTx objectDb1 = new OObjectDatabaseTx(db1);

    assertTrue(objectDb1.getMetadata().getSchema().existsClass("V"));
    assertTrue(objectDb1.getMetadata().getSchema().existsClass("X"));
    objectDb1.close();
    orientDB.close();
  }

  public class V {
    @Id private Object graphId;
    @Version private Object graphVersion;

    public Object getGraphId() {
      return graphId;
    }

    public Object getGraphVersion() {
      return graphVersion;
    }
  }

  public class X extends V {}
}
