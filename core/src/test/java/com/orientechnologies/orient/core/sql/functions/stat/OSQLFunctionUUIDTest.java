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

package com.orientechnologies.orient.core.sql.functions.stat;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionUUID;
import org.junit.Before;
import org.junit.Test;

public class OSQLFunctionUUIDTest {

  private OSQLFunctionUUID uuid;

  @Before
  public void setup() {
    uuid = new OSQLFunctionUUID();
  }

  @Test
  public void testEmpty() {
    Object result = uuid.getResult();
    assertNull(result);
  }

  @Test
  public void testResult() {
    String result = (String) uuid.execute(null, null, null, null, null);
    assertNotNull(result);
  }

  @Test
  public void testQuery() {
    try (OrientDB ctx = new OrientDB("embedded:./", OrientDBConfig.defaultConfig())) {
      ctx.execute("create database test memory users(admin identified by 'adminpwd' role admin)");
      try (ODatabaseDocument db = ctx.open("test", "admin", "adminpwd")) {

        try (final OResultSet result = db.query("select uuid() as uuid")) {
          assertNotNull(result.next().getProperty("uuid"));
          assertFalse(result.hasNext());
        }
      }
      ctx.drop("test");
    }
  }
}
