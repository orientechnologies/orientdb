/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseThreadLocalFactory;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @author Luca Molino (molino.luca--at--gmail.com) */
@Test
public class DatabaseThreadFactoryTest extends DocumentDBBaseTest {
  private final OPartitionedDatabasePoolFactory poolFactory = new OPartitionedDatabasePoolFactory();

  @Parameters(value = "url")
  public DatabaseThreadFactoryTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {}

  @BeforeClass
  public void init() {
    try {
      ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
      db.close();
      ODatabaseRecordThreadLocal.instance().remove();
    } catch (ODatabaseException ode) {
    }
  }

  @Test(expectedExceptions = {ODatabaseException.class})
  public void testNoFactory() {
    ODatabaseRecordThreadLocal.instance().get();
    Assert.fail("Database Should not be set in Current Thread");
  }

  @Test(dependsOnMethods = "testNoFactory")
  public void testFactory() {
    Orient.instance()
        .registerThreadDatabaseFactory(
            new ODatabaseThreadLocalFactory() {

              @Override
              public ODatabaseDocumentInternal getThreadDatabase() {
                return poolFactory.get(url, "admin", "admin").acquire();
              }
            });
    ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
    Assert.assertNotNull(db);
    db.close();
  }
}
