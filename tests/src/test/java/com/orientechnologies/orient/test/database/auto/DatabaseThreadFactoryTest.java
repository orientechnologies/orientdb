/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseThreadLocalFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * @author luca.molino
 * 
 */
@Test
public class DatabaseThreadFactoryTest extends DocumentDBBaseTest {
	private final OPartitionedDatabasePoolFactory poolFactory = new OPartitionedDatabasePoolFactory();

  @Parameters(value = "url")
  public DatabaseThreadFactoryTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
  }

  @BeforeClass
  public void init() {
    try {
      ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
      db.close();
      ODatabaseRecordThreadLocal.INSTANCE.remove();
    } catch (ODatabaseException ode) {
    }
  }

  @Test(expectedExceptions = { ODatabaseException.class })
  public void testNoFactory() {
    ODatabaseRecordThreadLocal.INSTANCE.get();
    Assert.fail("Database Should not be set in Current Thread");
  }

  @Test(dependsOnMethods = "testNoFactory")
  public void testFactory() {
    Orient.instance().registerThreadDatabaseFactory(new ODatabaseThreadLocalFactory() {

      @Override
      public ODatabaseDocumentInternal getThreadDatabase() {
        return poolFactory.get(url, "admin", "admin").acquire();
      }
    });
    ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    Assert.assertNotNull(db);
    db.close();
  }
}
