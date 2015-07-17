/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.domain.whiz.Profile;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Test(groups = "hook")
public class HookTxTest extends ORecordHookAbstract {
  public static final int   RECORD_BEFORE_CREATE = 3;
  public static final int   RECORD_AFTER_CREATE  = 5;
  public static final int   RECORD_BEFORE_READ   = 7;
  public static final int   RECORD_AFTER_READ    = 11;
  public static final int   RECORD_BEFORE_UPDATE = 13;
  public static final int   RECORD_AFTER_UPDATE  = 17;
  public static final int   RECORD_BEFORE_DELETE = 19;
  public static final int   RECORD_AFTER_DELETE  = 23;

  private OObjectDatabaseTx database;
  private int               callbackCount        = 0;
  private Profile           p;
  private int               expectedHookState;
  private String            url;

  @Parameters(value = "url")
  public HookTxTest(String url) {
    this.url = url;
  }

  @BeforeClass
  public void beforeClass() {
    database = new OObjectDatabaseTx(url);
    if (url.startsWith("memory:") && !database.exists()) {
      database.create();
      database.close();
    }
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Test
  public void testRegisterHook() throws IOException {
    database.open("admin", "admin");
    database.registerHook(this);
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");
  }

  @Test(dependsOnMethods = "testRegisterHook")
  public void testHookCallsCreate() {
    p = new Profile("HookTxTest");

    expectedHookState = 0;

    // TEST HOOKS ON CREATE
    Assert.assertEquals(callbackCount, 0);
    database.begin();
    database.save(p);
    database.commit();

    expectedHookState += RECORD_BEFORE_CREATE + RECORD_AFTER_CREATE;
    Assert.assertEquals(callbackCount, expectedHookState);
  }

  @Test(dependsOnMethods = "testHookCallsCreate")
  public void testHookCallsRead() {
    // TEST HOOKS ON READ
    database.begin();
    List<Profile> result = database.query(new OSQLSynchQuery<Profile>("select * from Profile where nick = 'HookTxTest'"));
    expectedHookState += RECORD_BEFORE_READ + RECORD_AFTER_READ;

    Assert.assertFalse(result.size() == 0);

    for (int i = 0; i < result.size(); ++i) {
      Assert.assertEquals(callbackCount, expectedHookState);

      p = result.get(i);
    }
    Assert.assertEquals(callbackCount, expectedHookState);
    database.commit();
  }

  @Test(dependsOnMethods = "testHookCallsRead")
  public void testHookCallsUpdate() {
    database.begin();
    // TEST HOOKS ON UPDATE
    p.setValue(p.getValue() + 1000);
    database.save(p);

    database.commit();

    expectedHookState += RECORD_BEFORE_UPDATE + RECORD_AFTER_UPDATE;
    Assert.assertEquals(callbackCount, expectedHookState);
  }

  @Test(dependsOnMethods = "testHookCallsUpdate")
  public void testHookCallsDelete() throws IOException {
    // TEST HOOKS ON DELETE
    database.begin();
    database.delete(p);
    database.commit();

    expectedHookState += RECORD_BEFORE_DELETE + RECORD_AFTER_DELETE;
    Assert.assertEquals(callbackCount, expectedHookState);

    database.unregisterHook(this);
  }

  @Test(dependsOnMethods = "testHookCallsDelete")
  public void testHookCannotBeginTx() throws IOException {
    final AtomicBoolean exc = new AtomicBoolean(false);
    database.activateOnCurrentThread();
    database.registerHook(new ORecordHookAbstract() {
      @Override
      public RESULT onRecordBeforeCreate(ORecord iRecord) {
        try {
          database.activateOnCurrentThread();
          database.begin();
        } catch (IllegalStateException e) {
          exc.set(true);
        }
        return null;
      }

      @Override
      public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return DISTRIBUTED_EXECUTION_MODE.BOTH;
      }
    });

    Assert.assertFalse(exc.get());
    new ODocument().field("test-hook", true).save();
    Assert.assertTrue(exc.get());

    database.activateOnCurrentThread();
    database.close();
  }

  @Override
  @Test(enabled = false)
  public RESULT onRecordBeforeCreate(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_BEFORE_CREATE;
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  @Test(enabled = false)
  public void onRecordAfterCreate(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_AFTER_CREATE;
  }

  @Override
  @Test(enabled = false)
  public RESULT onRecordBeforeRead(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_BEFORE_READ;
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  @Test(enabled = false)
  public void onRecordAfterRead(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_AFTER_READ;
  }

  @Override
  @Test(enabled = false)
  public RESULT onRecordBeforeUpdate(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_BEFORE_UPDATE;
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  @Test(enabled = false)
  public void onRecordAfterUpdate(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_AFTER_UPDATE;
  }

  @Override
  @Test(enabled = false)
  public RESULT onRecordBeforeDelete(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_BEFORE_DELETE;
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  @Test(enabled = false)
  public void onRecordAfterDelete(ORecord iRecord) {
    if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
        && ((ODocument) iRecord).getClassName().equals("Profile"))
      callbackCount += RECORD_AFTER_DELETE;
  }
}
