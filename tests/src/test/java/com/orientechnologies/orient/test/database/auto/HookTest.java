/*
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

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Profile;
import java.io.IOException;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "hook")
public class HookTest extends ObjectDBBaseTest {
  private int callbackCount = 0;
  private Profile p;
  private Hook hook = new Hook();

  @Parameters(value = "url")
  public HookTest(@Optional String url) {
    super(url);
  }

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {}

  @Test
  public void testRegisterHook() throws IOException {
    database.registerHook(hook);
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.base");
  }

  @Test(dependsOnMethods = "testRegisterHook")
  public void testHooksIsRegistered() throws IOException {
    for (ORecordHook hook : database.getHooks().keySet()) {
      if (hook.equals(this.hook)) return;
    }

    Assert.assertTrue(false);
  }

  @Test(dependsOnMethods = "testHooksIsRegistered")
  public void testHookCreate() throws IOException {
    p = new Profile("HookTest");

    // TEST HOOKS ON CREATE
    Assert.assertEquals(callbackCount, 0);
    database.save(p);
    Assert.assertEquals(callbackCount, 11);
  }

  @Test(dependsOnMethods = "testHookCreate")
  public void testHookRead() {
    // TEST HOOKS ON READ
    List<Profile> result =
        database.query(
            new OSQLSynchQuery<Profile>("select * from Profile where nick = 'HookTest'"));
    Assert.assertEquals(result.size(), 1);

    for (int i = 0; i < result.size(); ++i) {
      Assert.assertEquals(callbackCount, 46);

      p = result.get(i);
    }
    Assert.assertEquals(callbackCount, 46);
  }

  @Test(dependsOnMethods = "testHookRead")
  public void testHookUpdate() {
    // TEST HOOKS ON UPDATE
    p.setValue(p.getValue() + 1000);
    database.save(p);
    Assert.assertEquals(callbackCount, 136);
  }

  @Test(dependsOnMethods = "testHookUpdate")
  public void testHookDelete() {
    // TEST HOOKS ON DELETE
    database.delete(p);
    Assert.assertEquals(callbackCount, 266);
  }

  @Test(dependsOnMethods = "testHookDelete")
  public void testUnregisterHook() throws IOException {
    database.unregisterHook(hook);
    database.close();
  }

  public class Hook extends ORecordHookAbstract {
    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
    }

    @Override
    public RESULT onRecordBeforeCreate(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 1;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterCreate(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 10;
    }

    @Override
    public RESULT onRecordBeforeRead(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 20;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterRead(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 15;
    }

    @Override
    public RESULT onRecordBeforeUpdate(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 40;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterUpdate(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 50;
    }

    @Override
    public RESULT onRecordBeforeDelete(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 60;
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public void onRecordAfterDelete(ORecord iRecord) {
      if (iRecord instanceof ODocument
          && ((ODocument) iRecord).getClassName() != null
          && ((ODocument) iRecord).getClassName().equals("Profile")) callbackCount += 70;
    }
  }
}
