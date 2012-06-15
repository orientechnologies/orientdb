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
package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = "hook")
public class HookTest extends ORecordHookAbstract {
	private OObjectDatabaseTx	database;
	private int								callbackCount	= 0;
	private Profile						p;

	@Parameters(value = "url")
	public HookTest(String iURL) {
		database = new OObjectDatabaseTx(iURL);
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
	public void testHooksIsRegistered() throws IOException {
		for (ORecordHook hook : database.getHooks()) {
			if (hook.equals(this))
				return;
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
		List<Profile> result = database.query(new OSQLSynchQuery<Profile>("select * from Profile where nick = 'HookTest'"));
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
		database.unregisterHook(this);
		database.close();
	}

	@Override
	@Test(enabled = false)
	public boolean onRecordBeforeCreate(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 1;
		return false;
	}

	@Override
	@Test(enabled = false)
	public void onRecordAfterCreate(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 10;
	}

	@Override
	@Test(enabled = false)
	public void onRecordBeforeRead(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 20;
	}

	@Override
	@Test(enabled = false)
	public void onRecordAfterRead(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 15;
	}

	@Override
	@Test(enabled = false)
	public boolean onRecordBeforeUpdate(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 40;
		return false;
	}

	@Override
	@Test(enabled = false)
	public void onRecordAfterUpdate(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 50;
	}

	@Override
	@Test(enabled = false)
	public boolean onRecordBeforeDelete(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 60;
		return false;
	}

	@Override
	@Test(enabled = false)
	public void onRecordAfterDelete(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 70;
	}
}
