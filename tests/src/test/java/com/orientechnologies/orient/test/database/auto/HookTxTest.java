/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = "hook")
public class HookTxTest extends ORecordHookAbstract {
	private ODatabaseObjectTx	database;
	private int								callbackCount	= 0;
	private Profile						p;

	@Parameters(value = "url")
	public HookTxTest(String iURL) {
		database = new ODatabaseObjectTx(iURL);
	}

	@Test
	public void testRegisterHook() throws IOException {
		database.open("writer", "writer");
		database.registerHook(this);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
	}

	@Test(dependsOnMethods = "testRegisterHook")
	public void testHookCalls() throws IOException {

		p = new Profile("HookTxTest");

		// TEST HOOKS ON CREATE
		Assert.assertEquals(callbackCount, 0);
		database.begin();
		database.save(p);
		database.commit();

		Assert.assertEquals(callbackCount, 11);

		// TEST HOOKS ON READ
		database.begin();
		List<Profile> result = database.query(new OSQLSynchQuery<Profile>("select * from Profile where nick = 'HookTxTest'"));
		Assert.assertFalse(result.size() == 0);

		for (int i = 0; i < result.size(); ++i) {
			Assert.assertEquals(callbackCount, 46);

			p = result.get(i);
		}
		Assert.assertEquals(callbackCount, 46);
		database.commit();

		database.begin();
		// TEST HOOKS ON UPDATE
		p.setValue(p.getValue() + 1000);
		database.save(p);

		database.commit();
		Assert.assertEquals(callbackCount, 136);

		// TEST HOOKS ON DELETE
		database.begin();
		database.delete(p);
		database.commit();
		Assert.assertEquals(callbackCount, 266);

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
	public boolean onRecordAfterCreate(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 10;
		return false;
	}

	@Override
	@Test(enabled = false)
	public boolean onRecordBeforeRead(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 20;
		return false;
	}

	@Override
	@Test(enabled = false)
	public boolean onRecordAfterRead(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 15;
		return false;
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
	public boolean onRecordAfterUpdate(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 50;
		return false;
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
	public boolean onRecordAfterDelete(ORecord<?> iRecord) {
		if (iRecord instanceof ODocument && ((ODocument) iRecord).getClassName() != null
				&& ((ODocument) iRecord).getClassName().equals("Profile"))
			callbackCount += 70;
		return false;
	}
}
