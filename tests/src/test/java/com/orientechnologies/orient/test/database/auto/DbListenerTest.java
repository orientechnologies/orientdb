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

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.ORemoteServerEventListener;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

/**
 * Tests the right calls of all the db's listener API.
 * 
 * @author Sylvain Spinelli
 * 
 */
public class DbListenerTest {
	protected ODatabaseDocumentTx	database;

	protected String							dbUrl;

	protected int									onAfterTxCommit								= 0;
	protected int									onAfterTxRollback							= 0;
	protected int									onBeforeTxBegin								= 0;
	protected int									onBeforeTxCommit							= 0;
	protected int									onBeforeTxRollback						= 0;
	protected int									onClose												= 0;
	protected int									onCreate											= 0;
	protected int									onDelete											= 0;
	protected int									onOpen												= 0;

	protected int									onRecordPulled								= 0;
	protected int									onClusterConfigurationChange	= 0;

	public class DbListener implements ODatabaseListener {
		public void onAfterTxCommit(ODatabase iDatabase) {
			onAfterTxCommit++;
		}

		public void onAfterTxRollback(ODatabase iDatabase) {
			onAfterTxRollback++;
		}

		public void onBeforeTxBegin(ODatabase iDatabase) {
			onBeforeTxBegin++;
		}

		public void onBeforeTxCommit(ODatabase iDatabase) {
			onBeforeTxCommit++;
		}

		public void onBeforeTxRollback(ODatabase iDatabase) {
			onBeforeTxRollback++;
		}

		public void onClose(ODatabase iDatabase) {
			onClose++;
		}

		public void onCreate(ODatabase iDatabase) {
			onCreate++;
		}

		public void onDelete(ODatabase iDatabase) {
			onDelete++;
		}

		public void onOpen(ODatabase iDatabase) {
			onOpen++;
		}
	}

	@Parameters(value = "url")
	public DbListenerTest(String iURL) {
		dbUrl = iURL;
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void testEmbeddedDbListeners() throws IOException {
		if (database.getURL().startsWith("remote:"))
			return;

		if (database.exists())
			TestUtils.deleteDatabase(database);

		database.registerListener(new DbListener());

		TestUtils.createDatabase(database, dbUrl);

		Assert.assertEquals(onCreate, 1);

		database.close();
		Assert.assertEquals(onClose, 1);

		database.registerListener(new DbListener());

		database.open("admin", "admin");
		Assert.assertEquals(onOpen, 1);

		database.begin(TXTYPE.OPTIMISTIC);
		Assert.assertEquals(onBeforeTxBegin, 1);

		database.newInstance().save();
		database.commit();
		Assert.assertEquals(onBeforeTxCommit, 1);
		Assert.assertEquals(onAfterTxCommit, 1);

		database.begin(TXTYPE.OPTIMISTIC);
		Assert.assertEquals(onBeforeTxBegin, 2);

		database.newInstance().save();
		database.rollback();
		Assert.assertEquals(onBeforeTxRollback, 1);
		Assert.assertEquals(onAfterTxRollback, 1);

		TestUtils.deleteDatabase(database);
		Assert.assertEquals(onClose, 2);
		Assert.assertEquals(onDelete, 1);
	}

	@Test
	public void testRemoteDbListeners() throws IOException {
		if (!database.getURL().startsWith("remote:"))
			return;

		database.registerListener(new DbListener());

		database.open("admin", "admin");
		Assert.assertEquals(onOpen, 1);

		database.begin(TXTYPE.OPTIMISTIC);
		Assert.assertEquals(onBeforeTxBegin, 1);

		database.newInstance().save();
		database.commit();
		Assert.assertEquals(onBeforeTxCommit, 1);
		Assert.assertEquals(onAfterTxCommit, 1);

		database.begin(TXTYPE.OPTIMISTIC);
		Assert.assertEquals(onBeforeTxBegin, 2);

		database.newInstance().save();
		database.rollback();
		Assert.assertEquals(onBeforeTxRollback, 1);
		Assert.assertEquals(onAfterTxRollback, 1);

		database.close();
		Assert.assertEquals(onClose, 1);
	}

	@Test
	public void testAsynchEventListeners() throws IOException {
		if (!database.getURL().startsWith("remote:"))
			return;

		database.open("admin", "admin");

		((OStorageRemoteThread) database.getStorage()).addRemoteServerEventListener(new ORemoteServerEventListener() {

			public void onRecordPulled(ORecord<?> iRecord) {
				onRecordPulled++;
			}

			public void onClusterConfigurationChange(byte[] clusterConfig) {
				onClusterConfigurationChange++;
			}
		});
		
		database.close();
	}
}
