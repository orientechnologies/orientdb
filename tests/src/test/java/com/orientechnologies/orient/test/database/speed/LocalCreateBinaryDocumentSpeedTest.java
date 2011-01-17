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
package com.orientechnologies.orient.test.database.speed;

import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class LocalCreateBinaryDocumentSpeedTest extends OrientMonoThreadTest {
	private static final int	PAYLOAD_SIZE	= 2000;
	private ODatabaseDocument	database;
	private ORecordBytes			record;
	private byte[]						payload;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		LocalCreateBinaryDocumentSpeedTest test = new LocalCreateBinaryDocumentSpeedTest();
		test.data.go(test);
	}

	public LocalCreateBinaryDocumentSpeedTest() throws InstantiationException, IllegalAccessException {
		super(50000);
		payload = new byte[PAYLOAD_SIZE];
		for (int i = 0; i < PAYLOAD_SIZE; ++i) {
			payload[i] = (byte) i;
		}
	}

	@Override
	public void init() {
		OProfiler.getInstance().startRecording();

		database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");

		database.declareIntent(new OIntentMassiveInsert());
		database.begin(TXTYPE.NOTX);
	}

	@Override
	public void cycle() {
		record = new ORecordBytes(database, payload);
		record.save();

		if (data.getCyclesDone() == data.getCycles() - 1)
			database.commit();
	}

	@Override
	public void deinit() {
		if (database != null)
			database.close();
		super.deinit();
	}
}
