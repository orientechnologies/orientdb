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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.query.nativ.ONativeSynchQuery;
import com.orientechnologies.orient.core.query.nativ.OQueryContextNativeSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.database.base.OrientTest;

public class NativeSynchQuerySpeedTest extends SpeedTestMonoThread {
	private ODatabaseDocumentTx	database;
	private List<ODocument>			result;

	public NativeSynchQuerySpeedTest() {
		super(1);
	}

	@Override
	public void cycle() throws UnsupportedEncodingException {
		new ONativeSynchQuery<ODocument, OQueryContextNativeSchema<ODocument>>(database, "Animal",
				new OQueryContextNativeSchema<ODocument>()) {

			@Override
			public boolean filter(OQueryContextNativeSchema<ODocument> iRecord) {
				return iRecord.column("race").like("Euro%").and().column("race").like("%an").and().column("id").toInt().eq(10).go();
			}

		}.execute();
	}

	@Override
	public void deinit() throws IOException {
		if (result == null)
			System.out.println("Error on execution");
		else {
			System.out.println("Record found: " + result.size());
			OrientTest.printRecords(result);
		}
	}
}
