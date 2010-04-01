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
import com.orientechnologies.orient.core.db.vobject.ODatabaseVObjectTx;
import com.orientechnologies.orient.core.query.nativ.ONativeSynchQuery;
import com.orientechnologies.orient.core.query.nativ.OQueryContextNativeSchema;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;
import com.orientechnologies.orient.test.database.base.OrientTest;

public class NativeSynchQuerySpeedTest extends SpeedTestMonoThread {
	private ODatabaseVObjectTx		database;
	private List<ORecordVObject>	result;

	public NativeSynchQuerySpeedTest() {
		super(1);
	}

	public void cycle() throws UnsupportedEncodingException {
		result = database.query(
				new ONativeSynchQuery<ORecordVObject, OQueryContextNativeSchema<ORecordVObject>>("Animal",
						new OQueryContextNativeSchema<ORecordVObject>()) {

					@Override
					public boolean filter(OQueryContextNativeSchema<ORecordVObject> iRecord) {
						return iRecord.column("race").like("Euro%").and().column("race").like("%an").and().column("id").toInt().eq(10).go();
					}

				}).execute();
	}

	public void deinit() throws IOException {
		if (result == null)
			System.out.println("Error on execution");
		else {
			System.out.println("Record found: " + result.size());
			OrientTest.printRecords(result);
		}
	}
}
