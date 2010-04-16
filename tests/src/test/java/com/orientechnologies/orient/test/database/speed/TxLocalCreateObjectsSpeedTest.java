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

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ODatabaseColumn;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;
import com.orientechnologies.orient.test.database.base.OrientTxSpeedTest;

public class TxLocalCreateObjectsSpeedTest extends OrientTxSpeedTest {
	private ODatabaseColumn	database;
	private ORecordColumn					record;

	public TxLocalCreateObjectsSpeedTest(String iURL) {
		super(1000000);
		Orient.instance().registerEngine(new OEngineRemote());
	}

	public void init() throws IOException {
		database.begin();
	}

	public void cycle() throws UnsupportedEncodingException {
		record.reset();
		record.add(String.valueOf(data.getCyclesDone()));
		record.add("Gipsy");
		record.add("Cat");
		record.add("European");
		record.add("Italy");
		record.add((data.getCyclesDone() + 300) + ".00");

		record.save("csv");

		super.cycle(database);
	}
}
