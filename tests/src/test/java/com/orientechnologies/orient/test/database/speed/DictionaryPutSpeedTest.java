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
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class DictionaryPutSpeedTest extends OrientMonoThreadTest {
	private ODatabaseFlat	database;
	private ORecordFlat		record;
	private long					startNum;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
//		System.setProperty("url", "remote:localhost:2424/demo");
		DictionaryPutSpeedTest test = new DictionaryPutSpeedTest();
		test.data.go(test);
	}

	public DictionaryPutSpeedTest() throws InstantiationException, IllegalAccessException {
		super(1000000);

		String url = System.getProperty("url");
		database = new ODatabaseFlat(url).open("admin", "admin");
		database.declareIntent(new OIntentMassiveInsert());

		record = database.newInstance();
		startNum = 0;// database.countClusterElements("Animal");

		OProfiler.getInstance().startRecording();

		System.out.println("Total element in the dictionary: " + startNum);

		database.begin(TXTYPE.NOTX);
	}

	@Override
	public void cycle() {
		int id = (int) (startNum + data.getCyclesDone());

		record.reset();
		record = record.value("{ 'id' : " + id
				+ " , 'name' : 'Gipsy' , 'type' : 'Cat' , 'race' : 'European' , 'country' : 'Italy' , 'price' :"
				+ (data.getCyclesDone() + 300) + ".00");
		record.save("Animal");

		database.getDictionary().put("doc-" + id, record);
	}

	@Override
	public void deinit() {
		System.out.println("Total element in the dictionary: " + database.getDictionary().size());

		database.commit();

		database.close();
		super.deinit();
	}

}
