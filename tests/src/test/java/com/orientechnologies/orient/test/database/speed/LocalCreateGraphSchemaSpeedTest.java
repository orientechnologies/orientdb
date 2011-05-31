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

import java.util.Date;

import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class LocalCreateGraphSchemaSpeedTest extends OrientMonoThreadTest {
	private OGraphDatabase	database;
	private ODocument				record;
	private Date						date	= new Date();

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		LocalCreateGraphSchemaSpeedTest test = new LocalCreateGraphSchemaSpeedTest();
		test.data.go(test);
	}

	public LocalCreateGraphSchemaSpeedTest() throws InstantiationException, IllegalAccessException {
		super(5000000);
	}

	@Override
	public void init() {
		OProfiler.getInstance().startRecording();

		database = new OGraphDatabase(System.getProperty("url")).open("admin", "admin");

		//database = new OGraphDatabase(System.getProperty("url")).create();

		record = database.newInstance();

		database.declareIntent(new OIntentMassiveInsert());
		OClass cl = database.createVertexType("Person", "OGraphVertex");
		cl.createProperty("id", OType.LONG);
		cl.createProperty("name", OType.STRING);
		cl.createProperty("surname", OType.STRING);
		cl.createProperty("birthDate", OType.DATE);
		cl.createProperty("salary", OType.FLOAT);
	}

	@Override
	public void cycle() {
		record.reset();

		record.setClassName("Person");
		record.field("id", data.getCyclesDone());
		record.field("name", "Luca");
		record.field("surname", "Garulli");
		record.field("birthDate", date);
		record.field("salary", 3000f + data.getCyclesDone());

		record.save();
	}

	@Override
	public void deinit() {

		System.out.println(OProfiler.getInstance().dump());

		if (database != null)
			database.close();
		super.deinit();
	}
}