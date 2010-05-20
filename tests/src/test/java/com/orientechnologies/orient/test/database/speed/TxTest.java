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

import java.io.UnsupportedEncodingException;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class TxTest extends OrientMonoThreadTest {
	private ODatabaseFlat	database;
	private ORecordFlat					record;

	public TxTest() throws InstantiationException, IllegalAccessException {
		super(10);
		database = new ODatabaseFlat(System.getProperty("url")).open("admin", "admin");
		record = database.newInstance();

		database.begin(TXTYPE.OPTIMISTIC);
	}

	@Override
	public void cycle() throws UnsupportedEncodingException {
		record.value(data.getCyclesDone() + ",'Gipsy','Cat','European','Italy'," + (data.getCyclesDone() + 300) + ".00").save();

		if (data.getCyclesDone() == data.getCycles() - 1)
			database.commit();
	}
}
