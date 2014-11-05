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
package com.orientechnologies.orient.test.database.speed;

import java.io.UnsupportedEncodingException;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class DictionaryLookupInverseSpeedTest extends OrientMonoThreadTest {
	private ODatabaseDocumentTx database;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		DictionaryLookupInverseSpeedTest test = new DictionaryLookupInverseSpeedTest();
		test.data.go(test);
	}

	public DictionaryLookupInverseSpeedTest() {
		super(100000);
		Orient.instance().getProfiler().startRecording();
		database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");
	}

	@Override
	public void cycle() throws UnsupportedEncodingException {
		ORecordFlat value = database.getDictionary().get("doc-" + (data.getCycles() - data.getCyclesDone()-1));
		Assert.assertNotNull(value);
		// Assert.assertTrue(value.value().contains(String.valueOf(data.getCyclesDone())));
		// OrientTest.printRecord((int) data.getCyclesDone(), value);
	}
}
