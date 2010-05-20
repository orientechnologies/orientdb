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
import java.util.List;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.test.database.base.OrientTest;

@Test(enabled = false)
public class SQLAsynchQuerySpeedTest extends SpeedTestMonoThread implements OCommandResultListener {
	protected int								resultCount	= 0;
	private ODatabaseDocumentTx	database;

	public SQLAsynchQuerySpeedTest(String iURL) {
		super(1);
		database = new ODatabaseDocumentTx(iURL);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void cycle() throws UnsupportedEncodingException {
		System.out.println("1 -----------------------");
		OrientTest.printRecords((List<? extends ORecord<?>>) database.command(
				new OSQLAsynchQuery<ODocument>("select * from animal where column(0) < 5 or column(0) >= 3 and column(5) < 7", this))
				.execute());
	}

	public boolean result(final Object iRecord) {
		OrientTest.printRecord(resultCount++, iRecord);
		return true;
	}
}
