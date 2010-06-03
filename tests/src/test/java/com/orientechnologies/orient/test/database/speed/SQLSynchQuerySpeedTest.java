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

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.database.base.OrientTest;

public class SQLSynchQuerySpeedTest extends SpeedTestMonoThread implements OCommandResultListener {
	protected int								resultCount	= 0;
	private ODatabaseDocumentTx	database;

	public SQLSynchQuerySpeedTest(String iURL) {
		super(1);
		Orient.instance().registerEngine(new OEngineRemote());
		database = new ODatabaseDocumentTx(iURL);
	}

	@Override
	public void cycle() throws UnsupportedEncodingException {
		System.out.println("1 ----------------------");
		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select * from Account where id = 10 and name like 'G%'")).execute();
	}

	public boolean result(final Object iRecord) {
		OrientTest.printRecord(resultCount++, iRecord);
		return true;
	}
}
