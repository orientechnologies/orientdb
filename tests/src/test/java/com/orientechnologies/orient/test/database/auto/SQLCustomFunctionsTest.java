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
package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLParser;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class SQLCustomFunctionsTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLCustomFunctionsTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryCustomFunction() {
		database.open("admin", "admin");

		OSQLParser.getInstance().registerFunction("max", new OSQLFunctionAbstract("max", 2, 2) {
			public String getSyntax() {
				return "max(<first>, <second>)";
			}

			public Object execute(ORecordInternal<?> iRecord, Object[] iParameters) {
				if (iParameters[0] == null || iParameters[1] == null)
					// CHECK BOTH EXPECTED PARAMETERS
					return null;

				// USE DOUBLE TO AVOID LOSS OF PRECISION
				final double v1 = Double.parseDouble(iParameters[0].toString());
				final double v2 = Double.parseDouble(iParameters[1].toString());

				return Math.max(v1, v2);
			}
		});

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Account where max(nr,1000) > 0"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		OSQLParser.getInstance().unregisterFunction("max");
		database.close();
	}
}
