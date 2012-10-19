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
package com.orientechnologies.orient.core.sql;

import java.util.Collection;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Explains the execution of a command returning profiling information.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLExplain extends OCommandExecutorSQLDelegate {
	public static final String	KEYWORD_EXPLAIN	= "EXPLAIN";

	@SuppressWarnings("unchecked")
	@Override
	public OCommandExecutorSQLExplain parse(OCommandRequest iCommand) {
		String cmd = ((OCommandSQL) iCommand).getText();
		super.parse(new OCommandSQL(cmd.substring(KEYWORD_EXPLAIN.length())));
		return this;
	}

	@Override
	public Object execute(Map<Object, Object> iArgs) {
		delegate.getContext().setRecordingMetrics(true);

		final long startTime = System.nanoTime();

		final Object result = super.execute(iArgs);
		final ODocument report = new ODocument(delegate.getContext().getVariables());

		report.field("elapsed", (System.nanoTime() - startTime) / 1000000000f);

		if (result instanceof Collection<?>) {
			report.field("resultType", "collection");
			report.field("resultSize", ((Collection<?>) result).size());
		} else if (result instanceof ODocument) {
			report.field("resultType", "document");
			report.field("resultSize", 1);
		} else if (result instanceof Number) {
			report.field("resultType", "number");
		}

		return report;
	}
}
