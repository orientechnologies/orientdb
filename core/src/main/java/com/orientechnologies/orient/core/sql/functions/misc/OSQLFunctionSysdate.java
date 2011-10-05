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
package com.orientechnologies.orient.core.sql.functions.misc;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Returns the current date time. If no formatting is passed, then long format is used.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionSysdate extends OSQLFunctionAbstract {
	public static final String	NAME	= "sysdate";

	private Date								now;
	private SimpleDateFormat		format;

	/**
	 * Get the date at construction to have the same date for all the iteration.
	 */
	public OSQLFunctionSysdate() {
		super(NAME, 0, 1);
		now = new Date();
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
		if (iParameters.length == 0)
			return now;

		if (format == null)
			format = new SimpleDateFormat((String) iParameters[0]);

		synchronized (format) {
			return format.format(now);
		}
	}

	public boolean aggregateResults(final Object[] configuredParameters) {
		return false;
	}

	public String getSyntax() {
		return "Syntax error: sysdate([<format>])";
	}

	@Override
	public Object getResult() {
		format = null;
		return null;
	}
}
