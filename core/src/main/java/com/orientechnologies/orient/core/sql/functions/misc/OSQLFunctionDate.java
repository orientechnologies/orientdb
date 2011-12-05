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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Builds a date object from the format passed. If no arguments are passed, than the system date is built (like sysdate() function)
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see OSQLFunctionSysdate
 * 
 */
public class OSQLFunctionDate extends OSQLFunctionAbstract {
	public static final String	NAME	= "date";

	private Date								date;
	private SimpleDateFormat		format;

	/**
	 * Get the date at construction to have the same date for all the iteration.
	 */
	public OSQLFunctionDate() {
		super(NAME, 0, 2);
		date = new Date();
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
		if (iParameters.length == 0)
			return date;

		if (iParameters.length != 2)
			throw new OCommandSQLParsingException(getSyntax());

		if (format == null)
			format = new SimpleDateFormat((String) iParameters[1]);

		synchronized (format) {
			try {
				return format.parse((String) iParameters[0]);
			} catch (ParseException e) {
				throw new OQueryParsingException("Error on formatting date '" + iParameters[0] + "' using the format: " + iParameters[1], e);
			}
		}
	}

	public boolean aggregateResults(final Object[] configuredParameters) {
		return false;
	}

	public String getSyntax() {
		return "Syntax error: date([<date-as-string>, <format>])";
	}

	@Override
	public Object getResult() {
		format = null;
		return null;
	}
}
