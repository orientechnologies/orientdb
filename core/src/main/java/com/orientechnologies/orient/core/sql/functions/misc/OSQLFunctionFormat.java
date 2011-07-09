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

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Formats content.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionFormat extends OSQLFunctionAbstract {
	public static final String	NAME	= "format";

	public OSQLFunctionFormat() {
		super(NAME, 2, -1);
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters) {
		final Object[] args = new Object[iParameters.length - 1];

		for (int i = 0; i < args.length; ++i)
			args[i] = iParameters[i + 1];

		return String.format((String) iParameters[0], args);
	}

	public String getSyntax() {
		return "Syntax error: format(<format>, <arg1> [,<argN>]*)";
	}
}
