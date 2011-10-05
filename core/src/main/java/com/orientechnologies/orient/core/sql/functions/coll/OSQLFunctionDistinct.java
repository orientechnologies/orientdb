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
package com.orientechnologies.orient.core.sql.functions.coll;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Keeps items only once removing duplicates
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDistinct extends OSQLFunctionAbstract {
	public static final String	NAME		= "distinct";

	private Set<Object>					context	= new HashSet<Object>();

	public OSQLFunctionDistinct() {
		super(NAME, 1, 1);
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
		final Object value = iParameters[0];

		if (value != null && !context.contains(value)) {
			context.add(value);
			return value;
		}

		return null;
	}

	@Override
	public boolean filterResult() {
		return true;
	}

	public String getSyntax() {
		return "Syntax error: distinct(<field>)";
	}
}
