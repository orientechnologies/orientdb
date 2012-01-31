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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;

/**
 * Represents a context variable as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterItemVariable extends OSQLFilterItemAbstract {
	protected String	name;

	public OSQLFilterItemVariable(final OCommandToParse iQueryToParse, final String iName) {
		super(iQueryToParse, iName.substring(1));
	}

	public Object getValue(final OIdentifiable iRecord, OCommandContext iContext) {
		if (iRecord == null)
			throw new OCommandExecutionException("Context variable '$" + name + "' cannot be resolved because record is null");

		if (iContext == null)
			throw new OCommandExecutionException("Context variable '$" + name + "' cannot be resolved because context is null");

		return transformValue(iRecord, iContext.getVariable(name));
	}

	public String getRoot() {
		return name;
	}

	public void setRoot(final OCommandToParse iQueryToParse, final String iRoot) {
		this.name = iRoot;
	}

	@Override
	public String toString() {
		return "$" + super.toString();
	}
}
