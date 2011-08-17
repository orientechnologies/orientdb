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

import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Represent an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterItemField extends OSQLFilterItemAbstract {
	protected String	name;

	public OSQLFilterItemField(final OCommandToParse iQueryToParse, final String iName) {
		super(iQueryToParse, iName);
	}

	public Object getValue(final ORecordInternal<?> iRecord) {
		if (iRecord == null)
			throw new OCommandExecutionException("expression item '" + name + "' can't be resolved");

		return transformValue(iRecord.getDatabase(), ((ODocument) iRecord).rawField(name));
	}

	public String getRoot() {
		return name;
	}

	public void setRoot(final OCommandToParse iQueryToParse, final String iRoot) {
		this.name = iRoot;
	}
}
