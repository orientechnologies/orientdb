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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
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
	public OSQLFilterItemField(final OCommandToParse iQueryToParse, final String iName) {
		super(iQueryToParse, iName);
	}

	public Object getValue(final ORecordInternal<?> iRecord) {
		if (iRecord == null)
			throw new OCommandExecutionException("expression item '" + name + "' can't be resolved");

		final ODatabaseRecord db = iRecord.getDatabase();

		if (name.charAt(0) == '@')
			return transformValue(db, getRecordAttribute(db, iRecord, name));
		else
			return transformValue(db, ((ODocument) iRecord).rawField(name));
	}
}
