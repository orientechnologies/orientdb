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
package com.orientechnologies.orient.core.query.sql;

import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordPositional;

public class OQueryItemColumn implements OQueryItemValue {
	protected String	name;
	protected int			number	= -1;

	public OQueryItemColumn(int iNumber) {
		this.number = iNumber;
	}

	public OQueryItemColumn(OSQLQueryCompiled iQueryCompiled, String iName) {
		name = iName;

		for (String className : iQueryCompiled.getClusters().keySet()) {
			OClass cls = iQueryCompiled.query.getDatabase().getMetadata().getSchema().getClass(className);
			if (cls == null)
				throw new OQueryParsingException("Class '" + cls + "' is not defined in current database", iQueryCompiled.query.text(),
						iQueryCompiled.currentPos);

			number = cls.getPropertyIndex(iName);
			if (number > -1)
				return;
		}

		// TODO: USE RUN-TIME PROPERTY GATHERING FOR SCHEMA-LESS OBJECTS
		// if (number == -1)
		// throw new OQueryParsingException("Property '" + iName + "' is not defined in the classes part of query ("
		// + iQueryCompiled.getClusters().keySet() + ")", iQueryCompiled.query.text(), iQueryCompiled.currentPos);
	}

	public Object getValue(ORecordInternal<?> iRecord) {
		return ((ORecordPositional<?>) iRecord).field(number);
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		if (name != null)
			buffer.append(name);

		if (number > -1) {
			if (buffer.length() > 0)
				buffer.append(' ');

			buffer.append("column(");
			buffer.append(number);
			buffer.append(")");
		}

		return buffer.toString();
	}
}
