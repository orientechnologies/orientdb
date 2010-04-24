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

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * Represent one or more object fields as value in the query condition.
 * 
 * @author luca
 * 
 */
public abstract class OSQLDefinitionItemFieldMultiAbstract extends OSQLDefinitionItemAbstract {
	private String[]	names;

	public OSQLDefinitionItemFieldMultiAbstract(final OSQLDefinition iQueryCompiled, final String iName, final String[] iNames) {
		super(iQueryCompiled, iName);
		names = iNames;
	}

	public Object getValue(final ORecordInternal<?> iRecord) {
		return new OSQLRuntimeValueMulti(this, ((ORecordSchemaAware<?>) iRecord).fieldValues());
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append(name);
		buffer.append("(");
		if (names != null) {
			int i = 0;
			for (String n : names) {
				if (i++ > 0)
					buffer.append(", ");
				buffer.append(n);
			}
		}
		buffer.append(")");

		return buffer.toString();
	}
}
