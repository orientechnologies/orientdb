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

import java.util.List;

import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Represent one or more object fields as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OSQLFilterItemFieldMultiAbstract extends OSQLFilterItemAbstract {
	private List<String>	names;

	public OSQLFilterItemFieldMultiAbstract(final OSQLFilter iQueryCompiled, final String iName, final List<String> iNames) {
		super(iQueryCompiled, iName);
		names = iNames;
	}

	public Object getValue(final ORecordInternal<?> iRecord) {
		if (names.size() == 1)
			return transformValue(iRecord.getDatabase(), ((ODocument) iRecord).rawField(names.get(0)));

		Object[] values = ((ODocument) iRecord).fieldValues();

		if (hasChainOperators()) {
			// TRANSFORM ALL THE VALUES
			for (int i = 0; i < values.length; ++i)
				values[i] = transformValue(iRecord.getDatabase(), values[i]);
		}

		return new OQueryRuntimeValueMulti(this, values);
	}
}
