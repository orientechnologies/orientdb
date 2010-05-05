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

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordPositional;

/**
 * Represent a column (as positional order) as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterItemColumn extends OSQLFilterItemAbstract {
	protected int	number	= -1;

	public OSQLFilterItemColumn(final OSQLFilter iQueryCompiled, final String iName) {
		super(iQueryCompiled, iName);
		number = Integer.parseInt(iName);
	}

	public Object getValue(final ORecordInternal<?> iRecord) {
		if (iRecord instanceof ORecordPositional<?>)
			return transformValue(((ORecordPositional<?>) iRecord).field(number));
		return null;
	}

	@Override
	public String toString() {
		return "column(" + number + ")";
	}
}
