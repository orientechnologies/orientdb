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
package com.orientechnologies.orient.core.sql.operator;

import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * CONTAINS KEY operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorContainsText extends OQueryOperatorEqualityNotNulls {

	private boolean	ignoreCase	= true;

	public OQueryOperatorContainsText(final boolean iIgnoreCase) {
		super("CONTAINSTEXT", 5, false);
		ignoreCase = iIgnoreCase;
	}

	public OQueryOperatorContainsText() {
		super("CONTAINSTEXT", 5, false);
	}

	@Override
	public String getSyntax() {
		return "<left> CONTAINSTEXT[( noignorecase ] )] <right>";
	}

	@Override
	protected boolean evaluateExpression(final ODatabaseRecord<?> iDatabase, final OSQLFilterCondition iCondition,
			final Object iLeft, final Object iRight) {

		final OProperty prop = null;// iDatabase.getMetadata().getSchema().getClass(iCondition.get)

		return false;
	}

	@Override
	public OQueryOperator configure(final List<String> iParams) {
		if (iParams == null)
			return this;

		for (String p : iParams) {
			if (p.equals("noignorecase"))
				ignoreCase = false;
		}

		return new OQueryOperatorContainsText(ignoreCase);
	}

	public boolean isIgnoreCase() {
		return ignoreCase;
	}
}
