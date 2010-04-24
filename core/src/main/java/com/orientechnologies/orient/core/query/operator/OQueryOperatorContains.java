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
package com.orientechnologies.orient.core.query.operator;

import java.util.Collection;

import com.orientechnologies.orient.core.exception.OQueryExecutionException;
import com.orientechnologies.orient.core.query.sql.OSQLDefinitionCondition;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * CONTAINS operator.
 * 
 * @author luca
 * 
 */
public class OQueryOperatorContains extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorContains() {
		super("CONTAINS", 5, false);
	}

	@SuppressWarnings("unchecked")
	protected boolean evaluateExpression(final OSQLDefinitionCondition iCondition, final Object iLeft, final Object iRight) {
		OSQLDefinitionCondition condition;
		
		try {
			condition = (OSQLDefinitionCondition) (iCondition.getLeft() instanceof OSQLDefinitionCondition ? iCondition.getLeft() : iCondition.getRight());
		} catch (Exception e) {
			throw new OQueryExecutionException("Operator contains needs a condition to apply", e);
		}

		if (iLeft instanceof Collection<?>) {

			Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) iLeft;
			for (ORecordSchemaAware<?> o : collection) {
				if ((Boolean) condition.evaluate(o) == Boolean.TRUE)
					return true;
			}
		} else if (iRight instanceof Collection<?>) {

			Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) iRight;
			for (ORecordSchemaAware<?> o : collection) {
				if ((Boolean) condition.evaluate(o) == Boolean.TRUE)
					return true;
			}
		}
		return false;
	}
}
