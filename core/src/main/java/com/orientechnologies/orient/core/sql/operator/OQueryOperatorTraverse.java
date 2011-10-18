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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAny;

/**
 * TRAVERSE operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorTraverse extends OQueryOperatorEqualityNotNulls {
	private int				startDeepLevel	= 0;	// FIRST
	private int				endDeepLevel		= -1; // INFINITE
	private String[]	cfgFields;

	public OQueryOperatorTraverse() {
		super("TRAVERSE", 5, false);
	}

	public OQueryOperatorTraverse(final int startDeepLevel, final int endDeepLevel, final String[] iFieldList) {
		this();
		this.startDeepLevel = startDeepLevel;
		this.endDeepLevel = endDeepLevel;
		this.cfgFields = iFieldList;
	}

	@Override
	public String getSyntax() {
		return "<left> TRAVERSE[(<begin-deep-level> [,<maximum-deep-level> [,<fields>]] )] ( <conditions> )";
	}

	@Override
	protected boolean evaluateExpression(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight) {
		final OSQLFilterCondition condition;
		final Object target;

		if (iCondition.getLeft() instanceof OSQLFilterCondition) {
			condition = (OSQLFilterCondition) iCondition.getLeft();
			target = iRight;
		} else {
			condition = (OSQLFilterCondition) iCondition.getRight();
			target = iLeft;
		}

		final Set<ORID> evaluatedRecords = new HashSet<ORID>();
		return traverse(iRecord, iCondition, condition, target, 0, evaluatedRecords);
	}

	@SuppressWarnings("unchecked")
	private boolean traverse(final ORecordInternal<?> iRecord, final OSQLFilterCondition iRootCondition,
			final OSQLFilterCondition iCondition, Object iTarget, final int iLevel, final Set<ORID> iEvaluatedRecords) {
		if (endDeepLevel > -1 && iLevel > endDeepLevel)
			return false;

		if (iTarget instanceof ORID) {
			if (iEvaluatedRecords.contains(iTarget))
				// ALREADY EVALUATED
				return false;

			// TRANSFORM THE ORID IN ODOCUMENT
			iTarget = new ODocument(iRecord.getDatabase(), (ORID) iTarget);
		} else if (iTarget instanceof ODocument) {
			if (iEvaluatedRecords.contains(((ODocument) iTarget).getIdentity()))
				// ALREADY EVALUATED
				return false;
		}

		if (iTarget instanceof ODocument) {
			final ODocument target = (ODocument) iTarget;

			iEvaluatedRecords.add(target.getIdentity());

			if (target.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
				try {
					target.load();
				} catch (final ORecordNotFoundException e) {
					// INVALID RID
					return false;
				}

			if (iLevel >= startDeepLevel && (Boolean) iCondition.evaluate(target) == Boolean.TRUE)
				return true;

			// TRAVERSE THE DOCUMENT ITSELF
			for (final String cfgField : cfgFields) {
				if (cfgField.equalsIgnoreCase(OSQLFilterItemFieldAny.FULL_NAME)) {
					// ANY
					for (final String fieldName : target.fieldNames())
						if (traverse(iRecord, iRootCondition, iCondition, target.rawField(fieldName), iLevel + 1, iEvaluatedRecords))
							return true;
				} else if (cfgField.equalsIgnoreCase(OSQLFilterItemFieldAny.FULL_NAME)) {
					// ALL
					for (final String fieldName : target.fieldNames())
						if (!traverse(iRecord, iRootCondition, iCondition, target.rawField(fieldName), iLevel + 1, iEvaluatedRecords))
							return false;
					return true;
				} else {
					if (traverse(iRecord, iRootCondition, iCondition, target.rawField(cfgField), iLevel + 1, iEvaluatedRecords))
						return true;
				}
			}

		} else if (iTarget instanceof OQueryRuntimeValueMulti) {

			final OQueryRuntimeValueMulti multi = (OQueryRuntimeValueMulti) iTarget;
			for (final Object o : multi.values) {
				if (traverse(iRecord, iRootCondition, iCondition, o, iLevel + 1, iEvaluatedRecords) == Boolean.TRUE)
					return true;
			}
		} else if (iTarget instanceof Collection<?>) {

			final Collection<Object> collection = (Collection<Object>) iTarget;
			for (final Object o : collection) {
				if (traverse(iRecord, iRootCondition, iCondition, o, iLevel + 1, iEvaluatedRecords) == Boolean.TRUE)
					return true;
			}
		} else if (iTarget instanceof Map<?, ?>) {

			final Map<String, ODocument> map = (Map<String, ODocument>) iTarget;
			for (final ODocument o : map.values()) {
				if (traverse(iRecord, iRootCondition, iCondition, o, iLevel + 1, iEvaluatedRecords) == Boolean.TRUE)
					return true;
			}
		}

		return false;
	}

	@Override
	public OQueryOperator configure(final List<String> iParams) {
		if (iParams == null)
			return this;

		final int start = !iParams.isEmpty() ? Integer.parseInt(iParams.get(0)) : startDeepLevel;
		final int end = iParams.size() > 1 ? Integer.parseInt(iParams.get(1)) : endDeepLevel;

		String[] fields = new String[] { "any()" };
		if (iParams.size() > 2) {
			String f = iParams.get(2);
			if (f.startsWith("'") || f.startsWith("\""))
				f = f.substring(1, f.length() - 1);
			fields = f.split(",");
		}

		return new OQueryOperatorTraverse(start, end, fields);
	}

	public int getStartDeepLevel() {
		return startDeepLevel;
	}

	public int getEndDeepLevel() {
		return endDeepLevel;
	}

	public String[] getCfgFields() {
		return cfgFields;
	}

	@Override
	public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
		return OIndexReuseType.NO_INDEX;
	}

	@Override
	public String toString() {
		return String.format("%s(%d,%d,%s)", keyword, startDeepLevel, endDeepLevel, Arrays.toString(cfgFields));
	}
}
