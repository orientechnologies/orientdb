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

import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * Operator that filters the target records.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OQueryTargetOperator extends OQueryOperator {
	protected OQueryTargetOperator(final String iKeyword, final int iPrecedence, final boolean iLogical) {
		super(iKeyword, iPrecedence, iLogical);
	}

	public abstract Collection<OIdentifiable> filterRecords(final ODatabaseComplex<?> iRecord, final List<String> iTargetClasses,
			final OSQLFilterCondition iCondition, final Object iLeft, final Object iRight);

	/**
	 * At run-time the evaluation per record must return always true since the recordset are filtered at the begin.
	 */
	@Override
	public Object evaluateRecord(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight) {
		return true;
	}

	/**
	 * Default State-less implementation: does not save parameters and just return itself
	 * 
	 * @param iParams
	 * @return
	 */
	@Override
	public OQueryTargetOperator configure(final List<String> iParams) {
		return this;
	}
}
