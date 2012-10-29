/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.sql.functions.math;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;

/**
 * Evaluates a complex expression.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionEval extends OSQLFunctionMathAbstract {
	public static final String	NAME	= "eval";

	private OSQLPredicate				predicate;

	public OSQLFunctionEval() {
		super(NAME, 1, 1);
	}

	public Object execute(final OIdentifiable iRecord, final ODocument iCurrentResult, final Object[] iParameters,
			OCommandContext iContext) {
		if (predicate == null)
			predicate = new OSQLPredicate((String) iParameters[0]);

		try {
			return predicate.evaluate(iRecord.getRecord(), iCurrentResult, iContext);
		} catch (Exception e) {
			return null;
		}
	}

	public boolean aggregateResults() {
		return false;
	}

	public String getSyntax() {
		return "Syntax error: eval(<expression>)";
	}

	@Override
	public Object getResult() {
		return null;
	}

	@Override
	public Object mergeDistributedResult(List<Object> resultsToMerge) {
		return null;
	}
}
