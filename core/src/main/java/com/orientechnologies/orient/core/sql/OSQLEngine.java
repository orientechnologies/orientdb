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
package com.orientechnologies.orient.core.sql;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDifference;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionIntersect;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionUnion;
import com.orientechnologies.orient.core.sql.functions.geo.OSQLFunctionDistance;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionAverage;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMax;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMin;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionSum;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionFormat;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionSysdate;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorAnd;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContains;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsAll;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsKey;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsText;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsValue;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIs;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorLike;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMatches;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNot;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNotEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorOr;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorTraverse;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorDivide;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMinus;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMod;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMultiply;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorPlus;

public class OSQLEngine {
	private Map<String, OSQLFunction>																	inlineFunctions				= new HashMap<String, OSQLFunction>();
	private Map<String, Class<? extends OSQLFunction>>								aggregationFunctions	= new HashMap<String, Class<? extends OSQLFunction>>();
	private Map<String, Class<? extends OCommandExecutorSQLAbstract>>	commands							= new HashMap<String, Class<? extends OCommandExecutorSQLAbstract>>();
	public static OQueryOperator[]																		RECORD_OPERATORS			= { new OQueryOperatorAnd(),
			new OQueryOperatorOr(), new OQueryOperatorNotEquals(), new OQueryOperatorNot(), new OQueryOperatorEquals(),
			new OQueryOperatorMinorEquals(), new OQueryOperatorMinor(), new OQueryOperatorMajorEquals(), new OQueryOperatorContainsAll(),
			new OQueryOperatorMajor(), new OQueryOperatorLike(), new OQueryOperatorMatches(), new OQueryOperatorIs(),
			new OQueryOperatorIn(), new OQueryOperatorContainsKey(), new OQueryOperatorContainsValue(), new OQueryOperatorContainsText(),
			new OQueryOperatorContains(), new OQueryOperatorContainsText(), new OQueryOperatorTraverse(), new OQueryOperatorBetween(),
			new OQueryOperatorPlus(), new OQueryOperatorMinus(), new OQueryOperatorMultiply(), new OQueryOperatorDivide(),
			new OQueryOperatorMod()																														};

	private static final OSQLEngine																		INSTANCE							= new OSQLEngine();

	protected OSQLEngine() {
		// COMMANDS
		commands.put(OCommandExecutorSQLAlterDatabase.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterDatabase.KEYWORD_DATABASE,
				OCommandExecutorSQLAlterDatabase.class);
		commands.put(OCommandExecutorSQLSelect.KEYWORD_SELECT, OCommandExecutorSQLSelect.class);
		commands.put(OCommandExecutorSQLInsert.KEYWORD_INSERT, OCommandExecutorSQLInsert.class);
		commands.put(OCommandExecutorSQLUpdate.KEYWORD_UPDATE, OCommandExecutorSQLUpdate.class);
		commands.put(OCommandExecutorSQLDelete.KEYWORD_DELETE, OCommandExecutorSQLDelete.class);
		commands.put(OCommandExecutorSQLGrant.KEYWORD_GRANT, OCommandExecutorSQLGrant.class);
		commands.put(OCommandExecutorSQLRevoke.KEYWORD_REVOKE, OCommandExecutorSQLRevoke.class);
		commands.put(OCommandExecutorSQLCreateLink.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateLink.KEYWORD_LINK,
				OCommandExecutorSQLCreateLink.class);
		commands.put(OCommandExecutorSQLCreateIndex.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateIndex.KEYWORD_INDEX,
				OCommandExecutorSQLCreateIndex.class);
		commands.put(OCommandExecutorSQLDropIndex.KEYWORD_DROP + " " + OCommandExecutorSQLDropIndex.KEYWORD_INDEX,
				OCommandExecutorSQLDropIndex.class);
		commands.put(OCommandExecutorSQLRebuildIndex.KEYWORD_REBUILD + " " + OCommandExecutorSQLRebuildIndex.KEYWORD_INDEX,
				OCommandExecutorSQLRebuildIndex.class);
		commands.put(OCommandExecutorSQLCreateClass.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateClass.KEYWORD_CLASS,
				OCommandExecutorSQLCreateClass.class);
		commands.put(OCommandExecutorSQLAlterClass.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterClass.KEYWORD_CLASS,
				OCommandExecutorSQLAlterClass.class);
		commands.put(OCommandExecutorSQLCreateProperty.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLCreateProperty.class);
		commands.put(OCommandExecutorSQLAlterProperty.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLAlterProperty.class);
		commands.put(OCommandExecutorSQLDropClass.KEYWORD_DROP + " " + OCommandExecutorSQLDropClass.KEYWORD_CLASS,
				OCommandExecutorSQLDropClass.class);
		commands.put(OCommandExecutorSQLDropProperty.KEYWORD_DROP + " " + OCommandExecutorSQLDropProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLDropProperty.class);
		commands.put(OCommandExecutorSQLFindReferences.KEYWORD_FIND + " " + OCommandExecutorSQLFindReferences.KEYWORD_REFERENCES,
				OCommandExecutorSQLFindReferences.class);
		commands.put(OCommandExecutorSQLTruncateClass.KEYWORD_TRUNCATE + " " + OCommandExecutorSQLTruncateClass.KEYWORD_CLASS,
				OCommandExecutorSQLTruncateClass.class);
		commands.put(OCommandExecutorSQLTruncateCluster.KEYWORD_TRUNCATE + " " + OCommandExecutorSQLTruncateCluster.KEYWORD_CLUSTER,
				OCommandExecutorSQLTruncateCluster.class);
		commands.put(OCommandExecutorSQLTruncateRecord.KEYWORD_TRUNCATE + " " + OCommandExecutorSQLTruncateRecord.KEYWORD_RECORD,
				OCommandExecutorSQLTruncateRecord.class);
		commands.put(OCommandExecutorSQLAlterCluster.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterCluster.KEYWORD_CLUSTER,
				OCommandExecutorSQLAlterCluster.class);

		// MISC FUNCTIONS
		registerFunction(OSQLFunctionFormat.NAME, new OSQLFunctionFormat());
		registerFunction(OSQLFunctionSysdate.NAME, OSQLFunctionSysdate.class);
		registerFunction(OSQLFunctionCount.NAME, OSQLFunctionCount.class);
		registerFunction(OSQLFunctionDistinct.NAME, OSQLFunctionDistinct.class);
		registerFunction(OSQLFunctionUnion.NAME, OSQLFunctionUnion.class);
		registerFunction(OSQLFunctionIntersect.NAME, OSQLFunctionIntersect.class);
		registerFunction(OSQLFunctionDifference.NAME, OSQLFunctionDifference.class);

		// MATH FUNCTIONS
		registerFunction(OSQLFunctionMin.NAME, OSQLFunctionMin.class);
		registerFunction(OSQLFunctionMax.NAME, OSQLFunctionMax.class);
		registerFunction(OSQLFunctionSum.NAME, OSQLFunctionSum.class);
		registerFunction(OSQLFunctionAverage.NAME, OSQLFunctionAverage.class);

		// GEO FUNCTIONS
		registerFunction(OSQLFunctionDistance.NAME, new OSQLFunctionDistance());
	}

	public OQueryOperator[] getRecordOperators() {
		return RECORD_OPERATORS;
	}

	public static void registerOperator(final OQueryOperator iOperator) {
		final OQueryOperator[] ops = new OQueryOperator[RECORD_OPERATORS.length + 1];
		System.arraycopy(RECORD_OPERATORS, 0, ops, 0, RECORD_OPERATORS.length);
		RECORD_OPERATORS = ops;
	}

	public void registerFunction(final String iName, final OSQLFunction iFunction) {
		inlineFunctions.put(iName.toUpperCase(Locale.ENGLISH), iFunction);
	}

	public void registerFunction(final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
		aggregationFunctions.put(iName.toUpperCase(Locale.ENGLISH), iFunctionClass);
	}

	public void unregisterInlineFunction(final String iName) {
		inlineFunctions.remove(iName.toUpperCase(Locale.ENGLISH));
	}

	public OSQLFunction getInlineFunction(final String iFunctionName) {
		return inlineFunctions.get(iFunctionName.toUpperCase(Locale.ENGLISH));
	}

	public OSQLFunction getAggregationFunction(final String iFunctionName) {
		final Class<? extends OSQLFunction> f = aggregationFunctions.get(iFunctionName.toUpperCase(Locale.ENGLISH));
		if (f != null)
			try {
				return f.newInstance();
			} catch (Exception e) {
				throw new OCommandExecutionException("Error in creation of function " + iFunctionName
						+ "(). Probably there is not an empty constructor or the constructor generates errors", e);
			}
		return null;
	}

	public void unregisterFunction(final String iName) {
		final String name = iName.toUpperCase(Locale.ENGLISH);
		if (inlineFunctions.remove(name) == null)
			aggregationFunctions.remove(name);
	}

	public OCommandExecutorSQLAbstract getCommand(final String iText) {
		int pos = -1;
		Class<? extends OCommandExecutorSQLAbstract> commandClass = null;
		while (commandClass == null) {
			pos = iText.indexOf(' ', pos + 1);
			if (pos > -1) {
				String piece = iText.substring(0, pos);
				commandClass = commands.get(piece);
			} else
				break;
		}

		if (commandClass != null)
			try {
				return commandClass.newInstance();
			} catch (Exception e) {
				throw new OCommandExecutionException("Error in creation of command " + commandClass
						+ "(). Probably there is not an empty constructor or the constructor generates errors", e);
			}

		return null;
	}

	public OSQLFilter parseFromWhereCondition(final ODatabaseRecord iDatabase, final String iText) {
		return new OSQLFilter(iDatabase, iText);
	}

	public static OSQLEngine getInstance() {
		return INSTANCE;
	}
}
