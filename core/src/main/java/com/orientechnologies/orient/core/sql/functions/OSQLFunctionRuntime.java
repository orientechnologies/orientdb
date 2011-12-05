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
package com.orientechnologies.orient.core.sql.functions;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemAbstract;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * Wraps functions managing the binding of parameters.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionRuntime extends OSQLFilterItemAbstract {

	public OSQLFunction	function;
	public Object[]			configuredParameters;
	public Object[]			runtimeParameters;

	public OSQLFunctionRuntime(final OCommandToParse iQueryToParse, final String iText) {
		super(iQueryToParse, iText);
	}

	public boolean aggregateResults() {
		return function.aggregateResults(configuredParameters);
	}

	public boolean filterResult() {
		return function.filterResult();
	}

	/**
	 * Execute a function.
	 * 
	 * @param iRecord
	 *          Current record
	 * @param iRequester
	 * @return
	 */
	public Object execute(final ORecordSchemaAware<?> iRecord, final OCommandExecutor iRequester) {
		// RESOLVE VALUES USING THE CURRENT RECORD
		for (int i = 0; i < configuredParameters.length; ++i) {
			if (configuredParameters[i] instanceof OSQLFilterItemField)
				runtimeParameters[i] = ((OSQLFilterItemField) configuredParameters[i]).getValue(iRecord);
			else if (configuredParameters[i] instanceof OSQLFunctionRuntime)
				runtimeParameters[i] = ((OSQLFunctionRuntime) configuredParameters[i]).execute(iRecord, iRequester);
		}

		final Object functionResult = function.execute(iRecord, runtimeParameters, iRequester);

		return transformValue(iRecord, functionResult);
	}

	public Object getResult() {
		return transformValue(null, function.getResult());
	}

	public void setResult(final Object iValue) {
		function.setResult(iValue);
	}

	public Object getValue(final OIdentifiable iRecord) {
		return execute(iRecord != null ? (ORecordSchemaAware<?>) iRecord.getRecord() : null, null);
	}

	@Override
	public String getRoot() {
		return function.getName();
	}

	@Override
	protected void setRoot(final OCommandToParse iQueryToParse, final String iText) {
		final int beginParenthesis = iText.indexOf('(');

		// SEARCH FOR THE FUNCTION
		final String funcName = iText.substring(0, beginParenthesis);

		final List<String> funcParamsText = OStringSerializerHelper.getParameters(iText);

		function = OSQLEngine.getInstance().getInlineFunction(funcName);
		if (function == null)
			// AGGREGATION ?
			function = OSQLEngine.getInstance().getAggregationFunction(funcName);

		if (function == null)
			throw new OCommandSQLParsingException("Unknow function " + funcName + "()");

		if (function.getMinParams() > -1 && funcParamsText.size() < function.getMinParams() || function.getMaxParams() > -1
				&& funcParamsText.size() > function.getMaxParams())
			throw new IllegalArgumentException("Syntax error. Expected: " + function.getSyntax());

		// PARSE PARAMETERS
		this.configuredParameters = new Object[funcParamsText.size()];
		for (int i = 0; i < funcParamsText.size(); ++i) {
			this.configuredParameters[i] = OSQLHelper.parseValue(null, iQueryToParse, funcParamsText.get(i));
		}

		// COPY STATIC VALUES
		this.runtimeParameters = new Object[configuredParameters.length];
		for (int i = 0; i < configuredParameters.length; ++i) {
			if (!(configuredParameters[i] instanceof OSQLFilterItemField) && !(configuredParameters[i] instanceof OSQLFunctionRuntime))
				runtimeParameters[i] = configuredParameters[i];
		}
	}
}
