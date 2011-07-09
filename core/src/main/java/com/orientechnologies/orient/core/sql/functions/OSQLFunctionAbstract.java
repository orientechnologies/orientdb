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

/**
 * Abstract class to extend to build Custom SQL Functions. Extend it and register it with:
 * <code>OSQLParser.getInstance().registerStatelessFunction()</code> or
 * <code>OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSQLFunctionAbstract implements OSQLFunction {
	protected String	name;
	protected int			minParams;
	protected int			maxParams;

	public OSQLFunctionAbstract(final String iName, final int iMinParams, final int iMaxParams) {
		this.name = iName;
		this.minParams = iMinParams;
		this.maxParams = iMaxParams;
	}

	public String getName() {
		return name;
	}

	public int getMinParams() {
		return minParams;
	}

	public int getMaxParams() {
		return maxParams;
	}

	@Override
	public String toString() {
		return name + "()";
	}

	public boolean aggregateResults(final Object[] iConfiguredParameters) {
		return false;
	}

	public boolean filterResult() {
		return false;
	}

	public Object getResult() {
		return null;
	}

	public void setResult(final Object iResult) {
	}
}
