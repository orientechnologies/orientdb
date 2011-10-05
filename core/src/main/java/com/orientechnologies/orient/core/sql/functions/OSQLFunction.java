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

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Interface that defines a SQL Function. Functions can be state-less if registered as instance, or state-full when registered as
 * class. State-less functions are reused across queries, so don't keep any run-time information inside of it. State-full functions,
 * instead, stores Implement it and register it with: <code>OSQLParser.getInstance().registerFunction()</code> to being used by the
 * SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OSQLFunction {

	public Object execute(ORecord<?> iCurrentRecord, Object[] iFuncParams, OCommandExecutor iRequester);

	public boolean aggregateResults(Object[] configuredParameters);

	public boolean filterResult();

	public String getName();

	public int getMinParams();

	public int getMaxParams();

	public String getSyntax();

	public Object getResult();

	public void setResult(Object iResult);
}
