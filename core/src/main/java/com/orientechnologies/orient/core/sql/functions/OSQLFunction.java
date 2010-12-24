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

import com.orientechnologies.common.types.ORef;

/**
 * Interface that defines a SQL Function. Functions are state-less and are reused across queries. So don't keep any run-time
 * information inside of it. Implement it and register it with: <code>OSQLParser.getInstance().registerFunction()</code> to being
 * used by the SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OSQLFunction {

	public Object execute(final ORef<Object> context, final Object[] funcParams);

	public String getName();

	public int getMinParams();

	public int getMaxParams();

	public String getSyntax();
}
