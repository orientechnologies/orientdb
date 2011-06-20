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
package com.orientechnologies.orient.core.query;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandRequest;

public interface OQuery<T extends Object> extends OCommandRequest {

	/**
	 * Executes the query without limit about the result set. The limit will be bound to the maximum allowed.
	 * 
	 * @return List of records if any record matches the query constraints, otherwise an empty List.
	 */
	public List<T> run(Object... iArgs);

	/**
	 * Returns the first occurrence found if any
	 * 
	 * @return Record if found, otherwise null
	 */
	public T runFirst(Object... iArgs);

	/**
	 * Returns the fetch plan if any
	 * 
	 * @return Fetch plan as unique string or null if it was not defined.
	 */
	public String getFetchPlan();

	/**
	 * Set the fetch plan. The format is:
	 * 
	 * <pre>
	 * &lt;field&gt;:&lt;depth-level&gt;*
	 * </pre>
	 * 
	 * Where:
	 * <ul>
	 * <li><b>field</b> is the name of the field to specify the depth-level. <b>*</b> wildcard means any fields</li>
	 * <li><b>depth-level</b> is the depth level to fetch. -1 means infinite, 0 means no fetch at all and 1-N the depth level value.</li>
	 * </ul>
	 * Uses the blank spaces to separate the fields strategies.<br/>
	 * Example:
	 * 
	 * <pre>
	 * children:-1 parent:0 sibling:3 *:0
	 * </pre>
	 * 
	 * <br/>
	 * 
	 * @param iFetchPlan
	 * @return
	 */
	public OQuery<T> setFetchPlan(String iFetchPlan);

	public void reset();
}
