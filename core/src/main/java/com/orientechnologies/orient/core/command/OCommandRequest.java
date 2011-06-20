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
package com.orientechnologies.orient.core.command;

/**
 * Generic GOF command pattern implementation. Execute a command passing the optional arguments "iArgs" and returns an Object.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 */
public interface OCommandRequest {
	public <RET> RET execute(Object... iArgs);

	/**
	 * Returns the limit of result set. -1 means no limits.
	 * 
	 */
	public int getLimit();

	/**
	 * Sets the maximum items the command can returns. -1 means no limits.
	 * 
	 * @param iLimit
	 *          -1 = no limit. 1 to N to limit the result set.
	 * @return
	 */
	public OCommandRequest setLimit(final int iLimit);
}
