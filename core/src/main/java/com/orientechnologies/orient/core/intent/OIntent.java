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
package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

/**
 * Intents aim to define common use case in order to optimize the execution.
 * 
 * @author Luca Garulli
 * 
 */
public interface OIntent {
	/**
	 * Activate the intent.
	 * 
	 * @param iDatabase
	 *          Database where to activate it
	 * @param iArgs
	 *          Additional, optional arguments
	 */
	public void begin(ODatabaseRaw iDatabase, Object... iArgs);

	/**
	 * Activate the intent.
	 * 
	 * @param iDatabase
	 *          Database where to activate it
	 * @param iArgs
	 *          Additional, optional arguments
	 */
	public void end(ODatabaseRaw iDatabase);
}
