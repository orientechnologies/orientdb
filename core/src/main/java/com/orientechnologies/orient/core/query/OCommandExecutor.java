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

import com.orientechnologies.orient.core.command.OCommandInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

public interface OCommandExecutor {
	public Object execute(OCommandInternal iCommand);

	public <R extends ORecordSchemaAware<?>> List<R> execute(OQuery<R> iQuery, int iLimit);

	public <R extends ORecordSchemaAware<?>> R executeFirst(OQuery<R> iQuery);

}
