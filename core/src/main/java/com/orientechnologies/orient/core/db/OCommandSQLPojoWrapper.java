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
package com.orientechnologies.orient.core.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Wraps the execution of a generic command by assuring to convert the result set in POJO where applicable.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OCommandSQLPojoWrapper implements OCommandRequest {
	private OCommandRequest						command;
	private ODatabasePojoAbstract<?>	database;

	public OCommandSQLPojoWrapper(final ODatabasePojoAbstract<?> iDatabase, final OCommandRequest iCommand) {
		database = iDatabase;
		command = iCommand;
	}

	@SuppressWarnings("unchecked")
	public <RET> RET execute(Object... iArgs) {
		database.convertParameters(iArgs);

		Object result = command.execute(iArgs);

		if (result instanceof Collection<?>) {
			final List<Object> resultPojo = new ArrayList<Object>();

			Object obj;
			Collection<ODocument> coll = (Collection<ODocument>) result;
			for (ODocument doc : coll) {
				// GET THE ASSOCIATED DOCUMENT
				if (doc.getClassName() == null)
					obj = doc;
				else
					// CONVERT THE DOCUMENT INSIDE THE LIST
					obj = database.getUserObjectByRecord(doc, null, true);

				resultPojo.add(obj);
			}
			result = resultPojo;

		} else if (result instanceof ODocument) {
			if (((ODocument) result).getClassName() != null)
				// CONVERT THE SINGLE DOCUMENT
				result = database.getUserObjectByRecord((ODocument) result, null, true);
		}

		return (RET) result;
	}

	public int getLimit() {
		return command.getLimit();
	}

	public OCommandRequest setLimit(final int iLimit) {
		command.setLimit(iLimit);
		return this;
	}

}
