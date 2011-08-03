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

import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL CREATE INDEX command: Create a new index against a property.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateIndex extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_CREATE	= "CREATE";
	public static final String	KEYWORD_INDEX		= "INDEX";

	private String							name;
	private String							indexType;
	private OType								keyType;

	public OCommandExecutorSQLCreateIndex parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_INDEX))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_INDEX + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected index name", text, oldPos);

		name = word.toString();

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Index type requested", text, oldPos + 1);

		// GET THE LINK TYPE
		indexType = word.toString();

		if (indexType == null)
			throw new OCommandSQLParsingException("Index type is null", text, pos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || word.toString().equalsIgnoreCase("NULL")) {
			if (name.indexOf('.') > 0) {
				final String[] parts = name.split("\\.");

				final OClass cls = database.getMetadata().getSchema().getClass(parts[0]);
				final OProperty prop = cls.getProperty(parts[1]);
				keyType = prop.getType();
			}
		} else
			// GET THE LINK TYPE
			keyType = OType.valueOf(word.toString());

		return this;
	}

	/**
	 * Execute the CREATE INDEX.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (name == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		final OIndex idx;
		if (name.indexOf('.') > -1) {
			// PROPERTY INDEX
			final String[] parts = name.split("\\.");
			final String className = parts[0];
			if (className == null)
				throw new OCommandExecutionException("Class " + className + " not found");
			String fieldName = parts[1];

			final OClass cls = database.getMetadata().getSchema().getClass(className);
			if (cls == null)
				throw new OCommandExecutionException("Class '" + className + "' not found");

			final OPropertyImpl prop = (OPropertyImpl) cls.getProperty(fieldName);
			if (prop == null)
				throw new IllegalArgumentException("Property '" + fieldName + "' was not found in class '" + cls + "'");

			idx = prop.createIndexInternal(indexType.toUpperCase(), progressListener).getUnderlying();
		} else {
			idx = database.getMetadata().getIndexManager().createIndex(name, indexType.toUpperCase(), keyType, null, null, null, false);
		}

		if (idx != null)
			return idx.getSize();

		return null;
	}
}
