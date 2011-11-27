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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL CREATE INDEX command: Create a new index against a property.
 * <p/>
 * <p>
 * Supports following grammar: <br/>
 * "CREATE" "INDEX" &lt;indexName&gt; ["ON" &lt;className&gt; "(" &lt;propName&gt; ("," &lt;propName&gt;)* ")"] &lt;indexType&gt;
 * [&lt;keyType&gt; ("," &lt;keyType&gt;)*]
 * </p>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateIndex extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_CREATE	= "CREATE";
	public static final String	KEYWORD_INDEX		= "INDEX";
	public static final String	KEYWORD_ON			= "ON";

	private String							indexName;
	private OClass							oClass;
	private String[]						fields;
	private OClass.INDEX_TYPE		indexType;
	private OType[]							keyTypes;

	public OCommandExecutorSQLCreateIndex parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		final StringBuilder word = new StringBuilder();

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

		indexName = word.toString();

		final int namePos = oldPos;
		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Index type requested", text, oldPos + 1);

		if (word.toString().equals(KEYWORD_ON)) {
			if (indexName.contains(".")) {
				throw new OCommandSQLParsingException("Index name can't contain '.' character", text, namePos);
			}

			oldPos = pos;
			pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
			if (pos == -1)
				throw new OCommandSQLParsingException("Expected class name", text, oldPos);
			oldPos = pos;
			oClass = findClass(word.toString());

			if (oClass == null)
				throw new OCommandExecutionException("Class " + word + " not found");

			pos = textUpperCase.indexOf(")");
			if (pos == -1) {
				throw new OCommandSQLParsingException("No right bracket found", text, oldPos);
			}

			final String props = textUpperCase.substring(oldPos, pos).trim().substring(1);

			List<String> propList = new ArrayList<String>();
			List<OType> typeList = new ArrayList<OType>();
			for (String propName : props.trim().split("\\s*,\\s*")) {
				final OProperty property = oClass.getProperty(propName);

				if (property == null)
					throw new IllegalArgumentException("Property '" + propName + "' was not found in class '" + oClass.getName() + "'");

				propList.add(property.getName());
				typeList.add(property.getType());
			}

			fields = new String[propList.size()];
			propList.toArray(fields);

			oldPos = pos + 1;
			pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
			if (pos == -1)
				throw new OCommandSQLParsingException("Index type requested", text, oldPos + 1);

			keyTypes = new OType[propList.size()];
			typeList.toArray(keyTypes);
		} else {
			if (indexName.indexOf('.') > 0) {
				final String[] parts = indexName.split("\\.");

				oClass = findClass(parts[0]);
				if (oClass == null)
					throw new OCommandExecutionException("Class " + parts[0] + " not found");
				final OProperty prop = oClass.getProperty(parts[1]);
				if (prop == null)
					throw new IllegalArgumentException("Property '" + parts[1] + "' was not found in class '" + oClass.getName() + "'");

				fields = new String[] { prop.getName() };
				keyTypes = new OType[] { prop.getType() };
			}
		}

		indexType = OClass.INDEX_TYPE.valueOf(word.toString());

		if (indexType == null)
			throw new OCommandSQLParsingException("Index type is null", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos != -1 && !word.toString().equalsIgnoreCase("NULL")) {
			final String typesString = textUpperCase.substring(oldPos).trim();

			ArrayList<OType> keyTypeList = new ArrayList<OType>();
			for (String typeName : typesString.split("\\s*,\\s*")) {
				keyTypeList.add(OType.valueOf(typeName));
			}

			OType[] parsedKeyTypes = new OType[keyTypeList.size()];
			keyTypeList.toArray(parsedKeyTypes);

			if (keyTypes == null) {
				keyTypes = parsedKeyTypes;
			} else {
				if (!Arrays.deepEquals(keyTypes, parsedKeyTypes)) {
					throw new OCommandSQLParsingException("Property type list not match with real property types", text, oldPos);
				}
			}
		}

		return this;
	}

	private OClass findClass(String part) {
		return database.getMetadata().getSchema().getClass(part);
	}

	/**
	 * Execute the CREATE INDEX.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (indexName == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		final OIndex<?> idx;
		if (fields == null || fields.length == 0) {
			if (keyTypes != null)
				idx = database.getMetadata().getIndexManager()
						.createIndex(indexName, indexType.toString(), new OSimpleKeyIndexDefinition(keyTypes), null, null);
			else
				idx = database.getMetadata().getIndexManager().createIndex(indexName, indexType.toString(), null, null, null);
		} else {
			idx = oClass.createIndex(indexName, indexType, fields);
		}

		if (idx != null)
			return idx.getSize();

		return null;
	}
}
