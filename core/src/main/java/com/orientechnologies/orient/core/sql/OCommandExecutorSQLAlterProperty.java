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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterProperty extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_ALTER			= "ALTER";
	public static final String	KEYWORD_PROPERTY	= "PROPERTY";

	private String							className;
	private String							fieldName;
	private ATTRIBUTES					attribute;
	private String							value;

	public OCommandExecutorSQLAlterProperty parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_UPDATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_PROPERTY + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, oldPos);

		String[] parts = word.toString().split("\\.");
		if (parts.length != 2)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, oldPos);

		className = parts[0];
		if (className == null)
			throw new OCommandSQLParsingException("Class not found", text, oldPos);
		fieldName = parts[1];

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Missed the property's attribute to change", text, oldPos);

		final String attributeAsString = word.toString();

		try {
			attribute = OProperty.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
		} catch (IllegalArgumentException e) {
			throw new OCommandSQLParsingException("Unknown property's attribute '" + attributeAsString + "'. Supported attributes are: "
					+ Arrays.toString(OProperty.ATTRIBUTES.values()), text, oldPos);
		}

		value = text.substring(pos + 1).trim();

		if (value.length() == 0)
			throw new OCommandSQLParsingException("Missed the property's value to change for attribute '" + attribute + "'", text, oldPos);

		if (value.equalsIgnoreCase("null"))
			value = null;

		return this;
	}

	/**
	 * Execute the ALTER PROPERTY.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (attribute == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		final OClassImpl sourceClass = (OClassImpl) database.getMetadata().getSchema().getClass(className);
		if (sourceClass == null)
			throw new OCommandExecutionException("Source class '" + className + "' not found");

		final OPropertyImpl prop = (OPropertyImpl) sourceClass.getProperty(fieldName);
		if (prop == null)
			throw new OCommandExecutionException("Property '" + className + "." + fieldName + "' not exists");

		prop.setInternalAndSave(attribute, value);
		return null;
	}
}
