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
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterClass extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_ALTER	= "ALTER";
	public static final String	KEYWORD_CLASS	= "CLASS";

	private String							className;
	private ATTRIBUTES					attribute;
	private String							value;

	public OCommandExecutorSQLAlterClass parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_UPDATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>", text, oldPos);

		className = word.toString();

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Missed the class's attribute to change", text, oldPos);

		final String attributeAsString = word.toString();

		try {
			attribute = OClass.ATTRIBUTES.valueOf(attributeAsString.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new OCommandSQLParsingException("Unknown class's attribute '" + attributeAsString + "'. Supported attributes are: "
					+ Arrays.toString(OClass.ATTRIBUTES.values()), text, oldPos);
		}

		value = text.substring(pos + 1).trim();

		if (value.length() == 0)
			throw new OCommandSQLParsingException("Missed the property's value to change for attribute '" + attribute + "'", text, oldPos);

		return this;
	}

	/**
	 * Execute the ALTER CLASS.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (attribute == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		final OClassImpl cls = (OClassImpl) database.getMetadata().getSchema().getClass(className);
		if (cls == null)
			throw new OCommandExecutionException("Source class '" + className + "' not found");

		cls.setInternalAndSave(attribute, value);
		renameCluster();
		return null;
	}

	private void renameCluster() {
		if (attribute.equals(OClass.ATTRIBUTES.NAME) && checkClusterRenameOk(database.getStorage().getClusterIdByName(value))) {
			database.command(new OCommandSQL("alter cluster " + className + " name " + value)).execute();
		}
	}

	private boolean checkClusterRenameOk(int clusterId) {
		for (OClass clazz : database.getMetadata().getSchema().getClasses()) {
			if (clazz.getName().equals(value))
				continue;
			else if (clazz.getDefaultClusterId() == clusterId || Arrays.asList(clazz.getClusterIds()).contains(clusterId))
				return false;
		}
		return true;
	}
}
