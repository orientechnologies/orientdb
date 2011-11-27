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

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterCluster extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_ALTER		= "ALTER";
	public static final String	KEYWORD_CLUSTER	= "CLUSTER";

	protected String						clusterName;
	protected int								clusterId				= -1;
	protected ATTRIBUTES				attribute;
	protected String						value;

	public OCommandExecutorSQLAlterCluster parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_UPDATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLUSTER + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <cluster-name>", text, oldPos);

		clusterName = word.toString();

		final Pattern p = Pattern.compile("([0-9]*)");
		final Matcher m = p.matcher(clusterName);
		if (m.matches())
			clusterId = Integer.parseInt(clusterName);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Missing cluster attribute to change", text, oldPos);

		final String attributeAsString = word.toString();

		try {
			attribute = OCluster.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
		} catch (IllegalArgumentException e) {
			throw new OCommandSQLParsingException("Unknown class attribute '" + attributeAsString + "'. Supported attributes are: "
					+ Arrays.toString(OCluster.ATTRIBUTES.values()), text, oldPos);
		}

		value = text.substring(pos + 1).trim();

		if (value.length() == 0)
			throw new OCommandSQLParsingException("Missing property value to change for attribute '" + attribute + "'", text, oldPos);

		if (value.equalsIgnoreCase("null"))
			value = null;

		return this;
	}

	/**
	 * Execute the ALTER CLASS.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (attribute == null)
			throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

		final OCluster cls = getCluster();

		if (cls == null)
			throw new OCommandExecutionException("Cluster '" + clusterName + "' not found");

		if (clusterId > -1 && clusterName.equals(String.valueOf(clusterId))) {
			clusterName = cls.getName();
		} else {
			clusterId = cls.getId();
		}

		try {
			cls.set(attribute, value);
		} catch (IOException ioe) {
			throw new OCommandExecutionException("Error altering cluster '" + clusterName + "'", ioe);
		}

		return null;
	}

	protected OCluster getCluster() {
		if (clusterId > -1) {
			return database.getStorage().getClusterById(clusterId);
		} else {
			return database.getStorage().getClusterById(database.getStorage().getClusterIdByName(clusterName));
		}

	}
}
