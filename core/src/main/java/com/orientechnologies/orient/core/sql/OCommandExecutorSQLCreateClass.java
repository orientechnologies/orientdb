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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateClass extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_CREATE	= "CREATE";
	public static final String	KEYWORD_CLASS		= "CLASS";
	public static final String	KEYWORD_EXTENDS	= "EXTENDS";

	private String							className;
	private OClass							superClass;
	private int[]								clusterIds;

	public OCommandExecutorSQLCreateClass parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>", text, oldPos);

		className = word.toString();
		if (className == null)
			throw new OCommandSQLParsingException("Class " + className + " already exists", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos > -1) {
			if (word.toString().equals(KEYWORD_EXTENDS)) {
				oldPos = pos;
				pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
				if (pos == -1)
					throw new OCommandSQLParsingException("Syntax error after EXTENDS for class " + className
							+ ". Expected the super-class name ", text, oldPos);

				if (!database.getMetadata().getSchema().existsClass(word.toString()))
					throw new OCommandSQLParsingException("Super-class " + word + " not exists", text, oldPos);

				superClass = database.getMetadata().getSchema().getClass(word.toString());

				oldPos = pos;
				pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
			}

			if (pos > -1) {
				final String[] clusterIdsAsStrings = word.toString().split(",");
				if (clusterIdsAsStrings.length > 0) {
					clusterIds = new int[clusterIdsAsStrings.length];
					for (int i = 0; i < clusterIdsAsStrings.length; ++i) {
						clusterIds[i] = Integer.parseInt(clusterIdsAsStrings[i]);
						if (database.getStorage().getClusterById(clusterIds[i]) == null)
							throw new OCommandSQLParsingException("Cluster with id " + clusterIds[i] + " doesn't exists", text, oldPos);
					}
				}
			} else {
				final int clusterId = database.getStorage().getClusterIdByName(className);
				if (clusterId > -1) {
					clusterIds = new int[] { clusterId };
				}
			}
		} else {
			final int clusterId = database.getStorage().getClusterIdByName(className);
			if (clusterId > -1) {
				clusterIds = new int[] { clusterId };
			}
		}
		return this;
	}

	/**
	 * Execute the CREATE CLASS.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (className == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		if (database.getMetadata().getSchema().existsClass(className))
			throw new OCommandExecutionException("Class " + className + " already exists");

		final OClassImpl sourceClass = (OClassImpl) ((OSchemaProxy) database.getMetadata().getSchema()).createClassInternal(className,
				superClass, clusterIds);
		sourceClass.saveInternal();
		return database.getMetadata().getSchema().getClasses().size();
	}
}
