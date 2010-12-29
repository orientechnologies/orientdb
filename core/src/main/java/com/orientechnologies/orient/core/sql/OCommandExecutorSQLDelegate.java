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

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLDelegate extends OCommandExecutorSQLAbstract {
	private OCommandExecutorSQLAbstract	delegate;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLDelegate parse(final OCommandRequestText iCommand) {
		if (iCommand instanceof OCommandRequestText) {
			OCommandRequestText sql = iCommand;
			final String text = sql.getText();
			final String textUpperCase = text.toUpperCase();

			if (textUpperCase.startsWith(OCommandExecutorSQLSelect.KEYWORD_SELECT))
				delegate = new OCommandExecutorSQLSelect().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLInsert.KEYWORD_INSERT))
				delegate = new OCommandExecutorSQLInsert().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLUpdate.KEYWORD_UPDATE))
				delegate = new OCommandExecutorSQLUpdate().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLDelete.KEYWORD_DELETE))
				delegate = new OCommandExecutorSQLDelete().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLGrant.KEYWORD_GRANT))
				delegate = new OCommandExecutorSQLGrant().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLRevoke.KEYWORD_REVOKE))
				delegate = new OCommandExecutorSQLRevoke().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLCreateLink.KEYWORD_CREATE + " "
					+ OCommandExecutorSQLCreateLink.KEYWORD_LINK))
				delegate = new OCommandExecutorSQLCreateLink().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLCreateIndex.KEYWORD_CREATE + " "
					+ OCommandExecutorSQLCreateIndex.KEYWORD_INDEX))
				delegate = new OCommandExecutorSQLCreateIndex().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLRemoveIndex.KEYWORD_REMOVE + " "
					+ OCommandExecutorSQLRemoveIndex.KEYWORD_INDEX))
				delegate = new OCommandExecutorSQLRemoveIndex().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLCreateClass.KEYWORD_CREATE + " "
					+ OCommandExecutorSQLCreateClass.KEYWORD_CLASS))
				delegate = new OCommandExecutorSQLCreateClass().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLCreateProperty.KEYWORD_CREATE + " "
					+ OCommandExecutorSQLCreateProperty.KEYWORD_PROPERTY))
				delegate = new OCommandExecutorSQLCreateProperty().parse(iCommand);
			else if (textUpperCase.startsWith(OCommandExecutorSQLRemoveProperty.KEYWORD_REMOVE + " "
					+ OCommandExecutorSQLRemoveProperty.KEYWORD_PROPERTY))
				delegate = new OCommandExecutorSQLRemoveProperty().parse(iCommand);
			else
				throw new IllegalArgumentException("Can't find a command executor for the command request: " + iCommand);

			delegate.setProgressListener(progressListener);
		} else
			throw new IllegalArgumentException("Can't find a command executor for the command request: " + iCommand);
		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		return delegate.execute(iArgs);
	}
}
