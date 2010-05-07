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

import com.orientechnologies.orient.core.command.OCommandRequestInternal;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLDelegate extends OCommandExecutorSQLAbstract {
	private OCommandExecutorSQLAbstract	delegate;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLDelegate parse(final OCommandRequestInternal iCommand) {
		if (iCommand instanceof OCommandSQL) {
			OCommandSQL sql = (OCommandSQL) iCommand;
			final String text = sql.getText();
			final String textUpperCase = text.toUpperCase();

			if (textUpperCase.startsWith(OSQLHelper.KEYWORD_SELECT))
				delegate = new OCommandExecutorSQLSelect().parse(iCommand);
			else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_INSERT))
				delegate = new OCommandExecutorSQLInsert().parse(iCommand);
			else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_UPDATE))
				delegate = new OCommandExecutorSQLUpdate().parse(iCommand);
			else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_DELETE))
				delegate = new OCommandExecutorSQLDelete().parse(iCommand);
			else
				throw new IllegalArgumentException("Can't find a command executor for the command request: " + iCommand);
		} else
			throw new IllegalArgumentException("Can't find a command executor for the command request: " + iCommand);
		return this;
	}

	public Object execute(final Object... iArgs) {
		return delegate.execute(iArgs);
	}
}
