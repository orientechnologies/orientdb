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

import java.util.Locale;
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
			OCommandRequestText textRequest = iCommand;
			final String text = textRequest.getText();
			final String textUpperCase = text.toUpperCase(Locale.ENGLISH);

			delegate = (OCommandExecutorSQLAbstract) OSQLEngine.getInstance().getCommand(textUpperCase);
			if (delegate == null)
				throw new IllegalArgumentException("Can't find a command executor for the command request: " + iCommand);

			delegate.setLimit(iCommand.getLimit());
			delegate.parse(iCommand);
			delegate.setProgressListener(progressListener);
		} else
			throw new IllegalArgumentException("Can't find a command executor for the command request: " + iCommand);
		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		return delegate.execute(iArgs);
	}
}
