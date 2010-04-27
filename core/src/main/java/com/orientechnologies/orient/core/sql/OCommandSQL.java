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

/**
 * SQL command implementation.
 * 
 * @author luca
 * 
 */
public class OCommandSQL extends OCommandSQLAbstract {
	private OCommandSQLAbstract	commandToDelegate;

	public OCommandSQL(final String iText) {
		super(iText, iText.toUpperCase());
	}

	public Object execute() {
		parse();
		return commandToDelegate.execute();
	}

	public void parse() {
		if (commandToDelegate != null)
			// ALREADY PARSED
			return;

		if (text == null || text.length() == 0)
			throw new IllegalArgumentException("Invalid SQL command");

		if (textUpperCase.startsWith(OSQLHelper.KEYWORD_INSERT))
			commandToDelegate = new OCommandSQLInsert(text, textUpperCase, database);
		else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_UPDATE))
			commandToDelegate = new OCommandSQLUpdate(text, textUpperCase, database);
		else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_DELETE))
			commandToDelegate = new OCommandSQLDelete(text, textUpperCase, database);
		else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_SELECT)) {
			// TODO: WHAT TODO WITH SELECT?
			return;
		}
	}
}
