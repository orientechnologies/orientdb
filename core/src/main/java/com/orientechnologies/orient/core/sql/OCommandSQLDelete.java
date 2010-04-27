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

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.query.OAsynchQueryResultListener;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

/**
 * SQL UPDATE command.
 * 
 * @author luca
 * 
 */
public class OCommandSQLDelete extends OCommandSQLAbstract implements OAsynchQueryResultListener<ODocument> {
	private OQuery<ODocument>	query;
	private int								recordCount	= 0;

	public OCommandSQLDelete(final String iText, final String iTextUpperCase, final ODatabaseRecord<ODocument> iDatabase) {
		super(iText, iTextUpperCase, iDatabase);
	}

	@Override
	public void parse() {
		if (query != null)
			// ALREADY PARSED
			return;

		StringBuilder word = new StringBuilder();

		int pos = OSQLHelper.nextWord(text, textUpperCase, 0, word, true);
		if (pos == -1 || !word.toString().equals(OSQLHelper.KEYWORD_DELETE))
			throw new OCommandSQLParsingException("Keyword " + OSQLHelper.KEYWORD_DELETE + " not found", text, 0);

		query = database.query(new OSQLAsynchQuery<ODocument>("select " + text.substring(pos), this));
	}

	public Object execute() {
		parse();
		query.execute();
		return recordCount;
	}

	/**
	 * Delete the current record.
	 */
	public boolean result(final ODocument iRecord) {
		iRecord.delete();
		recordCount++;
		return true;
	}
}
