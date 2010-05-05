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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

/**
 * SQL UPDATE command.
 * 
 * @author luca
 * 
 */
public class OCommandExecutorSQLUpdate extends OCommandExecutorSQLAbstract implements OCommandResultListener {
	private static final String	KEYWORD_SET		= "SET";
	private String							className			= null;
	private Map<String, Object>	fieldEntries	= new HashMap<String, Object>();
	private OCommandSQL					query;
	private int									recordCount		= 0;

	public OCommandExecutorSQLUpdate parse(final OCommandRequestInternal<ODatabaseRecord<?>> iRequest) {
		init(iRequest.getDatabase(), iRequest.getText());

		className = null;
		fieldEntries.clear();
		query = null;
		recordCount = 0;

		final StringBuilder word = new StringBuilder();

		int pos = OSQLHelper.nextWord(text, textUpperCase, 0, word, true);
		if (pos == -1 || !word.toString().equals(OSQLHelper.KEYWORD_UPDATE))
			throw new OCommandSQLParsingException("Keyword " + OSQLHelper.KEYWORD_UPDATE + " not found", text, 0);

		int newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (newPos == -1)
			throw new OCommandSQLParsingException("Invalid cluster/class name", text, pos);

		pos = newPos;

		String subjectName = word.toString();

		if (subjectName.startsWith(OSQLHelper.CLASS_PREFIX))
			subjectName = subjectName.substring(OSQLHelper.CLASS_PREFIX.length());

		// CLASS
		final OClass cls = database.getMetadata().getSchema().getClass(subjectName);
		if (cls == null)
			throw new OCommandSQLParsingException("Class " + subjectName + " not found in database", text, pos);

		className = cls.getName();

		newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_SET))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_SET + " not found", text, pos);

		String fieldName;
		String fieldValue;

		while (pos != -1 && (fieldEntries.size() == 0 || word.toString().equals(","))) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, newPos, word, false);
			if (pos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);
			pos = newPos;

			fieldName = word.toString();

			pos = OSQLHelper.jumpWhiteSpaces(text, pos);

			if (pos == -1 || text.charAt(pos) != '=')
				throw new OCommandSQLParsingException("Character '=' was expected", text, pos);

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos+1, word, false);
			if (pos == -1)
				throw new OCommandSQLParsingException("Value expected", text, pos);

			fieldValue = word.toString();

			if (fieldValue.endsWith(",")) {
				pos = newPos - 1;
				fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
			} else
				pos = newPos;

			// INSERT TRANSFORMED FIELD VALUE
			fieldEntries.put(fieldName, OSQLHelper.convertValue(fieldValue));

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (fieldEntries.size() == 0)
			throw new OCommandSQLParsingException("Set of entries <field> = <value> is missed. Example: name = 'Bill', salary = 300.2",
					text, pos);

		String whereCondition = word.toString();

		if (whereCondition.equals(OSQLHelper.KEYWORD_WHERE))
			query = database.command(new OSQLAsynchQuery<ODocument>("select from " + className + text.substring(pos), this));
		else
			query = database.command(new OSQLAsynchQuery<ODocument>("select from " + className, this));

		return this;
	}

	public Object execute(final Object... iArgs) {
		if (className == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		query.execute();
		return recordCount;
	}

	/**
	 * Update current record.
	 */
	public boolean result(final Object iRecord) {
		ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;

		// BIND VALUES
		for (Map.Entry<String, Object> entry : fieldEntries.entrySet())
			record.field(entry.getKey(), entry.getValue());

		record.save();
		recordCount++;
		return true;
	}
}
