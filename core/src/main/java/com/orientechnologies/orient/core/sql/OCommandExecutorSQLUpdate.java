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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLUpdate extends OCommandExecutorSQLAbstract implements OCommandResultListener {
	private static final String	KEYWORD_SET			= "SET";
	private static final String	KEYWORD_REMOVE	= "REMOVE";
	private String							className				= null;
	private Map<String, Object>	setEntries			= new HashMap<String, Object>();
	private Set<String>					removeEntries		= new HashSet<String>();
	private OCommandSQL					query;
	private int									recordCount			= 0;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLUpdate parse(final OCommandRequestInternal iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.OPERATIONS.UPDATE);

		init(iRequest.getDatabase(), iRequest.getText());

		className = null;
		setEntries.clear();
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
		if (pos == -1 || (!word.toString().equals(KEYWORD_SET) && !word.toString().equals(KEYWORD_REMOVE)))
			throw new OCommandSQLParsingException("Expected keyword " + KEYWORD_SET + " or " + KEYWORD_REMOVE, text, pos);

		while (pos != -1 && !word.toString().equals(OSQLHelper.KEYWORD_WHERE)) {
			if (word.toString().equals(KEYWORD_SET)) {
				pos = parseSetFields(word, pos, newPos);
			} else if (word.toString().equals(KEYWORD_REMOVE)) {
				pos = parseRemoveFields(word, newPos);
			}
		}

		String whereCondition = word.toString();

		if (whereCondition.equals(OSQLHelper.KEYWORD_WHERE))
			query = database.command(new OSQLAsynchQuery<ODocument>("select from " + className + " where " + text.substring(pos), this));
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

		// BIND VALUES TO UPDATE
		Object v;
		for (Map.Entry<String, Object> entry : setEntries.entrySet()) {
			v = entry.getValue();

			if (v instanceof OSQLFilterItem)
				v = ((OSQLFilterItem) v).getValue(record);

			record.field(entry.getKey(), v);
		}

		// REMOVE FIELD IF ANY
		for (String entry : removeEntries)
			record.removeField(entry);

		record.save();
		recordCount++;
		return true;
	}

	private int parseSetFields(final StringBuilder word, int pos, int newPos) {
		String fieldName;
		String fieldValue;

		while (pos != -1 && (setEntries.size() == 0 || word.toString().equals(","))) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, newPos, word, false);
			if (pos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);
			pos = newPos;

			fieldName = word.toString();

			pos = OSQLHelper.jumpWhiteSpaces(text, pos);

			if (pos == -1 || text.charAt(pos) != '=')
				throw new OCommandSQLParsingException("Character '=' was expected", text, pos);

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos + 1, word, false, " =><");
			if (pos == -1)
				throw new OCommandSQLParsingException("Value expected", text, pos);

			fieldValue = word.toString();

			if (fieldValue.endsWith(",")) {
				pos = newPos - 1;
				fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
			} else
				pos = newPos;

			// INSERT TRANSFORMED FIELD VALUE
			setEntries.put(fieldName, OSQLHelper.parseValue(database, this, fieldValue));

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (setEntries.size() == 0)
			throw new OCommandSQLParsingException("Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2",
					text, pos);

		return newPos;
	}

	private int parseRemoveFields(final StringBuilder word, int pos) {
		String fieldName;

		while (pos != -1 && (removeEntries.size() == 0 || word.toString().equals(","))) {
			pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (pos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);

			fieldName = word.toString();

			// INSERT FIELD NAME TO BE REMOVED
			removeEntries.add(fieldName);

			pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (removeEntries.size() == 0)
			throw new OCommandSQLParsingException("Field(s) to remove are missed. Example: name, salary", text, pos);
		return pos;
	}
}
