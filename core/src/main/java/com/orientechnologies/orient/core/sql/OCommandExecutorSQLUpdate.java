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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLUpdate extends OCommandExecutorSQLAbstract implements OCommandResultListener {
	public static final String									KEYWORD_UPDATE	= "UPDATE";
	private static final String									KEYWORD_SET			= "SET";
	private static final String									KEYWORD_ADD			= "ADD";
	private static final String									KEYWORD_PUT			= "PUT";
	private static final String									KEYWORD_REMOVE	= "REMOVE";
	private Map<String, Object>									setEntries			= new LinkedHashMap<String, Object>();
	private Map<String, Object>									addEntries			= new LinkedHashMap<String, Object>();
	private Map<String, OPair<String, Object>>	putEntries			= new LinkedHashMap<String, OPair<String, Object>>();
	private Map<String, Object>									removeEntries		= new LinkedHashMap<String, Object>();
	private OQuery<?>														query;
	private int																	recordCount			= 0;
	private String															subjectName;
	private static final Object									EMPTY_VALUE			= new Object();
	private int																	fieldCounter		= 0;
	private Map<Object, Object>									parameters;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLUpdate parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_UPDATE);

		init(iRequest.getDatabase(), iRequest.getText());

		setEntries.clear();
		query = null;
		recordCount = 0;

		final StringBuilder word = new StringBuilder();

		int pos = OSQLHelper.nextWord(text, textUpperCase, 0, word, true);
		if (pos == -1 || !word.toString().equals(OCommandExecutorSQLUpdate.KEYWORD_UPDATE))
			throw new OCommandSQLParsingException("Keyword " + OCommandExecutorSQLUpdate.KEYWORD_UPDATE + " not found", text, 0);

		int newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (newPos == -1)
			throw new OCommandSQLParsingException("Invalid target", text, pos);

		pos = newPos;

		subjectName = word.toString();

		newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (newPos == -1
				|| (!word.toString().equals(KEYWORD_SET) && !word.toString().equals(KEYWORD_ADD) && !word.toString().equals(KEYWORD_PUT) && !word
						.toString().equals(KEYWORD_REMOVE)))
			throw new OCommandSQLParsingException("Expected keyword " + KEYWORD_SET + "," + KEYWORD_ADD + "," + KEYWORD_PUT + " or "
					+ KEYWORD_REMOVE, text, pos);

		pos = newPos;

		while (pos != -1 && !word.toString().equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE)) {
			if (word.toString().equals(KEYWORD_SET))
				pos = parseSetFields(word, pos);
			else if (word.toString().equals(KEYWORD_ADD))
				pos = parseAddFields(word, pos);
			else if (word.toString().equals(KEYWORD_PUT))
				pos = parsePutFields(word, pos);
			else if (word.toString().equals(KEYWORD_REMOVE))
				pos = parseRemoveFields(word, pos);
			else
				break;
		}

		String whereCondition = word.toString();

		if (whereCondition.equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE))
			query = new OSQLAsynchQuery<ODocument>("select from " + subjectName + " where " + text.substring(pos), this);
		else
			query = new OSQLAsynchQuery<ODocument>("select from " + subjectName, this);

		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		if (subjectName == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		parameters = iArgs;

		database.query(query, iArgs);
		return recordCount;
	}

	/**
	 * Update current record.
	 */
	@SuppressWarnings("unchecked")
	public boolean result(final Object iRecord) {
		final ODocument record = (ODocument) iRecord;

		boolean recordUpdated = false;

		// BIND VALUES TO UPDATE
		Object v;

		if (setEntries.size() > 0) {
			OSQLHelper.bindParameters(record, setEntries, parameters);
			recordUpdated = true;
		}

		// BIND VALUES TO ADD
		Collection<Object> coll;
		Object fieldValue;
		for (Map.Entry<String, Object> entry : addEntries.entrySet()) {
			coll = null;
			if (!record.containsField(entry.getKey())) {
				// GET THE TYPE IF ANY
				if (record.getSchemaClass() != null) {
					OProperty prop = record.getSchemaClass().getProperty(entry.getKey());
					if (prop != null && prop.getType() == OType.LINKSET)
						// SET TYPE
						coll = new HashSet<Object>();
				}

				if (coll == null)
					// IN ALL OTHER CASES USE A LIST
					coll = new ArrayList<Object>();

				record.field(entry.getKey(), coll);
			} else {
				fieldValue = record.field(entry.getKey());

				if (fieldValue instanceof Collection<?>)
					coll = (Collection<Object>) fieldValue;
				else
					continue;
			}

			v = entry.getValue();

			if (v instanceof OSQLFilterItem)
				v = ((OSQLFilterItem) v).getValue(record);
			else if (v instanceof OSQLFunctionRuntime)
				v = ((OSQLFunctionRuntime) v).execute(record);

			coll.add(v);
			recordUpdated = true;
		}

		// BIND VALUES TO PUT (AS MAP)
		Map<String, Object> map;
		OPair<String, Object> pair;
		for (Entry<String, OPair<String, Object>> entry : putEntries.entrySet()) {
			fieldValue = record.field(entry.getKey());

			if (fieldValue == null) {
				if (record.getSchemaClass() != null) {
					final OProperty property = record.getSchemaClass().getProperty(entry.getKey());
					if (property != null
							&& (property.getType() != null && (!property.getType().equals(OType.EMBEDDEDMAP) && !property.getType().equals(
									OType.LINKMAP)))) {
						throw new OCommandExecutionException("field " + entry.getKey() + " is not defined as a map");
					}
				}
				fieldValue = new HashMap();
				record.field(entry.getKey(), fieldValue);
			}

			if (fieldValue instanceof Map<?, ?>) {
				map = (Map<String, Object>) fieldValue;

				pair = entry.getValue();

				if (pair.getValue() instanceof OSQLFilterItem)
					pair.setValue(((OSQLFilterItem) pair.getValue()).getValue(record));
				else if (pair.getValue() instanceof OSQLFunctionRuntime)
					v = ((OSQLFunctionRuntime) pair.getValue()).execute(record);

				map.put(pair.getKey(), pair.getValue());
				recordUpdated = true;
			}
		}

		// REMOVE FIELD IF ANY
		for (Map.Entry<String, Object> entry : removeEntries.entrySet()) {
			v = entry.getValue();
			if (v == EMPTY_VALUE) {
				record.removeField(entry.getKey());
				recordUpdated = true;
			} else {
				fieldValue = record.field(entry.getKey());

				if (fieldValue instanceof Collection<?>) {
					coll = (Collection<Object>) fieldValue;
					if (coll.remove(v))
						recordUpdated = true;
				} else if (fieldValue instanceof Map<?, ?>) {
					map = (Map<String, Object>) fieldValue;
					if (map.remove(v) != null)
						recordUpdated = true;
				}
			}
		}

		if (recordUpdated) {
			record.setDirty();
			record.save();
			recordCount++;
		}

		return true;
	}

	private int parseSetFields(final StringBuilder word, int pos) {
		String fieldName;
		String fieldValue;
		int newPos = pos;

		while (pos != -1 && (setEntries.size() == 0 || word.toString().equals(","))) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (newPos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);
			pos = newPos;

			fieldName = word.toString();

			newPos = OStringParser.jumpWhiteSpaces(text, pos);

			if (newPos == -1 || text.charAt(newPos) != '=')
				throw new OCommandSQLParsingException("Character '=' was expected", text, pos);

			pos = newPos;
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

			pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (setEntries.size() == 0)
			throw new OCommandSQLParsingException("Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2",
					text, pos);

		return pos;
	}

	private int parseAddFields(final StringBuilder word, int pos) {
		String fieldName;
		String fieldValue;
		int newPos = pos;

		while (pos != -1 && (setEntries.size() == 0 || word.toString().equals(",")) && !word.toString().equals(KEYWORD_WHERE)) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (newPos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);
			pos = newPos;

			fieldName = word.toString();

			newPos = OStringParser.jumpWhiteSpaces(text, pos);

			if (newPos == -1 || text.charAt(newPos) != '=')
				throw new OCommandSQLParsingException("Character '=' was expected", text, pos);

			pos = newPos;
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
			addEntries.put(fieldName, OSQLHelper.parseValue(database, this, fieldValue));

			pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (addEntries.size() == 0)
			throw new OCommandSQLParsingException("Entries to add <field> = <value> are missed. Example: name = 'Bill', salary = 300.2",
					text, pos);

		return pos;
	}

	private int parsePutFields(final StringBuilder word, int pos) {
		String fieldName;
		String fieldKey;
		String fieldValue;
		int newPos = pos;

		while (pos != -1 && (setEntries.size() == 0 || word.toString().equals(",")) && !word.toString().equals(KEYWORD_WHERE)) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (newPos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);
			pos = newPos;

			fieldName = word.toString();

			newPos = OStringParser.jumpWhiteSpaces(text, pos);

			if (newPos == -1 || text.charAt(newPos) != '=')
				throw new OCommandSQLParsingException("Character '=' was expected", text, pos);

			pos = newPos;
			newPos = OSQLHelper.nextWord(text, textUpperCase, pos + 1, word, false, " =><,");
			if (pos == -1)
				throw new OCommandSQLParsingException("Key expected", text, pos);

			fieldKey = word.toString();

			if (fieldKey.endsWith(",")) {
				pos = newPos + 1;
				fieldKey = fieldKey.substring(0, fieldKey.length() - 1);
			} else {
				pos = newPos;

				newPos = OStringParser.jumpWhiteSpaces(text, pos);
				if (newPos == -1 || text.charAt(pos) != ',')
					throw new OCommandSQLParsingException("',' expected", text, pos);

				pos = newPos;
			}

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos + 1, word, false, " =><,");
			if (pos == -1)
				throw new OCommandSQLParsingException("Value expected", text, pos);

			fieldValue = word.toString();

			if (fieldValue.endsWith(",")) {
				pos = newPos - 1;
				fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
			} else
				pos = newPos;

			// INSERT TRANSFORMED FIELD VALUE
			putEntries.put(
					fieldName,
					new OPair<String, Object>((String) OSQLHelper.parseValue(database, this, fieldKey), OSQLHelper.parseValue(database, this,
							fieldValue)));

			pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (putEntries.size() == 0)
			throw new OCommandSQLParsingException("Entries to put <field> = <key>, <value> are missed. Example: name = 'Bill', 30", text,
					pos);

		return pos;
	}

	private int parseRemoveFields(final StringBuilder word, int pos) {
		String fieldName;
		String fieldValue;
		Object value;
		int newPos = pos;

		while (pos != -1 && (removeEntries.size() == 0 || word.toString().equals(",")) && !word.toString().equals(KEYWORD_WHERE)) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (newPos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);

			fieldName = word.toString();

			pos = OStringParser.jumpWhiteSpaces(text, newPos);

			if (pos > -1 && text.charAt(pos) == '=') {
				pos = OSQLHelper.nextWord(text, textUpperCase, pos + 1, word, false, " =><,");
				if (pos == -1)
					throw new OCommandSQLParsingException("Value expected", text, pos);

				fieldValue = word.toString();

				if (fieldValue.endsWith(",")) {
					pos = newPos - 1;
					fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
				} else
					pos = newPos;

				if (fieldValue.length() > 2 && Character.isDigit(fieldValue.charAt(0)) && fieldValue.contains(":"))
					value = new ORecordId(fieldValue);
				else
					value = fieldValue;

			} else
				value = EMPTY_VALUE;

			// INSERT FIELD NAME TO BE REMOVED
			removeEntries.put(fieldName, value);

			pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (removeEntries.size() == 0)
			throw new OCommandSQLParsingException("Field(s) to remove are missed. Example: name, salary", text, pos);
		return pos;
	}
}
