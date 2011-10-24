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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * SQL INSERT command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLInsert extends OCommandExecutorSQLAbstract {
	public static final String	KEYWORD_INSERT	= "INSERT";
	private static final String	KEYWORD_VALUES	= "VALUES";
	private static final String	KEYWORD_INTO		= "INTO";
	private String							className				= null;
	private String							clusterName			= null;
	private String							indexName				= null;
	private Map<String, Object>	fields;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLInsert parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		className = null;
		fields = null;

		StringBuilder word = new StringBuilder();

		int pos = OSQLHelper.nextWord(text, textUpperCase, 0, word, true);
		if (pos == -1 || !word.toString().equals(OCommandExecutorSQLInsert.KEYWORD_INSERT))
			throw new OCommandSQLParsingException("Keyword " + OCommandExecutorSQLInsert.KEYWORD_INSERT + " not found", text, 0);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_INTO))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_INTO + " not found", text, 0);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Invalid subject name. Expected cluster, class or index", text, pos);

		String subjectName = word.toString();

		if (subjectName.startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX))
			// CLUSTER
			clusterName = subjectName.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length());

		else if (subjectName.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX))
			// INDEX
			indexName = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());

		else {
			// CLASS
			if (subjectName.startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
				subjectName = subjectName.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

			final OClass cls = database.getMetadata().getSchema().getClass(subjectName);
			if (cls == null)
				throw new OCommandSQLParsingException("Class " + subjectName + " not found in database", text, pos);

			className = cls.getName();
		}

		final int beginFields = OStringParser.jumpWhiteSpaces(text, pos);
		if (beginFields == -1 || text.charAt(beginFields) != '(')
			throw new OCommandSQLParsingException("Set of fields is missed. Example: (name, surname)", text, pos);

		final int endFields = text.indexOf(')', beginFields + 1);
		if (endFields == -1)
			throw new OCommandSQLParsingException("Missed closed brace", text, beginFields);

		final ArrayList<String> fieldNames = new ArrayList<String>();
		OStringSerializerHelper.getParameters(text, beginFields, endFields, fieldNames);
		if (fieldNames.size() == 0)
			throw new OCommandSQLParsingException("Set of fields is empty. Example: (name, surname)", text, endFields);

		// REMOVE QUOTATION MARKS IF ANY
		for (int i = 0; i < fieldNames.size(); ++i)
			fieldNames.set(i, OStringSerializerHelper.removeQuotationMarks(fieldNames.get(i)));

		pos = OSQLHelper.nextWord(text, textUpperCase, endFields + 1, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_VALUES))
			throw new OCommandSQLParsingException("Missed VALUES keyword", text, endFields);

		final int beginValues = OStringParser.jumpWhiteSpaces(text, pos);
		if (pos == -1 || text.charAt(beginValues) != '(')
			throw new OCommandSQLParsingException("Set of values is missed. Example: ('Bill', 'Stuart', 300)", text, pos);

		final int endValues = text.lastIndexOf(')');
		if (endValues == -1)
			throw new OCommandSQLParsingException("Missed closed brace", text, beginValues);

		final List<String> values = OStringSerializerHelper.smartSplit(text, new char[] { ',' }, beginValues + 1, endValues - 1, true);

		if (values.size() == 0)
			throw new OCommandSQLParsingException("Set of values is empty. Example: ('Bill', 'Stuart', 300)", text, beginValues);

		if (values.size() != fieldNames.size())
			throw new OCommandSQLParsingException("Fields not match with values", text, beginValues);

		// TRANSFORM FIELD VALUES
		final Object[] fieldValues = new Object[values.size()];
		for (int i = 0; i < values.size(); ++i)
			fieldValues[i] = OSQLHelper.parseValue(database, this, OStringSerializerHelper.decode(values.get(i).trim()));

		fields = new LinkedHashMap<String, Object>();
		for (int i = 0; i < fieldNames.size(); ++i)
			fields.put(fieldNames.get(i), fieldValues[i]);

		return this;
	}

	/**
	 * Execute the INSERT and return the ODocument object created.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (fields == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		if (indexName != null) {
			final OIndex<?> index = database.getMetadata().getIndexManager().getIndex(indexName);
			if (index == null)
				throw new OCommandExecutionException("Target index '" + indexName + "' not found");

			// BIND VALUES
			index.put(fields.get(KEYWORD_KEY), (OIdentifiable) fields.get(KEYWORD_RID));
			return null;
		} else {
			// CREATE NEW DOCUMENT
			ODocument doc = className != null ? new ODocument(database, className) : new ODocument(database);

			OSQLHelper.bindParameters(doc, fields, iArgs);

			if (clusterName != null)
				doc.save(clusterName);
			else
				doc.save();
			return doc;
		}
	}
}
