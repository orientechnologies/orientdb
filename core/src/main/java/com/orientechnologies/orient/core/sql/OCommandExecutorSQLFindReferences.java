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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * FIND REFERENCES command: Finds references to records in all or part of database
 * 
 * @author Luca Molino
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLFindReferences extends OCommandExecutorSQLAbstract {
	public static final String	KEYWORD_FIND				= "FIND";
	public static final String	KEYWORD_REFERENCES	= "REFERENCES";

	private Set<ORID>						recordIds						= new HashSet<ORID>();
	private String							classList;
	private StringBuilder				subQuery;

	public OCommandExecutorSQLFindReferences parse(final OCommandRequestText iRequest) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

		init(iRequest.getText());

		final StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_FIND))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_FIND + " not found. Use " + getSyntax(), text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_REFERENCES))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_REFERENCES + " not found. Use " + getSyntax(), text, oldPos);

		pos = OStringParser.jumpWhiteSpaces(text, pos);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <target>. Use " + getSyntax(), text, oldPos);

		oldPos = pos;
		if (text.charAt(pos) == '(') {
			subQuery = new StringBuilder();
			pos = OStringSerializerHelper.getEmbedded(text, oldPos, -1, subQuery);
		} else {
			pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
			if (pos == -1)
				throw new OCommandSQLParsingException("Expected <recordId>. Use " + getSyntax(), text, oldPos);

			final String recordIdString = word.toString();
			if (recordIdString == null || recordIdString.equals(""))
				throw new OCommandSQLParsingException("Record to search cannot be null. Use " + getSyntax(), text, pos);
			try {
				final ORecordId rid = new ORecordId(recordIdString);
				if (!rid.isValid())
					throw new OCommandSQLParsingException("Record ID " + recordIdString + " is not valid", text, pos);
				recordIds.add(rid);

			} catch (IllegalArgumentException iae) {
				throw new OCommandSQLParsingException("Error reading record Id", text, pos, iae);
			}
		}

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos != -1) {
			// GET THE CLUSTER LIST TO SEARCH, IF NULL WILL SEARCH ENTIRE DATABASE
			classList = word.toString().trim();
			if (!classList.startsWith("[") || !classList.endsWith("]")) {
				throw new OCommandSQLParsingException("Class list must be contained in []. Use " + getSyntax(), text, pos);
			}
			classList = classList.substring(1, classList.length() - 1);
		}

		return this;
	}

	/**
	 * Execute the FIND REFERENCES.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (recordIds.isEmpty() && subQuery == null)
			throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

		if (subQuery != null) {
			final List<OIdentifiable> result = new OCommandSQL(subQuery.toString()).execute();
			for (OIdentifiable id : result)
				recordIds.add(id.getIdentity());
		}

		return OFindReferenceHelper.findReferences(recordIds, classList);
	}

	@Override
	public String getSyntax() {
		return "FIND REFERENCES <rid|<sub-query>> [class-list]";
	}
}
