/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAll;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAny;

/**
 * Executes a TRAVERSE crossing records. Returns a List<OIdentifiable> containing all the traversed records that match the WHERE
 * condition.
 * <p>
 * SYNTAX: <code>TRAVERSE <field>* FROM <target> WHERE <condition></code>
 * </p>
 * <p>
 * In the command context you've access to the variable $depth containing the depth level from the root node. This is useful to
 * limit the traverse up to a level. For example to consider from the first depth level (0 is root node) to the third use:
 * <code>TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3</code>. To filter traversed records use it combined with a SELECT
 * statement:
 * </p>
 * <p>
 * <code>SELECT FROM (TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3) WHERE city.name = 'Rome'</code>
 * </p>
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLTraverse extends OCommandExecutorSQLExtractAbstract {
	public static final String	KEYWORD_TRAVERSE	= "TRAVERSE";
	private Set<String>					fields;

	/**
	 * Compile the filter conditions only the first time.
	 */
	public OCommandExecutorSQLTraverse parse(final OCommandRequestText iRequest) {
		super.parse(iRequest);

		final int pos = parseFields();
		if (pos == -1)
			throw new OCommandSQLParsingException("Traverse must have the field list. Use " + getSyntax());

		int endPosition = text.length();
		int endP = textUpperCase.indexOf(" " + OCommandExecutorSQLTraverse.KEYWORD_LIMIT, currentPos);
		if (endP > -1 && endP < endPosition)
			endPosition = endP;

		compiledFilter = OSQLEngine.getInstance().parseFromWhereCondition(text.substring(pos, endPosition), context);

		optimize();

		currentPos = compiledFilter.currentPos < 0 ? endPosition : compiledFilter.currentPos + pos;

		if (currentPos > -1 && currentPos < text.length()) {
			currentPos = OStringParser.jump(text, currentPos, " \r\n");

			final StringBuilder word = new StringBuilder();
			String w;

			while (currentPos > -1) {
				currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

				if (currentPos > -1) {
					w = word.toString();
					if (w.equals(KEYWORD_LIMIT))
						parseLimit(word);
				}
			}
		}
		if (limit == 0 || limit < -1) {
			throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
		}
		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		if (!assignTarget(iArgs))
			throw new OQueryParsingException("No source found in query: specify class, cluster(s) or single record(s)");

		executeTraverse();

		applyLimit();

		return handleResult();
	}

	protected void executeTraverse() {
		if (target == null)
			throw new OCommandExecutionException("Traverse error: target not specified");

		context = new OTraverseContext();
		// BROWSE ALL THE RECORDS
		for (OIdentifiable id : target) {
			traverse(id, compiledFilter.getRootCondition());
		}
	}

	private void traverse(Object iTarget, final OSQLFilterCondition iCondition) {
		if (!(iTarget instanceof OIdentifiable))
			// JUMP IT BECAUSE IT ISN'T A RECORD
			return;

		final ORecord<?> record = ((OIdentifiable) iTarget).getRecord();
		if (!(record instanceof ODocument))
			// JUMP IT BECAUSE NOT ODOCUMENT
			return;

		final ODocument target = (ODocument) record;

		if (((OTraverseContext) context).traversed.contains(target.getIdentity()))
			// ALREADY EVALUATED, DON'T GO IN DEEP
			return;

		if (target.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
			try {
				target.load();
			} catch (final ORecordNotFoundException e) {
				// INVALID RID
				return;
			}

		if (iCondition != null) {
			final Object conditionResult = iCondition.evaluate(target, context);
			if (conditionResult != Boolean.TRUE)
				return;
		}

		// MATCH
		addResult(target);

		// UPDATE CONTEXT
		((OTraverseContext) context).traversed.add(target.getIdentity());
		((OTraverseContext) context).history.add(target.getIdentity());

		try {
			// TRAVERSE THE DOCUMENT ITSELF
			for (String cfgField : fields) {
				if ("*".equals(cfgField) || OSQLFilterItemFieldAll.FULL_NAME.equals(cfgField)
						|| OSQLFilterItemFieldAny.FULL_NAME.equals(cfgField)) {
					// ALL FIELDS
					for (final String fieldName : target.fieldNames())
						traverseField(target, fieldName, iCondition);
				} else {
					final int pos = cfgField.indexOf('.');
					if (pos > -1) {
						// FOUND <CLASS>.<FIELD>
						final OClass cls = target.getSchemaClass();
						if (cls == null)
							// JUMP IT BECAUSE NO SCHEMA
							continue;

						final String className = cfgField.substring(0, pos);
						if (!cls.isSubClassOf(className))
							// JUMP IT BECAUSE IT'S NOT A INSTANCEOF THE CLASS
							continue;

						cfgField = cfgField.substring(pos + 1);
					}

					traverseField(target, cfgField, iCondition);
				}
			}

		} finally {
			((OTraverseContext) context).history.remove(((OTraverseContext) context).history.size() - 1);
		}
	}

	protected void traverseField(final ODocument iDocument, final String iFieldName, final OSQLFilterCondition iCondition) {
		final Object fieldValue = iDocument.rawField(iFieldName);
		if (fieldValue == null)
			return;

		((OTraverseContext) context).path.add(iFieldName);
		try {

			if (OMultiValue.isMultiValue(fieldValue))
				for (Object o : OMultiValue.getMultiValueIterable(fieldValue)) {
					traverse(o, iCondition);
				}
			else if (fieldValue instanceof OIdentifiable)
				traverse(fieldValue, iCondition);

		} finally {
			((OTraverseContext) context).path.remove(((OTraverseContext) context).path.size() - 1);
		}
	}

	protected int parseFields() {
		int currentPos = 0;
		final StringBuilder word = new StringBuilder();

		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
		if (!word.toString().equals(KEYWORD_TRAVERSE))
			return -1;

		int fromPosition = textUpperCase.indexOf(KEYWORD_FROM_2FIND, currentPos);
		if (fromPosition == -1)
			throw new OQueryParsingException("Missed " + KEYWORD_FROM, text, currentPos);

		final String fieldString = text.substring(currentPos, fromPosition).trim();
		if (fieldString.length() > 0) {
			// EXTRACT PROJECTIONS
			fields = new HashSet<String>();
			final List<String> items = OStringSerializerHelper.smartSplit(fieldString, ',');

			for (String field : items)
				fields.add(field.trim());
		} else
			throw new OQueryParsingException("Missed field list to cross in TRAVERSE. Use " + getSyntax(), text, currentPos);

		currentPos = fromPosition + KEYWORD_FROM.length() + 1;

		return currentPos;
	}

	public String getSyntax() {
		return "TRAVERSE <field>* FROM <target> [WHERE <filter>]";
	}
}
