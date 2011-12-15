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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.object.OLazyObjectList;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.object.OLazyObjectSet;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * SQL CREATE INDEX command: Create a new index against a property.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLFindReferences extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_FIND				= "FIND";
	public static final String	KEYWORD_REFERENCES	= "REFERENCES";

	private ORID								recordId;
	private String							classList;

	public OCommandExecutorSQLFindReferences parse(final OCommandRequestText iRequest) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

		init(iRequest.getText());

		final StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_FIND))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_FIND + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_REFERENCES))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_REFERENCES + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <recordId>", text, oldPos);

		final String recordIdString = word.toString();
		if (recordIdString == null || recordIdString.equals(""))
			throw new OCommandSQLParsingException("Record to search cannot be null", text, pos);
		try {
			recordId = new ORecordId(recordIdString);
			if (!recordId.isValid())
				throw new OCommandSQLParsingException("Record ID " + recordIdString + " is not valid", text, pos);
		} catch (IllegalArgumentException iae) {
			throw new OCommandSQLParsingException("Error reading record Id", text, pos, iae);
		}

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos != -1) {
			// GET THE CLUSTER LIST TO SEARCH, IF NULL WILL SEARCH ENTIRE DATABASE
			classList = word.toString().trim();
			if (!classList.startsWith("[") || !classList.endsWith("]")) {
				throw new OCommandSQLParsingException("Class list must be contained in []", text, pos);
			}
			classList = classList.substring(1, classList.length() - 1);
		}

		return this;
	}

	/**
	 * Execute the FIND REFERENCES.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (recordId == null)
			throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

		final ODatabaseRecord database = getDatabase();

		final Set<ORID> result = new HashSet<ORID>();
		if (classList == null || classList.equals("")) {
			for (String clusterName : database.getClusterNames()) {
				browseCluster(clusterName, result);
			}
		} else {
			List<String> classes = OStringSerializerHelper.smartSplit(classList, ',');
			for (String clazz : classes) {
				if (clazz.startsWith("CLUSTER:")) {
					browseCluster(clazz.substring(clazz.indexOf("CLUSTER:") + "CLUSTER:".length()), result);
				} else {
					browseClass(clazz, result);
				}
			}
		}

		return new ArrayList<ORID>(result);
	}

	private void browseCluster(final String iClusterName, final Set<ORID> ids) {
		final ODatabaseRecord database = getDatabase();
		for (ORecordInternal<?> record : database.browseCluster(iClusterName)) {
			if (record instanceof ODocument) {
				try {
					for (String fieldName : ((ODocument) record).fieldNames()) {
						Object value = ((ODocument) record).field(fieldName);
						checkObject(ids, value, (ODocument) record);
					}
				} catch (Exception e) {
					OLogManager.instance().error(this, "Error reading record " + record.getIdentity(), e);
				}
			}
		}
	}

	private void browseClass(final String iClassName, final Set<ORID> ids) {
		final ODatabaseRecord database = getDatabase();
		final OClass clazz = database.getMetadata().getSchema().getClass(iClassName);

		if (clazz == null)
			throw new OCommandExecutionException("Class '" + iClassName + "' was not found");

		for (int i : clazz.getClusterIds()) {
			browseCluster(database.getClusterNameById(i), ids);
		}
	}

	private void checkObject(final Set<ORID> ids, final Object value, final ODocument iRootObject) {
		if (value instanceof OIdentifiable) {
			checkDocument(ids, (OIdentifiable) value, iRootObject);
		} else if (value instanceof Collection<?>) {
			checkCollection(ids, (Collection<?>) value, iRootObject);
		} else if (value instanceof Map<?, ?>) {
			checkMap(ids, (Map<?, ?>) value, iRootObject);
		}
	}

	private void checkDocument(final Set<ORID> ids, final OIdentifiable value, final ODocument iRootObject) {
		if (value.getIdentity().equals(recordId)) {
			ids.add(iRootObject.getIdentity());
		}
	}

	private void checkCollection(final Set<ORID> ids, final Collection<?> values, final ODocument iRootObject) {
		final Iterator<?> it;
		if (values instanceof OLazyObjectList) {
			((OLazyObjectList<?>) values).setConvertToRecord(false);
			it = ((OLazyObjectList<?>) values).listIterator();
		} else if (values instanceof OLazyObjectSet) {
			((OLazyObjectSet<?>) values).setConvertToRecord(false);
			it = ((OLazyObjectSet<?>) values).iterator();
		} else if (values instanceof ORecordLazyList) {
			it = ((ORecordLazyList) values).rawIterator();
		} else if (values instanceof OMVRBTreeRIDSet) {
			it = ((OMVRBTreeRIDSet) values).iterator();
		} else {
			it = values.iterator();
		}
		while (it.hasNext()) {
			checkObject(ids, it.next(), iRootObject);
		}
	}

	private void checkMap(final Set<ORID> ids, final Map<?, ?> values, final ODocument iRootObject) {
		final Iterator<?> it;
		if (values instanceof OLazyObjectMap) {
			((OLazyObjectMap<?>) values).setConvertToRecord(false);
			it = ((OLazyObjectMap<?>) values).values().iterator();
		} else if (values instanceof ORecordLazyMap) {
			it = ((ORecordLazyMap) values).rawIterator();
		} else {
			it = values.values().iterator();
		}
		while (it.hasNext()) {
			checkObject(ids, it.next(), iRootObject);
		}
	}
}
