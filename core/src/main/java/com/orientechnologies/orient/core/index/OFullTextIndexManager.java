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
package com.orientechnologies.orient.core.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Fast index for full-text searches.
 * 
 * @author Luca Garulli
 * 
 */
public class OFullTextIndexManager {
	private static final String					DICTIONARY_NAME	= "FullTextIndex";

	private ODocument										persistentIndexes;
	private Map<String, OFullTextIndex>	memoryIndexes		= new HashMap<String, OFullTextIndex>();

	@SuppressWarnings("unchecked")
	public OFullTextIndexManager(final ODatabaseDocumentTx iDatabase) {

		// CREATE THE SCHEMA CLASS THE FIRST TIME
		if (!iDatabase.getMetadata().getSchema().existsClass(OFullTextIndexManager.class.getSimpleName())) {
			OClass cls = iDatabase.getMetadata().getSchema().createClass(OFullTextIndexManager.class.getSimpleName());
			cls.createProperty("indexes", OType.EMBEDDEDMAP);
			iDatabase.getMetadata().getSchema().save();
		}

		persistentIndexes = iDatabase.getDictionary().get(DICTIONARY_NAME);
		if (persistentIndexes == null) {
			persistentIndexes = new ODocument(iDatabase, OFullTextIndexManager.class.getSimpleName());
			persistentIndexes.field("indexes", new HashMap<String, ODocument>());
			iDatabase.getDictionary().put(DICTIONARY_NAME, persistentIndexes);

		} else {
			persistentIndexes.load();
			// ACTIVATE INDEXES IN MEMORY
			for (Entry<String, ODocument> entry : ((HashMap<String, ODocument>) persistentIndexes.field("indexes")).entrySet()) {
				memoryIndexes.put(entry.getKey(), new OFullTextIndex(iDatabase, entry.getKey(), entry.getValue()));
			}
		}
	}

	public OFullTextIndex getIndex(final String iName) {
		return memoryIndexes.get(iName);
	}

	@SuppressWarnings("unchecked")
	public OFullTextIndex addIndex(final OFullTextIndex iIndex) {
		if (memoryIndexes.containsKey(iIndex.getName()))
			throw new OIndexException("FullText index '" + iIndex.getName() + "' was already defined");

		((Map<String, ODocument>) persistentIndexes.field("indexes")).put(iIndex.getName(), iIndex.getConfiguration());
		persistentIndexes.setDirty();
		persistentIndexes.save();
		return memoryIndexes.put(iIndex.getName(), iIndex);
	}
}
