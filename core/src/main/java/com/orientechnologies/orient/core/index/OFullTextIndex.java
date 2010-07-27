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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.type.tree.OTreeMapDatabaseLazySave;

/**
 * Fast index for full-text searches.
 * 
 * @author Luca Garulli
 * 
 */
public class OFullTextIndex implements Iterable<Entry<String, List<ORecordId>>> {
	private static final String																FIELD_NAME					= "name";
	private static final String																FIELD_MAP_RID				= "mapRid";
	private static final String																FIELD_CLUSTER_NAME	= "clusterName";
	private static final String																FIELD_STOP_WORDS		= "stopWords";
	private static final String																FIELD_IGNORE_CHARS	= "ignoreChars";

	private static String																			DEF_CLUSTER_NAME		= "FullTextIndex";
	private static String																			DEF_IGNORE_CHARS		= " \r\n\t:;,.|+*/\\=!?[]()'\"";
	private static String																			DEF_STOP_WORDS			= "the in a at as and or for his her "
																																										+ "him this that what which while "
																																										+ "up with be was is";
	private OTreeMapDatabaseLazySave<String, List<ORecordId>>	map;
	private boolean																						active							= false;
	private String																						ignoreChars;
	private Set<String>																				stopWords;
	private ODocument																					config;

	public OFullTextIndex(final ODatabaseDocumentTx iDatabase, final String iIndexName) {
		this(iDatabase, iIndexName, DEF_CLUSTER_NAME);
	}

	public OFullTextIndex(final ODatabaseDocumentTx iDatabase, final String iIndexName, final String iClusterIndexName) {
		this(iDatabase, iIndexName, iClusterIndexName, DEF_IGNORE_CHARS, DEF_STOP_WORDS);
	}

	public OFullTextIndex(final ODatabaseDocumentTx iDatabase, final String iIndexName, final String iClusterIndexName,
			final String iIgnoreChars, final String iStopWords) {
		if (iDatabase.getClusterIdByName(iClusterIndexName) == -1)
			// CREATE THE PHYSICAL CLUSTER THE FIRST TIME
			iDatabase.addPhysicalCluster(iClusterIndexName);

		map = new OTreeMapDatabaseLazySave<String, List<ORecordId>>((ODatabaseRecord<?>) iDatabase.getUnderlying(), iClusterIndexName,
				OStreamSerializerString.INSTANCE, OStreamSerializerListRID.INSTANCE);
		map.lazySave();

		config = new ODocument(iDatabase);
		config.field(FIELD_NAME, iIndexName);
		config.field(FIELD_IGNORE_CHARS, iIgnoreChars);
		config.field(FIELD_STOP_WORDS, iStopWords);
		config.field(FIELD_CLUSTER_NAME, iClusterIndexName);
		config.field(FIELD_MAP_RID, map.getRecord().getIdentity().toString());

		init();
	}

	/**
	 * Constructor used in load.
	 * 
	 * @param iDatabase
	 * @param iClusterIndexName
	 * @param iRecordId
	 */
	public OFullTextIndex(final ODatabaseDocumentTx iDatabase, final String iIndexName, final ODocument iIndexConfig) {
		config = iIndexConfig;
		map = new OTreeMapDatabaseLazySave<String, List<ORecordId>>((ODatabaseRecord<?>) iDatabase.getUnderlying(),
				(String) iIndexConfig.field(FIELD_CLUSTER_NAME), new ORecordId((String) iIndexConfig.field(FIELD_MAP_RID)));
		try {
			map.load();
		} catch (IOException e) {
			throw new OIndexException("Can't load tree map for Full Text index '" + iIndexName + "'");
		}
		init();
	}

	public void indexDocument(final ODocument iDocument) {
		Object fieldValue;
		List<ORecordId> refs;
		final StringBuilder buffer = new StringBuilder();
		char c;
		boolean ignore;

		for (String fieldName : iDocument.fieldNames()) {
			fieldValue = iDocument.field(fieldName);

			if (fieldValue != null) {
				// GET ALL THE WORDS OF THE STRING
				final List<String> words = OStringSerializerHelper.split(fieldValue.toString(), ' ');

				// FOREACH WORD CREATE THE LINK TO THE CURRENT DOCUMENT
				for (String word : words) {
					buffer.setLength(0);

					for (int i = 0; i < word.length(); ++i) {
						c = word.charAt(i);
						ignore = false;
						for (int k = 0; k < ignoreChars.length(); ++k)
							if (c == ignoreChars.charAt(k)) {
								ignore = true;
								break;
							}

						if (!ignore)
							buffer.append(c);
					}

					word = buffer.toString();

					// CHECK IF IT'S A STOP WORD
					if (stopWords.contains(word))
						continue;

					// SEARCH FOR THE WORD
					refs = map.get(word);
					if (refs == null)
						// WORD NOT EXISTS: CREATE THE KEYWORD CONTAINER THE FIRST TIME THE WORD IS FOUND
						refs = new ArrayList<ORecordId>();

					// ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
					refs.add((ORecordId) iDocument.getIdentity());

					// SAVE THE INDEX ENTRY
					map.put(word, refs);
				}
			}
		}

		try {
			map.save();
		} catch (IOException e) {
			throw new OIndexException("Can't save tree map for Full Text index '" + config.field(FIELD_NAME) + "'");
		}
	}

	public String getName() {
		return config.field(FIELD_NAME);
	}

	public boolean isActive() {
		return active;
	}

	public void setActivated(boolean iActive) {
		this.active = iActive;

		if (iActive)
			try {
				map.load();
			} catch (IOException e) {
				throw new OIndexException("Can't activate index on property");
			}
	}

	public void load() throws IOException {
		map.load();
	}

	public void clear() {
		if (!active)
			return;
		map.clear();
	}

	public void lazySave() {
		if (!active)
			return;
		map.lazySave();
	}

	public void remove(Object key) {
		if (!active)
			return;
		map.remove(key);
	}

	public void put(final String iKey, final ORecordId iSingleValue) {
		if (!active)
			return;

		List<ORecordId> values = map.get(iKey);
		if (values == null)
			values = new ArrayList<ORecordId>();

		// NOT UNIQUE
		int pos = values.indexOf(iSingleValue);
		if (pos > -1)
			// REPLACE IT
			values.set(pos, iSingleValue);
		else
			values.add(iSingleValue);

		map.put(iKey, values);
	}

	@SuppressWarnings("unchecked")
	public List<ORecordId> get(String iKey) {
		if (!active)
			return null;

		final List<ORecordId> values = map.get(iKey);

		if (values == null)
			return Collections.EMPTY_LIST;

		return values;
	}

	public ORecordBytes getRecord() {
		return map.getRecord();
	}

	public ODocument getConfiguration() {
		return config;
	}

	private void init() {
		ignoreChars = (String) config.field(FIELD_IGNORE_CHARS);
		stopWords = new HashSet<String>(OStringSerializerHelper.split((String) config.field(FIELD_STOP_WORDS), ' '));
	}

	public Iterator<Entry<String, List<ORecordId>>> iterator() {
		return map.entrySet().iterator();
	}
}
