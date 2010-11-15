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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
public class OPropertyIndexFullText extends OPropertyIndexMVRBTreeAbstract {
	private static final String	FIELD_MAP_RID				= "mapRid";
	private static final String	FIELD_CLUSTER_NAME	= "clusterName";
	private static final String	FIELD_STOP_WORDS		= "stopWords";
	private static final String	FIELD_IGNORE_CHARS	= "ignoreChars";

	private static String				DEF_CLUSTER_NAME		= "FullTextIndex";
	private static String				DEF_IGNORE_CHARS		= " \r\n\t:;,.|+*/\\=!?[]()'\"";
	private static String				DEF_STOP_WORDS			= "the in a at as and or for his her " + "him this that what which while "
																											+ "up with be was is";
	private String							ignoreChars;
	private Set<String>					stopWords;
	private ODocument						config;

	public OPropertyIndexFullText() {
	}

	public OPropertyIndexFullText(final ODatabaseRecord<?> iDatabase, final OProperty iProperty) {
		this(iDatabase, iProperty, DEF_CLUSTER_NAME);
	}

	public OPropertyIndexFullText(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final String iClusterIndexName) {
		create(iDatabase, iProperty, iClusterIndexName, null, DEF_IGNORE_CHARS, DEF_STOP_WORDS);
	}

	@Override
	public OPropertyIndex create(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final String iClusterIndexName,
			final OProgressListener iProgressListener) {
		return create(iDatabase, iProperty, iClusterIndexName, iProgressListener, DEF_IGNORE_CHARS, DEF_STOP_WORDS);
	}

	public OPropertyIndex create(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final String iClusterIndexName,
			final OProgressListener iProgressListener, final String iIgnoreChars, final String iStopWords) {
		if (iDatabase.getClusterIdByName(iClusterIndexName) == -1)
			// CREATE THE PHYSICAL CLUSTER THE FIRST TIME
			iDatabase.addPhysicalCluster(iClusterIndexName, iClusterIndexName, -1);

		ODatabaseComplex<?> db = iDatabase;
		while (db != null && !(db instanceof ODatabaseRecord<?>))
			db = db.getUnderlying();

		map = new OTreeMapDatabaseLazySave<String, List<ORecordId>>((ODatabaseRecord<?>) db, iClusterIndexName,
				OStreamSerializerString.INSTANCE, OStreamSerializerListRID.INSTANCE);
		map.lazySave();

		config = new ODocument(iDatabase);
		config.field(FIELD_IGNORE_CHARS, iIgnoreChars);
		config.field(FIELD_STOP_WORDS, iStopWords);
		config.field(FIELD_CLUSTER_NAME, iClusterIndexName);
		config.field(FIELD_MAP_RID, map.getRecord().getIdentity().toString());
		config.save();

		init();

		return this;
	}

	/**
	 * Configure the index to be loaded.
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 * @param iClusterIndexName
	 *          Cluster name where to place the TreeMap
	 * @param iRecordId
	 *          Record Id of the persistent TreeMap
	 */
	@Override
	public OPropertyIndex configure(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final ORID iRID) {
		owner = iProperty;
		config = new ODocument(iDatabase, iRID);
		config.load();

		init(iDatabase, new ORecordId((String) config.field(FIELD_MAP_RID)));
		init();

		return this;
	}

	/**
	 * Index an entire document field by field and save the index at the end.
	 * 
	 * @param iDocument
	 *          The document to index
	 */
	public void indexDocument(final ODocument iDocument) {
		Object fieldValue;

		for (String fieldName : iDocument.fieldNames()) {
			fieldValue = iDocument.field(fieldName);
			put(fieldValue, (ORecordId) iDocument.getIdentity());
		}

		try {
			map.save();
		} catch (IOException e) {
			throw new OIndexException("Can't save index for property '" + owner.getName() + "'");
		}
	}

	/**
	 * Index a value and save the index.
	 * 
	 * @param iDocument
	 *          The document to index
	 */
	public void put(final Object iKey, final ORecordId iSingleValue) {
		indexValue(iKey, iSingleValue);
	}

	/**
	 * Split the value in single words and index each one. Save of the index is responsability of the caller.
	 * 
	 * @param iKey
	 *          Value to index
	 * @param iOwnerRecord
	 *          ORecordId of the owner record
	 */
	private void indexValue(final Object iKey, final ORecordId iOwnerRecord) {
		if (iKey == null)
			return;

		List<ORecordId> refs;
		final StringBuilder buffer = new StringBuilder();
		char c;
		boolean ignore;

		// GET ALL THE WORDS OF THE STRING
		final List<String> words = OStringSerializerHelper.split(iKey.toString(), ' ');

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
			refs.add(iOwnerRecord);

			// SAVE THE INDEX ENTRY
			map.put(word, refs);
		}
	}

	public ODocument getConfiguration() {
		return config;
	}

	public INDEX_TYPE getType() {
		return INDEX_TYPE.FULLTEXT;
	}

	@Override
	public ORID getIdentity() {
		return config.getIdentity();
	}

	private void init() {
		ignoreChars = (String) config.field(FIELD_IGNORE_CHARS);
		stopWords = new HashSet<String>(OStringSerializerHelper.split((String) config.field(FIELD_STOP_WORDS), ' '));
	}
}
