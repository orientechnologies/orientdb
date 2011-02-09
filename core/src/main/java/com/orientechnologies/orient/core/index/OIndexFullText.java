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
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabaseLazySave;

/**
 * Fast index for full-text searches.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexFullText extends OIndexMVRBTreeAbstract {
	private static final String	CONFIG_STOP_WORDS		= "stopWords";
	private static final String	CONFIG_IGNORE_CHARS	= "ignoreChars";

	private static String				DEF_CLUSTER_NAME		= "FullTextIndex";
	private static String				DEF_IGNORE_CHARS		= " \r\n\t:;,.|+*/\\=!?[]()'\"";
	private static String				DEF_STOP_WORDS			= "the in a at as and or for his her " + "him this that what which while "
																											+ "up with be was is";
	private String							ignoreChars					= DEF_IGNORE_CHARS;
	private Set<String>					stopWords;

	public OIndexFullText() {
		super("FULLTEXT");
		stopWords = new HashSet<String>(OStringSerializerHelper.split(DEF_STOP_WORDS, ' '));
	}

	public OIndexFullText(final String iName, final ODatabaseRecord iDatabase, final int[] iClusterIdsToIndex,
			final boolean iAutomatic) {
		this(iName, iDatabase, DEF_CLUSTER_NAME, iClusterIdsToIndex, iAutomatic);
	}

	public OIndexFullText(final String iName, final ODatabaseRecord iDatabase, final String iClusterIndexName,
			final int[] iClusterIdsToIndex, final boolean iAutomatic) {
		this();
		create(iName, iDatabase, iClusterIndexName, iClusterIdsToIndex, null, iAutomatic);
	}

	@Override
	public OIndex create(final String iName, final ODatabaseRecord iDatabase, final String iClusterIndexName,
			final int[] iClusterIdsToIndex, final OProgressListener iProgressListener, final boolean iAutomatic) {
		name = iName;

		if (iDatabase.getClusterIdByName(iClusterIndexName) == -1)
			// CREATE THE PHYSICAL CLUSTER THE FIRST TIME
			iDatabase.addPhysicalCluster(iClusterIndexName, iClusterIndexName, -1);

		ODatabaseComplex<?> db = iDatabase;
		while (db != null && !(db instanceof ODatabaseRecord))
			db = db.getUnderlying();

		for (int id : iClusterIdsToIndex)
			clustersToIndex.add(iDatabase.getClusterNameById(id));

		map = new OMVRBTreeDatabaseLazySave<Object, List<ORecord<?>>>((ODatabaseRecord) db, iClusterIndexName,
				OStreamSerializerString.INSTANCE, OStreamSerializerListRID.INSTANCE);
		map.lazySave();

		configuration = new ODocument(iDatabase);
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
	public OIndex loadFromConfiguration(final ODatabaseRecord iDatabase, final ORID iRID) {
		configuration = new ODocument(iDatabase, iRID);
		configuration.load();

		load(iDatabase, new ORecordId((String) configuration.field(CONFIG_MAP_RID)));
		ignoreChars = (String) configuration.field(CONFIG_IGNORE_CHARS);
		stopWords = new HashSet<String>(OStringSerializerHelper.split((String) configuration.field(CONFIG_STOP_WORDS), ' '));

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
			put(fieldValue, iDocument);
		}

		try {
			map.save();
		} catch (IOException e) {
			throw new OIndexException("Can't save index entry for document '" + iDocument.getIdentity() + "'");
		}
	}

	/**
	 * Indexes a value and save the index. Splits the value in single words and index each one. Save of the index is responsibility of
	 * the caller.
	 * 
	 * @param iDocument
	 *          The document to index
	 */
	public OIndex put(final Object iKey, final ORecord<?> iSingleValue) {
		if (iKey == null)
			return this;

		List<ORecord<?>> refs;
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
				refs = new ArrayList<ORecord<?>>();

			// ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
			refs.add(iSingleValue);

			// SAVE THE INDEX ENTRY
			map.put(word, refs);
		}
		return this;
	}

	public OIndex remove(final Object iKey, final ORecord<?> value) {
		final List<ORecord<?>> recs = get(iKey);
		if (recs != null && !recs.isEmpty()) {
			if (recs.remove(value))
				map.put((String) iKey, recs);
		}
		return this;
	}

	public ODocument getConfiguration() {
		return configuration;
	}

	@Override
	public ORID getIdentity() {
		return configuration.getIdentity();
	}

	public byte[] toStream() throws OSerializationException {
		configuration.field(CONFIG_IGNORE_CHARS, ignoreChars);
		configuration.field(CONFIG_STOP_WORDS, stopWords);
		return super.toStream();
	}

	public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
		super.fromStream(iStream);
		ignoreChars = (String) configuration.field(CONFIG_IGNORE_CHARS);
		stopWords = new HashSet<String>(OStringSerializerHelper.split((String) configuration.field(CONFIG_STOP_WORDS), ' '));
		return null;
	}
}
