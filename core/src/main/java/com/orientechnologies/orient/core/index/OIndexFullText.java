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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fast index for full-text searches.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexFullText extends OIndexMultiValues {
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

	/**
	 * Index an entire document field by field and save the index at the end.
	 * 
	 * @param iDocument
	 *          The document to index
	 */
	public void indexDocument(final ODocument iDocument) {
		Object fieldValue;

		for (final String fieldName : iDocument.fieldNames()) {
			fieldValue = iDocument.field(fieldName);
			put(fieldValue, iDocument);
		}

		acquireExclusiveLock();

		try {
			map.save();
		} catch (IOException e) {
			throw new OIndexException("Can't save index entry for document '" + iDocument.getIdentity() + "'");
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Indexes a value and save the index. Splits the value in single words and index each one. Save of the index is responsibility of
	 * the caller.
	 */
	public OIndexFullText put(final Object iKey, final OIdentifiable iSingleValue) {
		if (iKey == null)
			return this;


		final List<String> words = splitIntoWords(iKey.toString());

		// FOREACH WORD CREATE THE LINK TO THE CURRENT DOCUMENT
		for (final String word : words) {
			acquireExclusiveLock();

			try {
                Set<OIdentifiable> refs;

				// SEARCH FOR THE WORD
				refs = map.get(word);

				if (refs == null)
					// WORD NOT EXISTS: CREATE THE KEYWORD CONTAINER THE FIRST TIME THE WORD IS FOUND
					refs = new ORecordLazySet(configuration.getDatabase()).setRidOnly(true);

				// ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
				refs.add(iSingleValue);

				// SAVE THE INDEX ENTRY
				map.put(word, refs);

			} finally {
				releaseExclusiveLock();
			}
		}
		return this;
	}

    /**
     * Splits passed in key on several words and remove records with keys equals to any item of split result and
     * values equals to passed in value.
     * @param iKey     Key to remove.
     * @param value    Value to remove.
     * @return   <code>true</code> if at least one record is removed.
     */
    public boolean remove(final Object iKey, final OIdentifiable value) {
        final List<String> words = splitIntoWords(iKey.toString());
        boolean removed = false;

        for (final String word : words) {
            acquireExclusiveLock();
            try {

                final Set<OIdentifiable> recs = get(word);
                if (recs != null && !recs.isEmpty()) {
                    if (recs.remove(value)) {
                        if(recs.isEmpty())
                            map.remove(iKey);
                        else
                            map.put(iKey, recs);
                        removed = true;
                    }
                }
            } finally {
                releaseExclusiveLock();
            }
        }

        return removed;
    }

    @Override
	public ODocument updateConfiguration() {
		super.updateConfiguration();
		configuration.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		try {
			configuration.field(CONFIG_IGNORE_CHARS, ignoreChars);
			configuration.field(CONFIG_STOP_WORDS, stopWords);

		} finally {
			configuration.setInternalStatus(ORecordElement.STATUS.LOADED);
		}
		return configuration;
	}

    private List<String> splitIntoWords(final String iKey) {
        final List<String> result = new ArrayList<String>();

        final List<String> words = OStringSerializerHelper.split(iKey, ' ');

        final StringBuilder buffer = new StringBuilder();
        // FOREACH WORD CREATE THE LINK TO THE CURRENT DOCUMENT

        char c;
        boolean ignore;
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

            result.add(word);
        }

        return result;
    }
}
