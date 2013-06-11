/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Fast index for full-text searches.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexFullText extends OIndexMultiValues {

  public static final String  TYPE_ID                = OClass.INDEX_TYPE.FULLTEXT.toString();

  private static final String CONFIG_STOP_WORDS      = "stopWords";
  private static final String CONFIG_SEPARATOR_CHARS = "separatorChars";
  private static final String CONFIG_IGNORE_CHARS    = "ignoreChars";

  private static String       DEF_SEPARATOR_CHARS    = " \r\n\t:;,.|+*/\\=!?[]()";
  private static String       DEF_IGNORE_CHARS       = "'\"";
  private static String       DEF_STOP_WORDS         = "the in a at as and or for his her " + "him this that what which while "
                                                         + "up with be was is";
  private final String        separatorChars         = DEF_SEPARATOR_CHARS;
  private final String        ignoreChars            = DEF_IGNORE_CHARS;
  private final Set<String>   stopWords;

  public OIndexFullText() {
    super(TYPE_ID);
    stopWords = new HashSet<String>(OStringSerializerHelper.split(DEF_STOP_WORDS, ' '));
  }

  /**
   * Index an entire document field by field and save the index at the end.
   * 
   * @param iDocument
   *          The document to index
   */
  public void indexDocument(final ODocument iDocument) {
    modificationLock.requestModificationLock();

    try {
      Object fieldValue;

      for (final String fieldName : iDocument.fieldNames()) {
        fieldValue = iDocument.field(fieldName);
        put(fieldValue, iDocument);
      }

      acquireExclusiveLock();
      try {

        map.save();

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  /**
   * Indexes a value and save the index. Splits the value in single words and index each one. Save of the index is responsibility of
   * the caller.
   */
  @Override
  public OIndexFullText put(final Object iKey, final OIdentifiable iSingleValue) {
    checkForRebuild();

    if (iKey == null)
      return this;

    modificationLock.requestModificationLock();

    try {
      final List<String> words = splitIntoWords(iKey.toString());

      // FOREACH WORD CREATE THE LINK TO THE CURRENT DOCUMENT
      for (final String word : words) {
        acquireExclusiveLock();

        try {
          Set<OIdentifiable> refs;

          // SEARCH FOR THE WORD
          refs = map.get(word);

          if (refs == null) {
            // WORD NOT EXISTS: CREATE THE KEYWORD CONTAINER THE FIRST TIME THE WORD IS FOUND
            refs = new OMVRBTreeRIDSet();
            ((OMVRBTreeRIDSet) refs).setAutoConvertToRecord(false);
          }

          // ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
          refs.add(iSingleValue);

          // SAVE THE INDEX ENTRY
          map.put(word, refs);

        } finally {
          releaseExclusiveLock();
        }
      }
      return this;
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  /**
   * Splits passed in key on several words and remove records with keys equals to any item of split result and values equals to
   * passed in value.
   * 
   * @param iKey
   *          Key to remove.
   * @param value
   *          Value to remove.
   * @return <code>true</code> if at least one record is removed.
   */
  @Override
  public boolean remove(final Object iKey, final OIdentifiable value) {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      final List<String> words = splitIntoWords(iKey.toString());
      boolean removed = false;

      for (final String word : words) {
        acquireExclusiveLock();
        try {

          final Set<OIdentifiable> recs = map.get(word);
          if (recs != null && !recs.isEmpty()) {
            if (recs.remove(value)) {
              if (recs.isEmpty())
                map.remove(word);
              else
                map.put(word, recs);
              removed = true;
            }
          }
        } finally {
          releaseExclusiveLock();
        }
      }

      return removed;
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  public OIndexInternal<?> create(String iName, OIndexDefinition iIndexDefinition, ODatabaseRecord iDatabase,
      String iClusterIndexName, int[] iClusterIdsToIndex, boolean rebuild, OProgressListener iProgressListener,
      OStreamSerializer iValueSerializer) {

    if (iIndexDefinition.getFields().size() > 1) {
      throw new OIndexException(TYPE_ID + " indexes cannot be used as composite ones.");
    }

    return super.create(iName, iIndexDefinition, iDatabase, iClusterIndexName, iClusterIdsToIndex, rebuild, iProgressListener,
        iValueSerializer);
  }

  @Override
  public OIndexMultiValues create(String iName, OIndexDefinition indexDefinition, ODatabaseRecord iDatabase,
      String iClusterIndexName, int[] iClusterIdsToIndex, boolean rebuild, OProgressListener iProgressListener) {
    if (indexDefinition.getFields().size() > 1) {
      throw new OIndexException(TYPE_ID + " indexes cannot be used as composite ones.");
    }
    return super.create(iName, indexDefinition, iDatabase, iClusterIndexName, iClusterIdsToIndex, rebuild, iProgressListener);
  }

  @Override
  public ODocument updateConfiguration() {
    super.updateConfiguration();
    configuration.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

    try {
      configuration.field(CONFIG_SEPARATOR_CHARS, separatorChars);
      configuration.field(CONFIG_IGNORE_CHARS, ignoreChars);
      configuration.field(CONFIG_STOP_WORDS, stopWords);

    } finally {
      configuration.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
    return configuration;
  }

  private List<String> splitIntoWords(final String iKey) {
    final List<String> result = new ArrayList<String>();

    final List<String> words = (List<String>) OStringSerializerHelper.split(new ArrayList<String>(), iKey, 0, -1, separatorChars);

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

  public boolean canBeUsedInEqualityOperators() {
    return false;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }
}
