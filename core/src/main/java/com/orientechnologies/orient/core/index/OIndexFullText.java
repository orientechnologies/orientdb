/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fast index for full-text searches.
 *
 * @author Luca Garulli
 */
@Deprecated
public class OIndexFullText extends OIndexMultiValues {

  private static final String CONFIG_STOP_WORDS = "stopWords";
  private static final String CONFIG_SEPARATOR_CHARS = "separatorChars";
  private static final String CONFIG_IGNORE_CHARS = "ignoreChars";
  private static final String CONFIG_INDEX_RADIX = "indexRadix";
  private static final String CONFIG_MIN_WORD_LEN = "minWordLength";
  private static final boolean DEF_INDEX_RADIX = true;
  private static final String DEF_SEPARATOR_CHARS = " \r\n\t:;,.|+*/\\=!?[]()";
  private static final String DEF_IGNORE_CHARS = "'\"";
  private static final String DEF_STOP_WORDS =
      "the in a at as and or for his her "
          + "him this that what which while "
          + "up with be was were is";
  private boolean indexRadix;
  private String separatorChars;
  private String ignoreChars;
  private int minWordLength;

  private Set<String> stopWords;

  public OIndexFullText(
      String name,
      String typeId,
      String algorithm,
      int version,
      OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm,
      ODocument metadata,
      int binaryFormatVersion) {
    super(
        name,
        typeId,
        algorithm,
        version,
        storage,
        valueContainerAlgorithm,
        metadata,
        binaryFormatVersion);
    acquireExclusiveLock();
    try {
      config();
      configWithMetadata(metadata);
    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Indexes a value and save the index. Splits the value in single words and index each one. Save
   * of the index is responsibility of the caller.
   */
  @Override
  public OIndexFullText put(Object key, final OIdentifiable value) {
    if (key == null) {
      return this;
    }
    final ORID rid = value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof ORecord) {
        // EARLY SAVE IT
        ((ORecord) value).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }

    key = getCollatingValue(key);

    final Set<String> words = splitIntoWords(key.toString());

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      OTransaction singleTx = database.getTransaction();
      for (String word : words) {
        singleTx.addIndexEntry(
            this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, word, value);
      }
    } else {
      database.begin();
      OTransaction singleTx = database.getTransaction();
      for (String word : words) {
        singleTx.addIndexEntry(
            this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, word, value);
      }
      database.commit();
    }

    return this;
  }

  /**
   * Splits passed in key on several words and remove records with keys equals to any item of split
   * result and values equals to passed in value.
   *
   * @param key Key to remove.
   * @param rid Value to remove.
   * @return <code>true</code> if at least one record is removed.
   */
  @Override
  public boolean remove(Object key, final OIdentifiable rid) {
    if (key == null) {
      return false;
    }
    key = getCollatingValue(key);

    final Set<String> words = splitIntoWords(key.toString());
    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      for (final String word : words) {
        database.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, word, rid);
      }
    } else {
      database.begin();
      for (final String word : words) {
        database.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, word, rid);
      }
      database.commit();
    }

    return true;
  }

  @Override
  public OIndexMultiValues create(
      OIndexMetadata metadata, boolean rebuild, OProgressListener progressListener) {
    if (metadata.getIndexDefinition().getFields().size() > 1) {
      throw new OIndexException(type + " indexes cannot be used as composite ones.");
    }
    super.create(metadata, rebuild, progressListener);
    return this;
  }

  @Override
  public ODocument updateConfiguration() {
    super.updateConfiguration();
    return ((FullTextIndexConfiguration) configuration)
        .updateFullTextIndexConfiguration(
            separatorChars, ignoreChars, stopWords, minWordLength, indexRadix);
  }

  @Override
  protected IndexConfiguration indexConfigurationInstance(ODocument document) {
    return new FullTextIndexConfiguration(document);
  }

  public boolean canBeUsedInEqualityOperators() {
    return false;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  private void configWithMetadata(ODocument metadata) {
    if (metadata != null) {
      if (metadata.containsField(CONFIG_IGNORE_CHARS)) {
        ignoreChars = metadata.field(CONFIG_IGNORE_CHARS);
      }

      if (metadata.containsField(CONFIG_INDEX_RADIX)) {
        indexRadix = metadata.field(CONFIG_INDEX_RADIX);
      }

      if (metadata.containsField(CONFIG_SEPARATOR_CHARS)) {
        separatorChars = metadata.field(CONFIG_SEPARATOR_CHARS);
      }

      if (metadata.containsField(CONFIG_MIN_WORD_LEN)) {
        minWordLength = metadata.field(CONFIG_MIN_WORD_LEN);
      }

      if (metadata.containsField(CONFIG_STOP_WORDS)) {
        stopWords = new HashSet<>(metadata.field(CONFIG_STOP_WORDS));
      }
    }
  }

  private void config() {
    ignoreChars = DEF_IGNORE_CHARS;
    indexRadix = DEF_INDEX_RADIX;
    separatorChars = DEF_SEPARATOR_CHARS;
    minWordLength = 3;
    stopWords = new HashSet<>(OStringSerializerHelper.split(DEF_STOP_WORDS, ' '));
  }

  private Set<String> splitIntoWords(final String iKey) {
    final Set<String> result = new HashSet<>();

    final List<String> words = new ArrayList<>();
    OStringSerializerHelper.split(words, iKey, 0, -1, separatorChars);

    final StringBuilder buffer = new StringBuilder(64);
    // FOREACH WORD CREATE THE LINK TO THE CURRENT DOCUMENT

    char c;
    boolean ignore;
    for (String word : words) {
      buffer.setLength(0);

      for (int i = 0; i < word.length(); ++i) {
        c = word.charAt(i);
        ignore = false;
        for (int k = 0; k < ignoreChars.length(); ++k) {
          if (c == ignoreChars.charAt(k)) {
            ignore = true;
            break;
          }
        }

        if (!ignore) {
          buffer.append(c);
        }
      }

      int length = buffer.length();

      while (length >= minWordLength) {
        buffer.setLength(length);
        word = buffer.toString();

        // CHECK IF IT'S A STOP WORD
        if (!stopWords.contains(word))
        // ADD THE WORD TO THE RESULT SET
        {
          result.add(word);
        }

        if (indexRadix) {
          length--;
        } else {
          break;
        }
      }
    }

    return result;
  }

  private static final class FullTextIndexConfiguration extends IndexConfiguration {
    private FullTextIndexConfiguration(ODocument document) {
      super(document);
    }

    private synchronized ODocument updateFullTextIndexConfiguration(
        String separatorChars,
        String ignoreChars,
        Set<String> stopWords,
        int minWordLength,
        boolean indexRadix) {
      document.field(CONFIG_SEPARATOR_CHARS, separatorChars);
      document.field(CONFIG_IGNORE_CHARS, ignoreChars);
      document.field(CONFIG_STOP_WORDS, stopWords);
      document.field(CONFIG_MIN_WORD_LEN, minWordLength);
      document.field(CONFIG_INDEX_RADIX, indexRadix);

      return document;
    }
  }
}
