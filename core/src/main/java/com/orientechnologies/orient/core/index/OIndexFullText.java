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
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * Fast index for full-text searches.
 *
 * @author Luca Garulli
 */
public class OIndexFullText extends OIndexMultiValues {

  private static final String  CONFIG_STOP_WORDS      = "stopWords";
  private static final String  CONFIG_SEPARATOR_CHARS = "separatorChars";
  private static final String  CONFIG_IGNORE_CHARS    = "ignoreChars";
  private static final String  CONFIG_INDEX_RADIX     = "indexRadix";
  private static final String  CONFIG_MIN_WORD_LEN    = "minWordLength";
  private static final boolean DEF_INDEX_RADIX        = true;
  private static final String  DEF_SEPARATOR_CHARS    = " \r\n\t:;,.|+*/\\=!?[]()";
  private static final String  DEF_IGNORE_CHARS       = "'\"";
  private static final String  DEF_STOP_WORDS         =
      "the in a at as and or for his her " + "him this that what which while " + "up with be was were is";
  private static       int     DEF_MIN_WORD_LENGTH    = 3;
  private boolean indexRadix;
  private String  separatorChars;
  private String  ignoreChars;
  private int     minWordLength;

  private Set<String> stopWords;

  public OIndexFullText(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, version, storage, valueContainerAlgorithm, metadata);
    acquireExclusiveLock();
    try {
      config();
      configWithMetadata(metadata);
    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Indexes a value and save the index. Splits the value in single words and index each one. Save of the index is responsibility of
   * the caller.
   */
  @Override
  public OIndexFullText put(Object key, final OIdentifiable singleValue) {
    if (key == null)
      return this;

    key = getCollatingValue(key);

    final Set<String> words = splitIntoWords(key.toString());

    // FOREACH WORD CREATE THE LINK TO THE CURRENT DOCUMENT
    for (final String word : words) {
      acquireSharedLock();
      try {
        Set<OIdentifiable> refs;
        while (true) {
          try {
            refs = (Set<OIdentifiable>) storage.getIndexValue(indexId, word);
            break;
          } catch (OInvalidIndexEngineIdException ignore) {
            doReloadIndexEngine();
          }
        }

        final boolean durable;
        if (metadata != null && Boolean.TRUE.equals(metadata.field("durableInNonTxMode")))
          durable = true;
        else
          durable = false;

        final Set<OIdentifiable> refsc = refs;

        // SAVE THE INDEX ENTRY
        while (true) {
          try {
            storage.updateIndexEntry(indexId, word, new OIndexKeyUpdater<Object>() {
              @Override
              public Object update(Object oldValue) {
                Set<OIdentifiable> result = null;

                if (refsc == null) {
                  // WORD NOT EXISTS: CREATE THE KEYWORD CONTAINER THE FIRST TIME THE WORD IS FOUND
                  if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
                    result = new OIndexRIDContainer(getName(), durable);
                  } else {
                    throw new IllegalStateException("MBRBTreeContainer is not supported any more");
                  }
                } else {
                  result = refsc;
                }

                // ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
                result.add(singleValue);

                return result;
              }
            });

            break;
          } catch (OInvalidIndexEngineIdException ignore) {
            doReloadIndexEngine();
          }
        }

      } finally {
        releaseSharedLock();
      }
    }
    return this;
  }

  /**
   * Splits passed in key on several words and remove records with keys equals to any item of split result and values equals to
   * passed in value.
   *
   * @param key   Key to remove.
   * @param value Value to remove.
   *
   * @return <code>true</code> if at least one record is removed.
   */
  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    if (key == null)
      return false;

    key = getCollatingValue(key);

    final Set<String> words = splitIntoWords(key.toString());
    final OModifiableBoolean removed = new OModifiableBoolean(false);

    for (final String word : words) {
      acquireSharedLock();
      try {
        Set<OIdentifiable> recs;
        while (true) {
          try {
            recs = (Set<OIdentifiable>) storage.getIndexValue(indexId, word);
            break;
          } catch (OInvalidIndexEngineIdException ignore) {
            doReloadIndexEngine();
          }
        }

        if (recs != null && !recs.isEmpty()) {
          while (true) {
            try {
              storage.updateIndexEntry(indexId, word, new EntityRemover(value, removed));
              break;
            } catch (OInvalidIndexEngineIdException ignore) {
              doReloadIndexEngine();
            }

          }

        }
      } finally {
        releaseSharedLock();
      }
    }

    return removed.getValue();
  }

  @Override
  public OIndexInternal<?> create(OIndexDefinition indexDefinition, String clusterIndexName, Set<String> clustersToIndex,
      boolean rebuild, OProgressListener progressListener, OBinarySerializer valueSerializer) {

    if (indexDefinition.getFields().size() > 1) {
      throw new OIndexException(type + " indexes cannot be used as composite ones.");
    }

    return super.create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener, valueSerializer);
  }

  @Override
  public OIndexMultiValues create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    if (indexDefinition.getFields().size() > 1) {
      throw new OIndexException(type + " indexes cannot be used as composite ones.");
    }
    return super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  @Override
  public ODocument updateConfiguration() {
    super.updateConfiguration();
    return ((FullTextIndexConfiguration) configuration)
        .updateFullTextIndexConfiguration(separatorChars, ignoreChars, stopWords, minWordLength, indexRadix);
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

  protected void configWithMetadata(ODocument metadata) {
    if (metadata != null) {
      if (metadata.containsField(CONFIG_IGNORE_CHARS))
        ignoreChars = (String) metadata.field(CONFIG_IGNORE_CHARS);

      if (metadata.containsField(CONFIG_INDEX_RADIX))
        indexRadix = (Boolean) metadata.field(CONFIG_INDEX_RADIX);

      if (metadata.containsField(CONFIG_SEPARATOR_CHARS))
        separatorChars = (String) metadata.field(CONFIG_SEPARATOR_CHARS);

      if (metadata.containsField(CONFIG_MIN_WORD_LEN))
        minWordLength = (Integer) metadata.field(CONFIG_MIN_WORD_LEN);

      if (metadata.containsField(CONFIG_STOP_WORDS))
        stopWords = new HashSet<String>((Collection<? extends String>) metadata.field(CONFIG_STOP_WORDS));
    }

  }

  protected void config() {
    ignoreChars = DEF_IGNORE_CHARS;
    indexRadix = DEF_INDEX_RADIX;
    separatorChars = DEF_SEPARATOR_CHARS;
    minWordLength = DEF_MIN_WORD_LENGTH;
    stopWords = new HashSet<String>(OStringSerializerHelper.split(DEF_STOP_WORDS, ' '));
  }

  private Set<String> splitIntoWords(final String iKey) {
    final Set<String> result = new HashSet<String>();

    final List<String> words = new ArrayList<String>();
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
        for (int k = 0; k < ignoreChars.length(); ++k)
          if (c == ignoreChars.charAt(k)) {
            ignore = true;
            break;
          }

        if (!ignore)
          buffer.append(c);
      }

      int length = buffer.length();

      while (length >= minWordLength) {
        buffer.setLength(length);
        word = buffer.toString();

        // CHECK IF IT'S A STOP WORD
        if (!stopWords.contains(word))
          // ADD THE WORD TO THE RESULT SET
          result.add(word);

        if (indexRadix)
          length--;
        else
          break;
      }
    }

    return result;
  }

  private static class EntityRemover implements OIndexKeyUpdater<Object> {
    private final OIdentifiable      value;
    private final OModifiableBoolean removed;

    public EntityRemover(OIdentifiable value, OModifiableBoolean removed) {
      this.value = value;
      this.removed = removed;
    }

    @Override
    public Object update(Object old) {
      Set<OIdentifiable> recs = (Set<OIdentifiable>) old;
      if (recs.remove(value)) {
        removed.setValue(true);

        if (recs.isEmpty())
          return null;
        else
          return recs;

      }

      return recs;
    }
  }

  private final class FullTextIndexConfiguration extends IndexConfiguration {
    public FullTextIndexConfiguration(ODocument document) {
      super(document);
    }

    public synchronized ODocument updateFullTextIndexConfiguration(String separatorChars, String ignoreChars, Set<String> stopWords,
        int minWordLength, boolean indexRadix) {
      document.field(CONFIG_SEPARATOR_CHARS, separatorChars);
      document.field(CONFIG_IGNORE_CHARS, ignoreChars);
      document.field(CONFIG_STOP_WORDS, stopWords);
      document.field(CONFIG_MIN_WORD_LEN, minWordLength);
      document.field(CONFIG_INDEX_RADIX, indexRadix);

      return document;
    }

  }
}
