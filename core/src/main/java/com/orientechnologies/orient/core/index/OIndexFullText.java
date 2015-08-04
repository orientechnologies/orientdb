/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

import java.util.ArrayList;
import java.util.Collection;
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

  private static final String  CONFIG_STOP_WORDS      = "stopWords";
  private static final String  CONFIG_SEPARATOR_CHARS = "separatorChars";
  private static final String  CONFIG_IGNORE_CHARS    = "ignoreChars";
  private static final String  CONFIG_INDEX_RADIX     = "indexRadix";
  private static final String  CONFIG_MIN_WORD_LEN    = "minWordLength";
  private static final boolean DEF_INDEX_RADIX        = true;
  private static final String  DEF_SEPARATOR_CHARS    = " \r\n\t:;,.|+*/\\=!?[]()";
  private static final String  DEF_IGNORE_CHARS       = "'\"";
  private static final String  DEF_STOP_WORDS         = "the in a at as and or for his her " + "him this that what which while "
                                                          + "up with be was were is";
  private static int           DEF_MIN_WORD_LENGTH    = 3;
  private boolean              indexRadix;
  private String               separatorChars;
  private String               ignoreChars;
  private int                  minWordLength;

  private Set<String>          stopWords;

  public OIndexFullText(String name, String typeId, String algorithm, OIndexEngine<Set<OIdentifiable>> indexEngine,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, indexEngine, valueContainerAlgorithm, metadata);
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
  public OIndexFullText put(Object key, final OIdentifiable iSingleValue) {
    checkForRebuild();

    if (key == null)
      return this;

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);

    try {
      modificationLock.requestModificationLock();

      try {
        final Set<String> words = splitIntoWords(key.toString());

        // FOREACH WORD CREATE THE LINK TO THE CURRENT DOCUMENT
        for (final String word : words) {
          acquireSharedLock();
          startStorageAtomicOperation();
          try {
            Set<OIdentifiable> refs;

            // SEARCH FOR THE WORD
            refs = indexEngine.get(word);

            if (refs == null) {
              // WORD NOT EXISTS: CREATE THE KEYWORD CONTAINER THE FIRST TIME THE WORD IS FOUND
              if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
                boolean durable = false;
                if (metadata != null && Boolean.TRUE.equals(metadata.field("durableInNonTxMode")))
                  durable = true;

                refs = new OIndexRIDContainer(getName(), durable);
              } else {
                refs = new OMVRBTreeRIDSet();
                ((OMVRBTreeRIDSet) refs).setAutoConvertToRecord(false);
              }
            }

            // ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
            refs.add(iSingleValue);

            // SAVE THE INDEX ENTRY
            indexEngine.put(word, refs);

            commitStorageAtomicOperation();
          } catch (RuntimeException e) {
            rollbackStorageAtomicOperation();
            throw new OIndexException("Error during put of key - value entry", e);
          } finally {
            releaseSharedLock();
          }
        }
        return this;
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }
  }

  /**
   * Splits passed in key on several words and remove records with keys equals to any item of split result and values equals to
   * passed in value.
   * 
   * @param key
   *          Key to remove.
   * @param value
   *          Value to remove.
   * @return <code>true</code> if at least one record is removed.
   */
  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);
    try {
      modificationLock.requestModificationLock();

      try {
        final Set<String> words = splitIntoWords(key.toString());
        boolean removed = false;

        for (final String word : words) {
          acquireSharedLock();
          startStorageAtomicOperation();
          try {

            final Set<OIdentifiable> recs = indexEngine.get(word);
            if (recs != null && !recs.isEmpty()) {
              if (recs.remove(value)) {
                if (recs.isEmpty())
                  indexEngine.remove(word);
                else
                  indexEngine.put(word, recs);
                removed = true;
              }
            }
            commitStorageAtomicOperation();
          } catch (RuntimeException e) {
            rollbackStorageAtomicOperation();
            throw new OIndexException("Error during removal of entry by key and value", e);
          } finally {
            releaseSharedLock();
          }
        }

        return removed;
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }
  }

  @Override
  public OIndexInternal<?> create(OIndexDefinition indexDefinition, String clusterIndexName, Set<String> clustersToIndex,
      boolean rebuild, OProgressListener progressListener, OStreamSerializer valueSerializer) {

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
  protected void doConfigurationUpdate(ODocument newConfig) {
    super.doConfigurationUpdate(newConfig);

    newConfig.field(CONFIG_SEPARATOR_CHARS, separatorChars);
    newConfig.field(CONFIG_IGNORE_CHARS, ignoreChars);
    newConfig.field(CONFIG_STOP_WORDS, stopWords);
    newConfig.field(CONFIG_MIN_WORD_LEN, minWordLength);
    newConfig.field(CONFIG_INDEX_RADIX, indexRadix);
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

    final List<String> words = (List<String>) OStringSerializerHelper.split(new ArrayList<String>(), iKey, 0, -1, separatorChars);

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
}
