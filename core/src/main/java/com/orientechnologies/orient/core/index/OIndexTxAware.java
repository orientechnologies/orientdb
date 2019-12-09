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

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;

import java.util.stream.Stream;

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit(). All the other operations are
 * delegated to the wrapped OIndex instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OIndexTxAware<T> extends OIndexAbstractDelegate {
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY    = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  protected ODatabaseDocumentInternal database;

  /**
   * Indicates search behavior in case of {@link com.orientechnologies.orient.core.index.OCompositeKey} keys that have less amount
   * of internal keys are used, whether lowest or highest partially matched key should be used. Such keys is allowed to use only in
   */
  public enum PartialSearchMode {
    /**
     * Any partially matched key will be used as search result.
     */
    NONE,
    /**
     * The biggest partially matched key will be used as search result.
     */
    HIGHEST_BOUNDARY,

    /**
     * The smallest partially matched key will be used as search result.
     */
    LOWEST_BOUNDARY
  }

  public OIndexTxAware(final ODatabaseDocumentInternal database, final OIndexInternal delegate) {
    super(delegate);
    this.database = database;
  }

  @Override
  public long size() {
    long tot = delegate.size();

    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction().getIndexChanges(delegate.getName());
    if (indexChanges != null) {
      try (Stream<ORawPair<Object, ORID>> stream = stream()) {
        return stream.count();
      }
    }

    return tot;
  }

  @Override
  public OIndexTxAware<T> put(Object iKey, final OIdentifiable iValue) {
    checkForKeyType(iKey);
    final ORID rid = iValue.getIdentity();

    if (!rid.isValid())
      if (iValue instanceof ORecord) {
        // EARLY SAVE IT
        ((ORecord) iValue).save();
      } else {
        throw new IllegalArgumentException("Cannot store non persistent RID as index value for key '" + iKey + "'");
      }

    iKey = getCollatingValue(iKey);

    database.getMicroOrRegularTransaction().addIndexEntry(delegate, super.getName(), OPERATION.PUT, iKey, iValue);
    return this;
  }

  @Override
  public boolean remove(Object key) {
    key = getCollatingValue(key);
    database.getMicroOrRegularTransaction().addIndexEntry(delegate, super.getName(), OPERATION.REMOVE, key, null);
    return true;
  }

  @Override
  public boolean remove(Object iKey, final OIdentifiable iRID) {
    iKey = getCollatingValue(iKey);
    database.getMicroOrRegularTransaction().addIndexEntry(delegate, super.getName(), OPERATION.REMOVE, iKey, iRID);
    return true;
  }

  @SuppressWarnings("deprecation")
  @Override
  public OIndexTxAware<T> clear() {
    database.getMicroOrRegularTransaction().addIndexEntry(delegate, super.getName(), OPERATION.CLEAR, null, null);
    return this;
  }

  private Object enhanceCompositeKey(Object key, PartialSearchMode partialSearchMode) {
    if (!(key instanceof OCompositeKey))
      return key;

    final OCompositeKey compositeKey = (OCompositeKey) key;
    final int keySize = getDefinition().getParamCount();

    if (!(keySize == 1 || compositeKey.getKeys().size() == keySize || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY))
        keyItem = ALWAYS_GREATER_KEY;
      else
        keyItem = ALWAYS_LESS_KEY;

      for (int i = 0; i < itemsToAdd; i++)
        fullKey.addKey(keyItem);

      return fullKey;
    }

    return key;
  }

  protected Object enhanceToCompositeKeyBetweenAsc(Object keyTo, boolean toInclusive) {
    PartialSearchMode partialSearchModeTo;
    if (toInclusive)
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  protected Object enhanceFromCompositeKeyBetweenAsc(Object keyFrom, boolean fromInclusive) {
    PartialSearchMode partialSearchModeFrom;
    if (fromInclusive)
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  protected Object enhanceToCompositeKeyBetweenDesc(Object keyTo, boolean toInclusive) {
    PartialSearchMode partialSearchModeTo;
    if (toInclusive)
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  protected Object enhanceFromCompositeKeyBetweenDesc(Object keyFrom, boolean fromInclusive) {
    PartialSearchMode partialSearchModeFrom;
    if (fromInclusive)
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }
}
