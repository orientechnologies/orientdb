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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Basic interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndex<T> extends OKeyValueIndex<T> {

  /**
   * Returns an iterator to walk across all the index items from the first to the latest one.
   * 
   * @return
   */
  public Iterator<Entry<Object, T>> iterator();

  /**
   * Returns an iterator to walk across all the index items from the last to the first one.
   * 
   * @return
   */
  public Iterator<Entry<Object, T>> inverseIterator();

  /**
   * Returns an iterator to walk across all the index values from the first to the latest one.
   * 
   * @return
   */
  public Iterator<OIdentifiable> valuesIterator();

  /**
   * Returns an iterator to walk across all the index values from the last to the first one.
   * 
   * @return
   */
  public Iterator<OIdentifiable> valuesInverseIterator();

  /**
   * Returns an Iterable instance of all the keys contained in the index.
   * 
   * @return A Iterable<Object> that lazy load the entries once fetched
   */
  public Iterable<Object> keys();

  /**
   * Returns a set of records with key between the range passed as parameter. Range bounds are included.
   * 
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * 
   * @return a set of records with key between the range passed as parameter. Range bounds are included.
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   * @see #getValuesBetween(Object, boolean, Object, boolean)
   */
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo);

  /**
   * Returns a set of records with key between the range passed as parameter.
   * 
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iFromInclusive
   *          Indicates whether start range boundary is included in result.
   * @param iRangeTo
   *          Ending range
   * @param iToInclusive
   *          Indicates whether end range boundary is included in result.
   * 
   * @return Returns a set of records with key between the range passed as parameter.
   * 
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   * 
   */
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive);

  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo,
      boolean iToInclusive, int maxValuesToFetch);

  /**
   * Returns a set of records with keys greater than passed parameter.
   * 
   * @param fromKey
   *          Starting key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with keys greater than passed parameter.
   */
  public abstract Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive);

  public abstract Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch);

  /**
   * Returns a set of records with keys less than passed parameter.
   * 
   * @param toKey
   *          Ending key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with keys less than passed parameter.
   */
  public abstract Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive);

  public abstract Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, int maxValuesToFetch);

  /**
   * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
   * greater than passed parameter.
   * 
   * @param fromKey
   *          Starting key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with key greater than passed parameter.
   */
  public abstract Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive);

  public abstract Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, int maxEntriesToFetch);

  /**
   * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
   * less than passed parameter.
   * 
   * @param toKey
   *          Ending key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with key greater than passed parameter.
   */
  public abstract Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive);

  public abstract Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch);

  /**
   * Returns a set of documents with key between the range passed as parameter.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * @param iInclusive
   *          Include from/to bounds
   * @see #getEntriesBetween(Object, Object)
   * @return
   */
  public abstract Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive);

  public abstract Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive,
      final int maxEntriesToFetch);

  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo);

  /**
   * Returns the Record Identity of the index if persistent.
   * 
   * @return Valid ORID if it's persistent, otherwise ORID(-1:-1)
   */
  public ORID getIdentity();

  public boolean supportsOrderedIterations();

  public boolean isRebuiding();
}
