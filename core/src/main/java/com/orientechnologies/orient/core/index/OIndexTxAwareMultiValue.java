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

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.stream.Streams;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit().
 * All the other operations are delegated to the wrapped OIndex instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexTxAwareMultiValue extends OIndexTxAware<Collection<OIdentifiable>> {
  public OIndexTxAwareMultiValue(
      final ODatabaseDocumentInternal database, final OIndexInternal delegate) {
    super(database, delegate);
  }

  @Deprecated
  @Override
  public Collection<OIdentifiable> get(Object key) {
    final List<OIdentifiable> rids;
    try (Stream<ORID> stream = getRids(key)) {
      rids = stream.collect(Collectors.toList());
    }
    return rids;
  }

  public static Set<OIdentifiable> changesForKey(
      ODatabaseDocumentInternal database, OIndexInternal index, final Object key) {

    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(index.getName());
    if (indexChanges == null) {
      return null;
    }
    final Object collatedKey = index.getCollatingValue(key);
    Set<OIdentifiable> txChanges = calculateTxValue(collatedKey, indexChanges);
    return txChanges;
  }

  @Override
  public Stream<ORID> getRids(final Object key) {
    return super.getRids(key);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream() {
    return super.stream();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    return super.descStream();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey,
      final boolean fromInclusive,
      Object toKey,
      final boolean toInclusive,
      final boolean ascOrder) {
    return super.streamEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
  }

  private Stream<ORawPair<Object, ORID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, ORID>> txStream,
      Stream<ORawPair<Object, ORID>> backedStream,
      boolean ascOrder) {
    return Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map((entry) -> calculateTxIndexEntry(entry.first, entry.second, indexChanges))
            .filter(Objects::nonNull),
        (entryOne, entryTwo) -> {
          if (ascOrder) {
            return ODefaultComparator.INSTANCE.compare(
                getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
          } else {
            return -ODefaultComparator.INSTANCE.compare(
                getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
          }
        });
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return super.streamEntriesMajor(fromKey, fromInclusive, ascOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder) {

    return super.streamEntriesMinor(toKey, toInclusive, ascOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    return super.streamEntries(keys, ascSortOrder);
  }

  private ORawPair<Object, ORID> calculateTxIndexEntry(
      Object key, final ORID backendValue, OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return new ORawPair<>(key, backendValue);
    }

    int putCounter = 1;
    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.PUT && entry.getValue().equals(backendValue))
        putCounter++;
      else if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) putCounter = 0;
        else if (entry.getValue().equals(backendValue) && putCounter > 0) putCounter--;
      }
    }

    if (putCounter <= 0) {
      return null;
    }

    return new ORawPair<>(key, backendValue);
  }

  static Set<OIdentifiable> calculateTxValue(
      final Object key, OTransactionIndexChanges indexChanges) {
    final List<OIdentifiable> result = new ArrayList<>();
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return null;
    }

    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) result.clear();
        else result.remove(entry.getValue());
      } else result.add(entry.getValue());
    }

    if (result.isEmpty()) return null;

    return new HashSet<>(result);
  }
}
