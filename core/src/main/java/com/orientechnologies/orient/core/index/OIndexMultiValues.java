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
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract index implementation that supports multi-values for the same key.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OIndexMultiValues extends OIndexAbstract<Set<OIdentifiable>> {
  public OIndexMultiValues(String name, final String type, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, final ODocument metadata) {
    super(name, type, algorithm, valueContainerAlgorithm, metadata, version, storage);
  }

  public Set<OIdentifiable> get(Object key) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {

      Set<OIdentifiable> values;

      while (true) {
        try {
          values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

      if (values == null)
        return Collections.emptySet();

      return Collections.unmodifiableSet(values);

    } finally {
      releaseSharedLock();
    }
  }

  public long count(Object key) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {

      Set<OIdentifiable> values;

      while (true) {
        try {
          values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

      if (values == null)
        return 0;

      return values.size();

    } finally {
      releaseSharedLock();
    }
  }

  public OIndexMultiValues put(Object key, final OIdentifiable singleValue) {
    key = getCollatingValue(key);

    acquireSharedLock();

    try {
      if (!singleValue.getIdentity().isValid())
        (singleValue.getRecord()).save();

      final ORID identity = singleValue.getIdentity();

      final boolean durable;

      if (metadata != null && Boolean.TRUE.equals(metadata.field("durableInNonTxMode")))
        durable = true;
      else
        durable = false;

      final OIndexKeyUpdater<Object> creator = (oldValue, bonsayFileId) -> {
        Set<OIdentifiable> toUpdate = (Set<OIdentifiable>) oldValue;
        if (toUpdate == null) {
          if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
            toUpdate = new OIndexRIDContainer(getName(), durable, bonsayFileId);
          } else {
            throw new IllegalStateException("MVRBTree is not supported any more");
          }
        }
        boolean isTree = toUpdate instanceof OIndexRIDContainer && !((OIndexRIDContainer) toUpdate).isEmbedded();
        toUpdate.add(identity);
        if (isTree) {
          return OIndexUpdateAction.nothing();
        } else {
          return OIndexUpdateAction.changed(toUpdate);
        }
      };

      while (true) {
        try {
          storage.updateIndexEntry(indexId, key, creator);
          return this;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      Set<OIdentifiable> values = null;
      while (true) {
        try {
          values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

      if (values == null) {
        return false;
      }

      final OModifiableBoolean removed = new OModifiableBoolean(false);

      final OIndexKeyUpdater<Object> creator = new EntityRemover(value, removed);

      while (true)
        try {
          storage.updateIndexEntry(indexId, key, creator);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }

      return removed.getValue();

    } finally {
      releaseSharedLock();
    }
  }

  public OIndexMultiValues create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {

    return (OIndexMultiValues) super
        .create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener, determineValueSerializer());
  }

  protected OBinarySerializer determineValueSerializer() {
    return storage.getComponentsFactory().binarySerializerFactory.getObjectSerializer(OStreamSerializerSBTreeIndexRIDContainer.ID);
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      while (true)
        try {
          return storage.iterateIndexEntriesBetween(indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder,
              MultiValuesTransformer.INSTANCE);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);

    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, MultiValuesTransformer.INSTANCE);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    final Comparator<Object> comparator;
    if (ascSortOrder)
      comparator = ODefaultComparator.INSTANCE;
    else
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    Collections.sort(sortedKeys, comparator);

    return new OIndexAbstractCursor() {
      private Iterator<?> keysIterator = sortedKeys.iterator();

      private Iterator<OIdentifiable> currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
      private Object currentKey;

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (currentIterator == null)
          return null;

        Object key = null;
        if (!currentIterator.hasNext()) {
          Collection<OIdentifiable> result = null;
          while (keysIterator.hasNext() && (result == null || result.isEmpty())) {
            key = keysIterator.next();
            key = getCollatingValue(key);

            acquireSharedLock();
            try {
              while (true)
                try {
                  result = (Collection<OIdentifiable>) storage.getIndexValue(indexId, key);
                  break;
                } catch (OInvalidIndexEngineIdException ignore) {
                  doReloadIndexEngine();
                }

            } finally {
              releaseSharedLock();
            }
          }

          if (result == null) {
            currentIterator = null;
            return null;
          }

          currentKey = key;
          currentIterator = result.iterator();
        }

        final OIdentifiable resultValue = currentIterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return currentKey;
          }

          @Override
          public OIdentifiable getValue() {
            return resultValue;
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  public long getSize() {
    acquireSharedLock();
    try {
      while (true)
        try {
          return storage.getIndexSize(indexId, MultiValuesTransformer.INSTANCE);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }

  }

  public long getKeySize() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexSize(indexId, null);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor cursor() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexCursor(indexId, MultiValuesTransformer.INSTANCE);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor descCursor() {
    acquireSharedLock();
    try {
      while (true)
        try {
          return storage.getIndexDescCursor(indexId, MultiValuesTransformer.INSTANCE);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  private static final class MultiValuesTransformer implements OIndexEngine.ValuesTransformer {
    private static final MultiValuesTransformer INSTANCE = new MultiValuesTransformer();

    @Override
    public Collection<OIdentifiable> transformFromValue(Object value) {
      return (Collection<OIdentifiable>) value;
    }
  }

  private static class EntityRemover implements OIndexKeyUpdater<Object> {
    private final OIdentifiable      value;
    private final OModifiableBoolean removed;

    public EntityRemover(OIdentifiable value, OModifiableBoolean removed) {
      this.value = value;
      this.removed = removed;
    }

    @Override
    public OIndexUpdateAction<Object> update(Object persistentValue, AtomicLong bonsayFileId) {
      Set<OIdentifiable> values = (Set<OIdentifiable>) persistentValue;
      if (value == null) {
        removed.setValue(true);

        return OIndexUpdateAction.remove();
      } else if (values.remove(value)) {
        removed.setValue(true);

        if (values.isEmpty())
          return OIndexUpdateAction.remove();
        else
          return OIndexUpdateAction.changed(values);
      }

      return OIndexUpdateAction.changed(values);
    }
  }
}
