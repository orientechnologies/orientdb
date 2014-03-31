package com.orientechnologies.lucene.index;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.lucene.OLuceneIndexEngine;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexFullText;
import com.orientechnologies.orient.core.index.OIndexMultiValues;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by enricorisa on 22/03/14.
 */
public class OLuceneFullTextIndex extends OIndexFullText implements OLuceneIndex {

  public OLuceneFullTextIndex(String typeId, String algorithm, OLuceneIndexEngine indexEngine, String valueContainerAlgorithm,
      ODocument metadata) {
    super(typeId, algorithm, indexEngine, valueContainerAlgorithm);
    indexEngine.setIndexMetadata(metadata);
  }

  @Override
  public OIndexMultiValues create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    OLuceneIndexEngine engine = (OLuceneIndexEngine) indexEngine;
    engine.setManagedIndex(this);
    return super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  @Override
  public OIndexFullText put(Object key, OIdentifiable iSingleValue) {
    if (key == null)
      return this;

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();

      try {
        Set<OIdentifiable> refs = new HashSet<OIdentifiable>();

        // ADD THE CURRENT DOCUMENT AS REF FOR THAT WORD
        refs.add(iSingleValue);

        // SAVE THE INDEX ENTRY
        indexEngine.put(key, refs);

      } finally {
        releaseExclusiveLock();
      }
      return this;
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();

    try {

      acquireExclusiveLock();
      try {

        if (indexEngine instanceof OLuceneIndexEngine) {
          return ((OLuceneIndexEngine) indexEngine).remove(key, value);
        }
      } finally {
        releaseExclusiveLock();
      }

    } finally {
      modificationLock.releaseModificationLock();
    }
    return false;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {

    long size = 0;
    OLuceneIndexEngine engine = (OLuceneIndexEngine) indexEngine;
    try {
      engine.setRebuilding(true);
      super.rebuild(iProgressListener);
    } finally {
      engine.setRebuilding(false);
    }

    return size;

  }
}
