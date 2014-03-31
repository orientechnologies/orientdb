package com.orientechnologies.lucene.index;

import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.lucene.OLuceneIndexEngine;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.index.ODefaultIndexFactory;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexMultiValues;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Created by enricorisa on 21/03/14.
 */
public class OLuceneIndexNotUnique extends OIndexNotUnique implements OLuceneIndex {

  public OLuceneIndexNotUnique(String typeId, String algorithm, OLuceneIndexEngine engine, String valueContainerAlgorithm) {
    super(typeId, algorithm, engine, valueContainerAlgorithm);
  }

  @Override
  public OIndexMultiValues create(String name, OIndexDefinition indexDefinition, String clusterIndexName,
      Set<String> clustersToIndex, boolean rebuild, OProgressListener progressListener) {
    OLuceneIndexEngine engine = (OLuceneIndexEngine) indexEngine;
    engine.setManagedIndex(this);
    return super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener);
  }

  @Override
  public OIndexMultiValues put(Object key, OIdentifiable iSingleValue) {
    checkForRebuild();

    key = getCollatingValue(key);

    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        checkForKeyType(key);
        Set<OIdentifiable> values = null;
        if (ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
          values = new OIndexRIDContainer(getName());
        } else {
          values = new OMVRBTreeRIDSet(OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.getValueAsInteger());
          ((OMVRBTreeRIDSet) values).setAutoConvertToRecord(false);
        }
        if (!iSingleValue.getIdentity().isValid())
          ((ORecord<?>) iSingleValue).save();

        values.add(iSingleValue.getIdentity());

        indexEngine.put(key, values);
        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    checkForRebuild();

    key = getCollatingValue(key);
    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {

        if (indexEngine instanceof OLuceneIndexEngine) {
          return ((OLuceneIndexEngine) indexEngine).remove(key, value);
        } else {
          return false;
        }

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }
}
