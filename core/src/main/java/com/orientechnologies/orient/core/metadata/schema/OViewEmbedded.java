package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OViewEmbedded extends OViewImpl {

  protected OViewEmbedded(OSchemaShared iOwner, String iName, OViewConfig cfg, int[] iClusterIds) {
    super(iOwner, iName, cfg, iClusterIds);
  }

  protected OViewEmbedded(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  public OProperty addProperty(
      final String propertyName,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  public OClassImpl setEncryption(final String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass setClusterSelection(final String value) {
    throw new UnsupportedOperationException();
  }

  public OClassImpl setCustom(final String name, final String value) {
    throw new UnsupportedOperationException();
  }

  public void clearCustom() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass setSuperClasses(final List<? extends OClass> classes) {
    throw new UnsupportedOperationException();
  }

  protected OClass removeBaseClassInternal(final OClass baseClass) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (subclasses == null) return this;

      if (subclasses.remove(baseClass)) removePolymorphicClusterIds((OClassImpl) baseClass);

      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass addSuperClass(final OClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass removeSuperClass(OClass superClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(final List<? extends OClass> classes) {
    List<OClassImpl> newSuperClasses = new ArrayList<OClassImpl>();
    OClassImpl cls;
    for (OClass superClass : classes) {
      if (superClass instanceof OClassAbstractDelegate)
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      else cls = (OClassImpl) superClass;

      if (newSuperClasses.contains(cls)) {
        throw new OSchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<OClassImpl> toAddList = new ArrayList<OClassImpl>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<OClassImpl> toRemoveList = new ArrayList<OClassImpl>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (OClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(this);
    }
    for (OClassImpl addTo : toAddList) {
      addTo.addBaseClass(this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public OClass setName(final String name) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(final int defaultClusterId) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();
      this.defaultClusterId = defaultClusterId;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  protected OClass addClusterIdInternal(ODatabaseDocumentInternal database, final int clusterId) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      owner.checkClusterCanBeAdded(clusterId, this);

      for (int currId : clusterIds)
        if (currId == clusterId)
          // ALREADY ADDED
          return this;

      clusterIds = OArrays.copyOf(clusterIds, clusterIds.length + 1);
      clusterIds[clusterIds.length - 1] = clusterId;
      Arrays.sort(clusterIds);

      addPolymorphicClusterId(clusterId);

      if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID) defaultClusterId = clusterId;

      ((OSchemaEmbedded) owner).addClusterForView(database, clusterId, this);
      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClass setShortName(String shortName) {
    throw new UnsupportedOperationException();
  }

  protected OPropertyImpl createPropertyInstance(ODocument p) {
    return new OPropertyEmbedded(this, p);
  }

  /** {@inheritDoc} */
  @Override
  public OClass truncateCluster(String clusterName) {
    throw new UnsupportedOperationException();
  }

  public OClass setStrictMode(final boolean isStrict) {
    throw new UnsupportedOperationException();
  }

  public OClass setDescription(String iDescription) {
    throw new UnsupportedOperationException();
  }

  public OClass addClusterId(final int clusterId) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock();
    try {
      addClusterIdInternal(database, clusterId);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OClass removeClusterId(final int clusterId) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0])
      throw new ODatabaseException(
          " Impossible to remove the last cluster of class '"
              + getName()
              + "' drop the class instead");

    acquireSchemaWriteLock();
    try {
      removeClusterIdInternal(database, clusterId);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  private OClass removeClusterIdInternal(
      ODatabaseDocumentInternal database, final int clusterToRemove) {

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      boolean found = false;
      for (int clusterId : clusterIds) {
        if (clusterId == clusterToRemove) {
          found = true;
          break;
        }
      }

      if (found) {
        final int[] newClusterIds = new int[clusterIds.length - 1];
        for (int i = 0, k = 0; i < clusterIds.length; ++i) {
          if (clusterIds[i] == clusterToRemove)
            // JUMP IT
            continue;

          newClusterIds[k] = clusterIds[i];
          k++;
        }
        clusterIds = newClusterIds;

        removePolymorphicClusterId(clusterToRemove);
      }

      if (defaultClusterId == clusterToRemove) {
        if (clusterIds.length >= 1) defaultClusterId = clusterIds[0];
        else defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
      }

      ((OSchemaEmbedded) owner).removeClusterForView(database, clusterToRemove, this);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public void dropProperty(final String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass addCluster(final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public OClass setOverSize(final float overSize) {
    throw new UnsupportedOperationException();
  }

  public OClass setAbstract(boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OViewRemovedMetadata replaceViewClusterAndIndex(final int cluster, List<OIndex> indexes) {
    acquireSchemaWriteLock();
    try {
      int[] oldClusters = getClusterIds();
      addClusterId(cluster);
      for (int i : oldClusters) {
        removeClusterId(i);
      }
      List<String> oldIndexes = getInactiveIndexes();
      inactivateIndexes();
      addActiveIndexes(indexes.stream().map(x -> x.getName()).collect(Collectors.toList()));
      return new OViewRemovedMetadata(oldClusters, oldIndexes);
    } finally {
      releaseSchemaWriteLock();
    }
  }
}
