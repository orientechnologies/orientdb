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
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/21/14
 */
public class OImmutableClass implements OClass {
  /** use OClass.EDGE_CLASS_NAME instead */
  @Deprecated public static final String EDGE_CLASS_NAME = OClass.EDGE_CLASS_NAME;
  /** use OClass.EDGE_CLASS_NAME instead */
  @Deprecated public static final String VERTEX_CLASS_NAME = OClass.VERTEX_CLASS_NAME;

  private boolean inited = false;
  private final boolean isAbstract;
  private final boolean strictMode;
  private final String name;
  private final String streamAbleName;
  private final Map<String, OProperty> properties;
  private Map<String, OProperty> allPropertiesMap;
  private Collection<OProperty> allProperties;
  private final OClusterSelectionStrategy clusterSelection;
  private final int defaultClusterId;
  private final int[] clusterIds;
  private final int[] polymorphicClusterIds;
  private final Collection<String> baseClassesNames;
  private final List<String> superClassesNames;
  private final float overSize;
  private final float classOverSize;
  private final String shortName;
  private final Map<String, String> customFields;
  private final String description;

  private final OImmutableSchema schema;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private final List<OImmutableClass> superClasses;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private Collection<OImmutableClass> subclasses;
  private boolean restricted;
  private boolean isVertexType;
  private boolean isEdgeType;
  private boolean triggered;
  private boolean function;
  private boolean scheduler;
  private boolean sequence;
  private boolean ouser;
  private boolean orole;
  private boolean securityPolicy;
  private OIndex autoShardingIndex;
  private HashSet<OIndex> indexes;

  public OImmutableClass(final OClass oClass, final OImmutableSchema schema) {
    isAbstract = oClass.isAbstract();
    strictMode = oClass.isStrictMode();
    this.schema = schema;

    superClassesNames = oClass.getSuperClassesNames();
    superClasses = new ArrayList<OImmutableClass>(superClassesNames.size());

    name = oClass.getName();
    streamAbleName = oClass.getStreamableName();
    clusterSelection = oClass.getClusterSelection();
    defaultClusterId = oClass.getDefaultClusterId();
    clusterIds = oClass.getClusterIds();
    polymorphicClusterIds = oClass.getPolymorphicClusterIds();

    baseClassesNames = new ArrayList<String>();
    for (OClass baseClass : oClass.getSubclasses()) baseClassesNames.add(baseClass.getName());

    overSize = oClass.getOverSize();
    classOverSize = oClass.getClassOverSize();
    shortName = oClass.getShortName();

    properties = new HashMap<String, OProperty>();
    for (OProperty p : oClass.declaredProperties())
      properties.put(p.getName(), new OImmutableProperty(p, this));

    Map<String, String> customFields = new HashMap<String, String>();
    for (String key : oClass.getCustomKeys()) customFields.put(key, oClass.getCustom(key));

    this.customFields = Collections.unmodifiableMap(customFields);
    this.description = oClass.getDescription();
  }

  public void init() {
    if (!inited) {
      initSuperClasses();

      final Collection<OProperty> allProperties = new ArrayList<OProperty>();
      final Map<String, OProperty> allPropsMap = new HashMap<String, OProperty>(20);
      for (int i = superClasses.size() - 1; i >= 0; i--) {
        allProperties.addAll(superClasses.get(i).allProperties);
        allPropsMap.putAll(superClasses.get(i).allPropertiesMap);
      }
      allProperties.addAll(properties.values());
      for (OProperty p : properties.values()) {
        final String propName = p.getName();

        if (!allPropsMap.containsKey(propName)) allPropsMap.put(propName, p);
      }

      this.allProperties = Collections.unmodifiableCollection(allProperties);
      this.allPropertiesMap = Collections.unmodifiableMap(allPropsMap);
      this.restricted = isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME);
      this.isVertexType = isSubClassOf(OClass.VERTEX_CLASS_NAME);
      this.isEdgeType = isSubClassOf(OClass.EDGE_CLASS_NAME);
      this.triggered = isSubClassOf(OClassTrigger.CLASSNAME);
      this.function = isSubClassOf(OFunctionLibraryImpl.CLASSNAME);
      this.scheduler = isSubClassOf(OScheduledEvent.CLASS_NAME);
      this.sequence = isSubClassOf(OSequence.CLASS_NAME);
      this.ouser = isSubClassOf(OUser.CLASS_NAME);
      this.orole = isSubClassOf(ORole.CLASS_NAME);
      this.securityPolicy = OSecurityPolicy.class.getSimpleName().equals(this.name);
      this.indexes = new HashSet<>();
      getRawIndexes(indexes);

      final ODatabaseDocumentInternal db = getDatabase();
      if (db != null
          && db.getMetadata() != null
          && db.getMetadata().getIndexManagerInternal() != null) {
        this.autoShardingIndex =
            db.getMetadata().getIndexManagerInternal().getClassAutoShardingIndex(db, name);
      } else {
        this.autoShardingIndex = null;
      }
    }

    inited = true;
  }

  public boolean isSecurityPolicy() {
    return securityPolicy;
  }

  @Override
  public boolean isAbstract() {
    return isAbstract;
  }

  @Override
  public OClass setAbstract(boolean iAbstract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStrictMode() {
    return strictMode;
  }

  @Override
  public OClass setStrictMode(boolean iMode) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public OClass getSuperClass() {
    initSuperClasses();

    return superClasses.isEmpty() ? null : superClasses.get(0);
  }

  @Override
  @Deprecated
  public OClass setSuperClass(OClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OClass> getSuperClasses() {
    return Collections.unmodifiableList((List<? extends OClass>) superClasses);
  }

  @Override
  public boolean hasSuperClasses() {
    return !superClasses.isEmpty();
  }

  @Override
  public List<String> getSuperClassesNames() {
    return superClassesNames;
  }

  @Override
  public OClass setSuperClasses(List<? extends OClass> classes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass addSuperClass(OClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass removeSuperClass(OClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public OClass setName(String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStreamableName() {
    return streamAbleName;
  }

  @Override
  public Collection<OProperty> declaredProperties() {
    return Collections.unmodifiableCollection(properties.values());
  }

  @Override
  public Collection<OProperty> properties() {
    return allProperties;
  }

  @Override
  public Map<String, OProperty> propertiesMap() {
    return allPropertiesMap;
  }

  public void getIndexedProperties(Collection<OProperty> indexedProperties) {
    for (OProperty p : properties.values()) if (areIndexed(p.getName())) indexedProperties.add(p);
    initSuperClasses();
    for (OImmutableClass superClass : superClasses) {
      superClass.getIndexedProperties(indexedProperties);
    }
  }

  @Override
  public Collection<OProperty> getIndexedProperties() {
    Collection<OProperty> indexedProps = new HashSet<OProperty>();
    getIndexedProperties(indexedProps);
    return indexedProps;
  }

  @Override
  public OProperty getProperty(String propertyName) {
    initSuperClasses();

    OProperty p = properties.get(propertyName);
    if (p != null) return p;
    for (int i = 0; i < superClasses.size() && p == null; i++) {
      p = superClasses.get(i).getProperty(propertyName);
    }
    return p;
  }

  @Override
  public OProperty createProperty(String iPropertyName, OType iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OProperty createProperty(String iPropertyName, OType iType, OClass iLinkedClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OProperty createProperty(
      String iPropertyName, OType iType, OClass iLinkedClass, boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OProperty createProperty(String iPropertyName, OType iType, OType iLinkedType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OProperty createProperty(
      String iPropertyName, OType iType, OType iLinkedType, boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropProperty(String iPropertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsProperty(String propertyName) {
    boolean result = properties.containsKey(propertyName);
    if (result) return true;
    for (OImmutableClass superClass : superClasses) {
      result = superClass.existsProperty(propertyName);
      if (result) return true;
    }
    return false;
  }

  @Override
  public int getClusterForNewInstance(final ODocument doc) {
    return clusterSelection.getCluster(this, doc);
  }

  @Override
  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  @Override
  public void setDefaultClusterId(int iDefaultClusterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getClusterIds() {
    return clusterIds;
  }

  @Override
  public OClass addClusterId(int iId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClusterSelectionStrategy getClusterSelection() {
    return clusterSelection;
  }

  @Override
  public OClass setClusterSelection(OClusterSelectionStrategy clusterSelection) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass setClusterSelection(String iStrategyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass addCluster(String iClusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass truncateCluster(String clusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass removeClusterId(int iId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getPolymorphicClusterIds() {
    return Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length);
  }

  public OImmutableSchema getSchema() {
    return schema;
  }

  @Override
  public Collection<OClass> getSubclasses() {
    initBaseClasses();

    ArrayList<OClass> result = new ArrayList<OClass>();
    for (OClass c : subclasses) result.add(c);

    return result;
  }

  @Override
  public Collection<OClass> getAllSubclasses() {
    initBaseClasses();

    final Set<OClass> set = new HashSet<OClass>();
    set.addAll(getSubclasses());

    for (OImmutableClass c : subclasses) set.addAll(c.getAllSubclasses());

    return set;
  }

  @Override
  @Deprecated
  public Collection<OClass> getBaseClasses() {
    return getSubclasses();
  }

  @Override
  @Deprecated
  public Collection<OClass> getAllBaseClasses() {
    return getAllSubclasses();
  }

  @Override
  public Collection<OClass> getAllSuperClasses() {
    Set<OClass> ret = new HashSet<OClass>();
    getAllSuperClasses(ret);
    return ret;
  }

  private void getAllSuperClasses(Set<OClass> set) {
    set.addAll(superClasses);
    for (OImmutableClass superClass : superClasses) {
      superClass.getAllSuperClasses(set);
    }
  }

  @Override
  public long getSize() {
    long size = 0;
    for (int clusterId : clusterIds) size += getDatabase().getClusterRecordSizeById(clusterId);

    return size;
  }

  @Override
  public float getOverSize() {
    return overSize;
  }

  @Override
  public float getClassOverSize() {
    return classOverSize;
  }

  @Override
  public OClass setOverSize(float overSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long count() {
    return count(true);
  }

  @Override
  public long count(boolean isPolymorphic) {
    return getDatabase().countClass(getName(), isPolymorphic);
  }

  public long countImpl(boolean isPolymorphic) {
    if (isPolymorphic)
      return getDatabase()
          .countClusterElements(
              OClassImpl.readableClusters(getDatabase(), polymorphicClusterIds, name));

    return getDatabase()
        .countClusterElements(OClassImpl.readableClusters(getDatabase(), clusterIds, name));
  }

  @Override
  public void truncate() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSubClassOf(final String iClassName) {
    if (iClassName == null) return false;

    if (iClassName.equalsIgnoreCase(getName()) || iClassName.equalsIgnoreCase(getShortName()))
      return true;

    final int s = superClasses.size();
    for (int i = 0; i < s; ++i) {
      if (superClasses.get(i).isSubClassOf(iClassName)) return true;
    }

    return false;
  }

  @Override
  public boolean isSubClassOf(final OClass clazz) {
    if (clazz == null) return false;
    if (equals(clazz)) return true;

    final int s = superClasses.size();
    for (int i = 0; i < s; ++i) {
      if (superClasses.get(i).isSubClassOf(clazz)) return true;
    }
    return false;
  }

  @Override
  public boolean isSuperClassOf(OClass clazz) {
    return clazz != null && clazz.isSubClassOf(this);
  }

  @Override
  public String getShortName() {
    return shortName;
  }

  @Override
  public OClass setShortName(String shortName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public OClass setDescription(String iDescription) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    if (iAttribute == null) throw new IllegalArgumentException("attribute is null");

    switch (iAttribute) {
      case NAME:
        return getName();
      case SHORTNAME:
        return getShortName();
      case SUPERCLASS:
        return getSuperClass();
      case SUPERCLASSES:
        return getSuperClasses();
      case OVERSIZE:
        return getOverSize();
      case STRICTMODE:
        return isStrictMode();
      case ABSTRACT:
        return isAbstract();
      case CLUSTERSELECTION:
        return getClusterSelection();
      case CUSTOM:
        return getCustomInternal();
      case DESCRIPTION:
        return getDescription();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
  }

  @Override
  public OClass set(ATTRIBUTES attribute, Object iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(String iName, INDEX_TYPE iType, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(String iName, String iType, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(
      String iName, INDEX_TYPE iType, OProgressListener iProgressListener, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(
      String iName,
      String iType,
      OProgressListener iProgressListener,
      ODocument metadata,
      String algorithm,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(
      String iName,
      String iType,
      OProgressListener iProgressListener,
      ODocument metadata,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(Collection<String> fields) {
    initSuperClasses();

    final Set<OIndex> result = new HashSet<OIndex>(getClassInvolvedIndexes(fields));

    for (OImmutableClass superClass : superClasses) {
      result.addAll(superClass.getInvolvedIndexes(fields));
    }
    return result;
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(String... fields) {
    return getInvolvedIndexes(Arrays.asList(fields));
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(Collection<String> fields) {
    final ODatabaseDocumentInternal database = getDatabase();
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    return indexManager.getClassInvolvedIndexes(database, name, fields);
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(String... fields) {
    return getClassInvolvedIndexes(Arrays.asList(fields));
  }

  @Override
  public boolean areIndexed(Collection<String> fields) {
    final ODatabaseDocumentInternal database = getDatabase();
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    final boolean currentClassResult = indexManager.areIndexed(name, fields);

    initSuperClasses();

    if (currentClassResult) return true;
    for (OImmutableClass superClass : superClasses) {
      if (superClass.areIndexed(fields)) return true;
    }
    return false;
  }

  @Override
  public boolean areIndexed(String... fields) {
    return areIndexed(Arrays.asList(fields));
  }

  @Override
  public OIndex getClassIndex(String iName) {
    final ODatabaseDocumentInternal database = getDatabase();
    return database
        .getMetadata()
        .getIndexManagerInternal()
        .getClassIndex(database, this.name, iName);
  }

  @Override
  public Set<OIndex> getClassIndexes() {
    final ODatabaseDocumentInternal database = getDatabase();
    return database.getMetadata().getIndexManagerInternal().getClassIndexes(database, name);
  }

  @Override
  public void getClassIndexes(final Collection<OIndex> indexes) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.getMetadata().getIndexManagerInternal().getClassIndexes(database, name, indexes);
  }

  public void getRawClassIndexes(final Collection<OIndex> indexes) {
    getDatabase().getMetadata().getIndexManagerInternal().getClassRawIndexes(name, indexes);
  }

  @Override
  public void getIndexes(final Collection<OIndex> indexes) {
    initSuperClasses();

    getClassIndexes(indexes);
    for (OClass superClass : superClasses) {
      superClass.getIndexes(indexes);
    }
  }

  public void getRawIndexes(final Collection<OIndex> indexes) {
    initSuperClasses();

    getRawClassIndexes(indexes);
    for (OImmutableClass superClass : superClasses) {
      superClass.getRawIndexes(indexes);
    }
  }

  @Override
  public Set<OIndex> getIndexes() {
    return this.indexes;
  }

  public Set<OIndex> getRawIndexes() {
    return indexes;
  }

  @Override
  public OIndex getAutoShardingIndex() {
    return autoShardingIndex;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result;
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!OClass.class.isAssignableFrom(obj.getClass())) return false;
    final OClass other = (OClass) obj;
    if (name == null) {
      if (other.getName() != null) return false;
    } else if (!name.equals(other.getName())) return false;
    return true;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public String getCustom(final String iName) {
    return customFields.get(iName);
  }

  @Override
  public OClass setCustom(String iName, String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeCustom(String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearCustom() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(customFields.keySet());
  }

  @Override
  public boolean hasClusterId(final int clusterId) {
    return Arrays.binarySearch(clusterIds, clusterId) >= 0;
  }

  @Override
  public boolean hasPolymorphicClusterId(final int clusterId) {
    return Arrays.binarySearch(polymorphicClusterIds, clusterId) >= 0;
  }

  @Override
  public int compareTo(final OClass other) {
    return name.compareTo(other.getName());
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private Map<String, String> getCustomInternal() {
    return customFields;
  }

  private void initSuperClasses() {
    if (superClassesNames != null && superClassesNames.size() != superClasses.size()) {
      superClasses.clear();
      for (String superClassName : superClassesNames) {
        OImmutableClass superClass = (OImmutableClass) schema.getClass(superClassName);
        superClass.init();
        superClasses.add(superClass);
      }
    }
  }

  private void initBaseClasses() {
    if (subclasses == null) {
      final List<OImmutableClass> result = new ArrayList<OImmutableClass>(baseClassesNames.size());
      for (String clsName : baseClassesNames)
        result.add((OImmutableClass) schema.getClass(clsName));

      subclasses = result;
    }
  }

  public boolean isRestricted() {
    return restricted;
  }

  public boolean isEdgeType() {
    return isEdgeType;
  }

  public boolean isVertexType() {
    return isVertexType;
  }

  public boolean isTriggered() {
    return triggered;
  }

  public boolean isFunction() {
    return function;
  }

  public boolean isScheduler() {
    return scheduler;
  }

  public boolean isOuser() {
    return ouser;
  }

  public boolean isOrole() {
    return orole;
  }

  public boolean isSequence() {
    return sequence;
  }
}
