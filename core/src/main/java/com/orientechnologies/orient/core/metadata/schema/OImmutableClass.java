package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.record.impl.ODocument;

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
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/21/14
 */
public class OImmutableClass implements OClass {
  public static final String              EDGE_CLASS_NAME   = "E";
  public static final String              VERTEX_CLASS_NAME = "V";
  private boolean                         inited            = false;
  private final boolean                   isAbstract;
  private final boolean                   strictMode;

  private final String                    name;
  private final String                    streamAbleName;
  private final Map<String, OProperty>    properties;
  private Map<String, OProperty>          allPropertiesMap;
  private Collection<OProperty>           allProperties;
  private final Class<?>                  javaClass;
  private final OClusterSelectionStrategy clusterSelection;
  private final int                       defaultClusterId;
  private final int[]                     clusterIds;
  private final int[]                     polymorphicClusterIds;
  private final Collection<String>        baseClassesNames;
  private final List<String>              superClassesNames;
  private final float                     overSize;
  private final float                     classOverSize;
  private final String                    shortName;
  private final Map<String, String>       customFields;

  private final OImmutableSchema          schema;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private final List<OImmutableClass>     superClasses;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private Collection<OImmutableClass>     subclasses;
  private boolean                         restricted;
  private boolean                         isVertexType;
  private boolean                         isEdgeType;

  public OImmutableClass(OClass oClass, OImmutableSchema schema) {
    isAbstract = oClass.isAbstract();
    strictMode = oClass.isStrictMode();
    this.schema = schema;

    superClassesNames = oClass.getSuperClassesNames();
    superClasses = new ArrayList<OImmutableClass>();

    name = oClass.getName();
    streamAbleName = oClass.getStreamableName();
    clusterSelection = oClass.getClusterSelection();
    defaultClusterId = oClass.getDefaultClusterId();
    clusterIds = oClass.getClusterIds();
    polymorphicClusterIds = oClass.getPolymorphicClusterIds();

    baseClassesNames = new ArrayList<String>();
    for (OClass baseClass : oClass.getSubclasses())
      baseClassesNames.add(baseClass.getName());

    overSize = oClass.getOverSize();
    classOverSize = oClass.getClassOverSize();
    shortName = oClass.getShortName();
    javaClass = oClass.getJavaClass();

    properties = new HashMap<String, OProperty>();
    for (OProperty p : oClass.declaredProperties())
      properties.put(p.getName().toLowerCase(), new OImmutableProperty(p, this));

    Map<String, String> customFields = new HashMap<String, String>();
    for (String key : oClass.getCustomKeys())
      customFields.put(key, oClass.getCustom(key));

    this.customFields = Collections.unmodifiableMap(customFields);
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

        if (!allPropsMap.containsKey(propName))
          allPropsMap.put(propName, p);
      }

      this.allProperties = Collections.unmodifiableCollection(allProperties);
      this.allPropertiesMap = Collections.unmodifiableMap(allPropsMap);
      this.restricted = isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME);
      this.isVertexType = isSubClassOf(VERTEX_CLASS_NAME);
      this.isEdgeType = isSubClassOf(EDGE_CLASS_NAME);
      inited = true;
    }
  }

  @Override
  public <T> T newInstance() throws InstantiationException, IllegalAccessException {
    throw new UnsupportedOperationException();
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
  public OClass getSuperClass() {
    initSuperClasses();

    return superClasses.isEmpty() ? null : superClasses.get(0);
  }

  @Override
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
    for (OProperty p : properties.values())
      if (areIndexed(p.getName()))
        indexedProperties.add(p);
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

    propertyName = propertyName.toLowerCase();

    OProperty p = properties.get(propertyName);
    if (p != null)
      return p;
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
  public OProperty createProperty(String iPropertyName, OType iType, OType iLinkedType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropProperty(String iPropertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsProperty(String propertyName) {
    propertyName = propertyName.toLowerCase();
    boolean result = properties.containsKey(propertyName);
    if (result)
      return true;
    for (OImmutableClass superClass : superClasses) {
      result = superClass.existsProperty(propertyName);
      if (result)
        return true;
    }
    return false;
  }

  @Override
  public Class<?> getJavaClass() {
    return javaClass;
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
    return polymorphicClusterIds;
  }

  public OImmutableSchema getSchema() {
    return schema;
  }

  @Override
  public Collection<OClass> getSubclasses() {
    initBaseClasses();

    ArrayList<OClass> result = new ArrayList<OClass>();
    for (OClass c : subclasses)
      result.add(c);

    return result;
  }

  @Override
  public Collection<OClass> getAllSubclasses() {
    initBaseClasses();

    final Set<OClass> set = new HashSet<OClass>();
    set.addAll(getSubclasses());

    for (OImmutableClass c : subclasses)
      set.addAll(c.getAllSubclasses());

    return set;
  }

  @Override
  public Collection<OClass> getBaseClasses() {
    return getSubclasses();
  }

  @Override
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
    for (int clusterId : clusterIds)
      size += getDatabase().getClusterRecordSizeById(clusterId);

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
    if (isPolymorphic)
      return getDatabase().countClusterElements(OClassImpl.readableClusters(getDatabase(), polymorphicClusterIds));

    return getDatabase().countClusterElements(OClassImpl.readableClusters(getDatabase(), clusterIds));
  }

  @Override
  public void truncate() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSubClassOf(final String iClassName) {
    if (iClassName == null)
      return false;

    if (iClassName.equalsIgnoreCase(getName()) || iClassName.equalsIgnoreCase(getShortName()))
      return true;

    final int s = superClasses.size();
    for (int i = 0; i < s; ++i) {
      if (superClasses.get(i).isSubClassOf(iClassName))
        return true;
    }

    return false;
  }

  @Override
  public boolean isSubClassOf(final OClass clazz) {
    if (clazz == null)
      return false;
    if (equals(clazz))
      return true;

    final int s = superClasses.size();
    for (int i = 0; i < s; ++i) {
      if (superClasses.get(i).isSubClassOf(clazz))
        return true;
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
  public Object get(ATTRIBUTES iAttribute) {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

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
    }

    throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
  }

  @Override
  public OClass set(ATTRIBUTES attribute, Object iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex<?> createIndex(String iName, INDEX_TYPE iType, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex<?> createIndex(String iName, INDEX_TYPE iType, OProgressListener iProgressListener, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata,
      String algorithm, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<OIndex<?>> getInvolvedIndexes(Collection<String> fields) {
    initSuperClasses();

    final Set<OIndex<?>> result = new HashSet<OIndex<?>>(getClassInvolvedIndexes(fields));

    for (OImmutableClass superClass : superClasses) {
      result.addAll(superClass.getInvolvedIndexes(fields));
    }
    return result;
  }

  @Override
  public Set<OIndex<?>> getInvolvedIndexes(String... fields) {
    return getInvolvedIndexes(Arrays.asList(fields));
  }

  @Override
  public Set<OIndex<?>> getClassInvolvedIndexes(Collection<String> fields) {
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();
    return indexManager.getClassInvolvedIndexes(name, fields);
  }

  @Override
  public Set<OIndex<?>> getClassInvolvedIndexes(String... fields) {
    return getClassInvolvedIndexes(Arrays.asList(fields));
  }

  @Override
  public boolean areIndexed(Collection<String> fields) {
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();
    final boolean currentClassResult = indexManager.areIndexed(name, fields);

    initSuperClasses();

    if (currentClassResult)
      return true;
    for (OImmutableClass superClass : superClasses) {
      if (superClass.areIndexed(fields))
        return true;
    }
    return false;

  }

  @Override
  public boolean areIndexed(String... fields) {
    return areIndexed(Arrays.asList(fields));
  }

  @Override
  public OIndex<?> getClassIndex(String iName) {
    return getDatabase().getMetadata().getIndexManager().getClassIndex(this.name, name);
  }

  @Override
  public Set<OIndex<?>> getClassIndexes() {
    return getDatabase().getMetadata().getIndexManager().getClassIndexes(name);
  }

  @Override
  public void getClassIndexes(Collection<OIndex<?>> indexes) {
    getDatabase().getMetadata().getIndexManager().getClassIndexes(name, indexes);
  }

  @Override
  public void getIndexes(Collection<OIndex<?>> indexes) {
    initSuperClasses();

    getClassIndexes(indexes);
    for (OClass superClass : superClasses) {
      superClass.getIndexes(indexes);
    }
  }

  @Override
  public Set<OIndex<?>> getIndexes() {
    Set<OIndex<?>> indexes = new HashSet<OIndex<?>>();
    getIndexes(indexes);
    return indexes;
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
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!OClass.class.isAssignableFrom(obj.getClass()))
      return false;
    final OClass other = (OClass) obj;
    if (name == null) {
      if (other.getName() != null)
        return false;
    } else if (!name.equals(other.getName()))
      return false;
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

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
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

}
