package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;
import java.util.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/21/14
 */
public class OImmutableClass implements OClass {
  private final boolean                   isAbstract;
  private final boolean                   strictMode;
  private final String                    superClass;

  private final String                    name;
  private final String                    streamAbleName;
  private final Map<String, OProperty>    properties;
  private final Class<?>                  javaClass;
  private final OClusterSelectionStrategy clusterSelection;
  private final int                       defaultClusterId;
  private final int[]                     clusterIds;
  private final int[]                     polymorphicClusterIds;
  private final Collection<String>        baseClasses;
  private final float                     overSize;
  private final String                    shortName;
  private final Map<String, String>       customFields;
  private final OImmutableSchema          schema;

  public OImmutableClass(OClass oClass, OImmutableSchema schema) {
    isAbstract = oClass.isAbstract();
    strictMode = oClass.isStrictMode();
    this.schema = schema;

    if (oClass.getSuperClass() != null)
      superClass = oClass.getSuperClass().getName();
    else
      superClass = null;

    name = oClass.getName();
    streamAbleName = oClass.getStreamableName();
    clusterSelection = oClass.getClusterSelection();
    defaultClusterId = oClass.getDefaultClusterId();
    clusterIds = oClass.getClusterIds();
    polymorphicClusterIds = oClass.getPolymorphicClusterIds();

    baseClasses = new ArrayList<String>();
    for (OClass baseClass : oClass.getBaseClasses())
      baseClasses.add(baseClass.getName());

    overSize = oClass.getOverSize();
    shortName = oClass.getShortName();
    javaClass = oClass.getJavaClass();

    properties = new HashMap<String, OProperty>();
    Map<String, OProperty> propertyMap = oClass.propertiesMap();
    for (Map.Entry<String, OProperty> propertyEntry : propertyMap.entrySet())
      properties.put(propertyEntry.getKey(), new OImmutableProperty(propertyEntry.getValue()));

    customFields = new HashMap<String, String>();
    for (String key : oClass.getCustomKeys())
      customFields.put(key, oClass.getCustom(key));
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
    return schema.getClass(superClass);
  }

  @Override
  public OClass setSuperClass(OClass iSuperClass) {
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
    final Collection<OProperty> props = new ArrayList<OProperty>();

    OImmutableClass currentClass = this;
    do {
      props.addAll(currentClass.properties.values());

      currentClass = (OImmutableClass) schema.getClass(currentClass.superClass);

    } while (currentClass != null);

    return Collections.unmodifiableCollection(props);
  }

  @Override
  public Map<String, OProperty> propertiesMap() {
    final Map<String, OProperty> props = new HashMap<String, OProperty>(20);

    OImmutableClass currentClass = this;
    do {

      for (OProperty p : currentClass.properties.values()) {
        final String propName = p.getName();

        if (!props.containsKey(propName))
          props.put(propName, p);
      }

      currentClass = (OImmutableClass) schema.getClass(currentClass.superClass);

    } while (currentClass != null);

    return Collections.unmodifiableMap(props);
  }

  @Override
  public Collection<OProperty> getIndexedProperties() {
    Collection<OProperty> indexedProps = null;

    OImmutableClass currentClass = this;

    do {
      for (OProperty p : currentClass.properties.values())
        if (areIndexed(p.getName())) {
          if (indexedProps == null)
            indexedProps = new ArrayList<OProperty>();
          indexedProps.add(p);
        }

      currentClass = (OImmutableClass) schema.getClass(currentClass.superClass);

    } while (currentClass != null);

    return (Collection<OProperty>) (indexedProps != null ? Collections.unmodifiableCollection(indexedProps) : Collections
        .emptyList());
  }

  @Override
  public OProperty getProperty(String propertyName) {
    propertyName = propertyName.toLowerCase();

    OImmutableClass currentClass = this;
    do {
      final OProperty p = currentClass.properties.get(propertyName);

      if (p != null)
        return p;

      currentClass = (OImmutableClass) schema.getClass(currentClass.superClass);

    } while (currentClass != null);

    return null;
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
  public boolean existsProperty(String iPropertyName) {
    return properties.containsKey(iPropertyName.toLowerCase());
  }

  @Override
  public Class<?> getJavaClass() {
    return javaClass;
  }

  @Override
  public int getClusterForNewInstance() {
    return clusterSelection.getCluster(this);
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
    throw new UnsupportedOperationException();
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
  public OClass removeClusterId(int iId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getPolymorphicClusterIds() {
    return polymorphicClusterIds;
  }

  @Override
  public Collection<OClass> getBaseClasses() {
    ArrayList<OClass> result = new ArrayList<OClass>();
    for (String className : baseClasses)
      result.add(schema.getClass(className));

    return result;
  }

  @Override
  public Collection<OClass> getAllBaseClasses() {
    final Set<OClass> set = new HashSet<OClass>();
    set.addAll(getBaseClasses());

    for (String c : baseClasses)
      set.addAll(schema.getClass(c).getAllBaseClasses());

    return set;
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
  public boolean isSubClassOf(String iClassName) {
    if (iClassName == null)
      return false;

    OClass cls = this;
    do {
      if (iClassName.equalsIgnoreCase(cls.getName()) || iClassName.equalsIgnoreCase(cls.getShortName()))
        return true;

      cls = cls.getSuperClass();
    } while (cls != null);

    return false;
  }

  @Override
  public boolean isSubClassOf(OClass clazz) {
    if (clazz == null)
      return false;

    OClass cls = this;
    while (cls != null) {
      if (cls.equals(clazz))
        return true;
      cls = cls.getSuperClass();
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
    final Set<OIndex<?>> result = new HashSet<OIndex<?>>(getClassInvolvedIndexes(fields));

    if (superClass != null)
      result.addAll(schema.getClass(superClass).getInvolvedIndexes(fields));

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

    if (superClass != null)
      return currentClassResult || schema.getClass(superClass).areIndexed(fields);
    return currentClassResult;
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
  public Set<OIndex<?>> getIndexes() {
    final Set<OIndex<?>> indexes = getClassIndexes();
    for (OClass s = schema.getClass(superClass); s != null; s = s.getSuperClass()) {
      s.getClassIndexes(indexes);
    }
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
  public String getCustom(String iName) {
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
  public boolean hasClusterId(int clusterId) {
    return Arrays.binarySearch(clusterIds, clusterId) >= 0;
  }

  @Override
  public int compareTo(OClass other) {
    return name.compareTo(other.getName());
  }

  private ODatabaseRecordInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private Map<String, String> getCustomInternal() {
    return Collections.unmodifiableMap(customFields);
  }
}