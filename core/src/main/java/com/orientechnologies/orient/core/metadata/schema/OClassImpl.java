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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingClusterSelectionStrategy;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Schema Class implementation.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public abstract class OClassImpl implements OClass {
  private static final long serialVersionUID = 1L;
  protected static final int NOT_EXISTENT_CLUSTER_ID = -1;
  protected final OSchemaShared owner;
  protected final Map<String, OProperty> properties = new HashMap<String, OProperty>();
  protected int defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
  protected String name;
  protected String description;
  protected int[] clusterIds;
  protected List<OClassImpl> superClasses = new ArrayList<OClassImpl>();
  protected int[] polymorphicClusterIds;
  protected List<OClass> subclasses;
  protected float overSize = 0f;
  protected String shortName;
  protected boolean strictMode = false; // @SINCE v1.0rc8
  protected boolean abstractClass = false; // @SINCE v1.2.0
  protected Map<String, String> customFields;
  protected volatile OClusterSelectionStrategy clusterSelection; // @SINCE 1.7
  protected volatile int hashCode;

  private static Set<String> reserved = new HashSet<String>();
  protected ODocument document;

  static {
    // reserved.add("select");
    reserved.add("traverse");
    reserved.add("insert");
    reserved.add("update");
    reserved.add("delete");
    reserved.add("from");
    reserved.add("where");
    reserved.add("skip");
    reserved.add("limit");
    reserved.add("timeout");
  }

  /** Constructor used in unmarshalling. */
  protected OClassImpl(final OSchemaShared iOwner, final String iName) {
    this(iOwner, new ODocument().setTrackingChanges(false), iName);
  }

  protected OClassImpl(final OSchemaShared iOwner, final String iName, final int[] iClusterIds) {
    this(iOwner, iName);
    setClusterIds(iClusterIds);
    defaultClusterId = iClusterIds[0];
    if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID) abstractClass = true;

    if (abstractClass) setPolymorphicClusterIds(OCommonConst.EMPTY_INT_ARRAY);
    else setPolymorphicClusterIds(iClusterIds);

    clusterSelection = owner.getClusterSelectionFactory().newInstanceOfDefaultClass();
  }

  /** Constructor used in unmarshalling. */
  protected OClassImpl(final OSchemaShared iOwner, final ODocument iDocument, final String iName) {
    name = iName;
    document = iDocument;
    owner = iOwner;
  }

  public static int[] readableClusters(
      final ODatabaseDocument db, final int[] iClusterIds, String className) {
    List<Integer> listOfReadableIds = new ArrayList<Integer>();

    boolean all = true;
    for (int clusterId : iClusterIds) {
      try {
        // This will exclude (filter out) any specific classes without explicit read permission.
        if (className != null)
          db.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, className);

        final String clusterName = db.getClusterNameById(clusterId);
        db.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, clusterName);
        listOfReadableIds.add(clusterId);
      } catch (OSecurityAccessException ignore) {
        all = false;
        // if the cluster is inaccessible it's simply not processed in the list.add
      }
    }

    if (all)
      // JUST RETURN INPUT ARRAY (FASTER)
      return iClusterIds;

    final int[] readableClusterIds = new int[listOfReadableIds.size()];
    int index = 0;
    for (int clusterId : listOfReadableIds) {
      readableClusterIds[index++] = clusterId;
    }

    return readableClusterIds;
  }

  @Override
  public OClusterSelectionStrategy getClusterSelection() {
    acquireSchemaReadLock();
    try {
      return clusterSelection;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OClass setClusterSelection(final OClusterSelectionStrategy clusterSelection) {
    return setClusterSelection(clusterSelection.getName());
  }

  public String getCustom(final String iName) {
    acquireSchemaReadLock();
    try {
      if (customFields == null) return null;

      return customFields.get(iName);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, String> getCustomInternal() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) return Collections.unmodifiableMap(customFields);
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void removeCustom(final String name) {
    setCustom(name, null);
  }

  public Set<String> getCustomKeys() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) return Collections.unmodifiableSet(customFields.keySet());
      return new HashSet<String>();
    } finally {
      releaseSchemaReadLock();
    }
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
  @Deprecated
  public OClass getSuperClass() {
    acquireSchemaReadLock();
    try {
      return superClasses.isEmpty() ? null : superClasses.get(0);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  @Deprecated
  public OClass setSuperClass(OClass iSuperClass) {
    setSuperClasses(iSuperClass != null ? Arrays.asList(iSuperClass) : Collections.EMPTY_LIST);
    return this;
  }

  public String getName() {
    acquireSchemaReadLock();
    try {
      return name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public List<OClass> getSuperClasses() {
    acquireSchemaReadLock();
    try {
      return Collections.unmodifiableList((List<? extends OClass>) superClasses);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public boolean hasSuperClasses() {
    acquireSchemaReadLock();
    try {
      return !superClasses.isEmpty();
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public List<String> getSuperClassesNames() {
    acquireSchemaReadLock();
    try {
      List<String> superClassesNames = new ArrayList<String>(superClasses.size());
      for (OClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      return superClassesNames;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass setSuperClassesByNames(List<String> classNames) {
    if (classNames == null) classNames = Collections.EMPTY_LIST;

    final List<OClass> classes = new ArrayList<OClass>(classNames.size());
    final OSchema schema = getDatabase().getMetadata().getSchema();
    for (String className : classNames) {
      classes.add(schema.getClass(decodeClassName(className)));
    }
    return setSuperClasses(classes);
  }

  protected abstract void setSuperClassesInternal(final List<? extends OClass> classes);

  public long getSize() {
    acquireSchemaReadLock();
    try {
      long size = 0;
      for (int clusterId : clusterIds) size += getDatabase().getClusterRecordSizeById(clusterId);

      return size;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getShortName() {
    acquireSchemaReadLock();
    try {
      return shortName;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getDescription() {
    acquireSchemaReadLock();
    try {
      return description;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getStreamableName() {
    acquireSchemaReadLock();
    try {
      return shortName != null ? shortName : name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<OProperty> declaredProperties() {
    acquireSchemaReadLock();
    try {
      return Collections.unmodifiableCollection(properties.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, OProperty> propertiesMap() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Map<String, OProperty> props = new HashMap<String, OProperty>(20);
      propertiesMap(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void propertiesMap(Map<String, OProperty> propertiesMap) {
    for (OProperty p : properties.values()) {
      String propName = p.getName();
      if (!propertiesMap.containsKey(propName)) propertiesMap.put(propName, p);
    }
    for (OClassImpl superClass : superClasses) {
      superClass.propertiesMap(propertiesMap);
    }
  }

  public Collection<OProperty> properties() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Collection<OProperty> props = new ArrayList<OProperty>();
      properties(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void properties(Collection<OProperty> properties) {
    properties.addAll(this.properties.values());
    for (OClassImpl superClass : superClasses) {
      superClass.properties(properties);
    }
  }

  public void getIndexedProperties(Collection<OProperty> indexedProperties) {
    for (OProperty p : properties.values()) if (areIndexed(p.getName())) indexedProperties.add(p);
    for (OClassImpl superClass : superClasses) {
      superClass.getIndexedProperties(indexedProperties);
    }
  }

  @Override
  public Collection<OProperty> getIndexedProperties() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      Collection<OProperty> indexedProps = new HashSet<OProperty>();
      getIndexedProperties(indexedProps);
      return indexedProps;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OProperty getProperty(String propertyName) {
    acquireSchemaReadLock();
    try {

      OProperty p = properties.get(propertyName);
      if (p != null) return p;
      for (int i = 0; i < superClasses.size() && p == null; i++) {
        p = superClasses.get(i).getProperty(propertyName);
      }
      return p;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OProperty createProperty(final String iPropertyName, final OType iType) {
    return addProperty(iPropertyName, iType, null, null, false);
  }

  public OProperty createProperty(
      final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    return addProperty(iPropertyName, iType, null, iLinkedClass, false);
  }

  public OProperty createProperty(
      final String iPropertyName,
      final OType iType,
      final OClass iLinkedClass,
      final boolean unsafe) {
    return addProperty(iPropertyName, iType, null, iLinkedClass, unsafe);
  }

  public OProperty createProperty(
      final String iPropertyName, final OType iType, final OType iLinkedType) {
    return addProperty(iPropertyName, iType, iLinkedType, null, false);
  }

  public OProperty createProperty(
      final String iPropertyName,
      final OType iType,
      final OType iLinkedType,
      final boolean unsafe) {
    return addProperty(iPropertyName, iType, iLinkedType, null, unsafe);
  }

  @Override
  public boolean existsProperty(String propertyName) {
    acquireSchemaReadLock();
    try {
      boolean result = properties.containsKey(propertyName);
      if (result) return true;
      for (OClassImpl superClass : superClasses) {
        result = superClass.existsProperty(propertyName);
        if (result) return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void fromStream() {
    subclasses = null;
    superClasses.clear();

    name = document.field("name");
    if (document.containsField("shortName")) shortName = document.field("shortName");
    else shortName = null;
    if (document.containsField("description")) description = document.field("description");
    else description = null;
    defaultClusterId = document.field("defaultClusterId");
    if (document.containsField("strictMode")) strictMode = document.field("strictMode");
    else strictMode = false;

    if (document.containsField("abstract")) abstractClass = document.field("abstract");
    else abstractClass = false;

    if (document.field("overSize") != null) overSize = document.field("overSize");
    else overSize = 0f;

    final Object cc = document.field("clusterIds");
    if (cc instanceof Collection<?>) {
      final Collection<Integer> coll = document.field("clusterIds");
      clusterIds = new int[coll.size()];
      int i = 0;
      for (final Integer item : coll) clusterIds[i++] = item;
    } else clusterIds = (int[]) cc;
    Arrays.sort(clusterIds);

    if (clusterIds.length == 1 && clusterIds[0] == -1)
      setPolymorphicClusterIds(OCommonConst.EMPTY_INT_ARRAY);
    else setPolymorphicClusterIds(clusterIds);

    // READ PROPERTIES
    OPropertyImpl prop;

    final Map<String, OProperty> newProperties = new HashMap<String, OProperty>();
    final Collection<ODocument> storedProperties = document.field("properties");

    if (storedProperties != null)
      for (OIdentifiable id : storedProperties) {
        ODocument p = id.getRecord();
        String name = p.field("name");
        // To lower case ?
        if (properties.containsKey(name)) {
          prop = (OPropertyImpl) properties.get(name);
          prop.fromStream(p);
        } else {
          prop = createPropertyInstance(p);
          prop.fromStream();
        }

        newProperties.put(prop.getName(), prop);
      }

    properties.clear();
    properties.putAll(newProperties);
    customFields = document.field("customFields", OType.EMBEDDEDMAP);
    clusterSelection =
        owner.getClusterSelectionFactory().getStrategy((String) document.field("clusterSelection"));
  }

  protected abstract OPropertyImpl createPropertyInstance(ODocument p);

  public ODocument toStream() {
    document.field("name", name);
    document.field("shortName", shortName);
    document.field("description", description);
    document.field("defaultClusterId", defaultClusterId);
    document.field("clusterIds", clusterIds);
    document.field("clusterSelection", clusterSelection.getName());
    document.field("overSize", overSize);
    document.field("strictMode", strictMode);
    document.field("abstract", abstractClass);

    final Set<ODocument> props = new LinkedHashSet<ODocument>();
    for (final OProperty p : properties.values()) {
      props.add(((OPropertyImpl) p).toStream());
    }
    document.field("properties", props, OType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      document.field("superClass", null, OType.STRING);
      document.field("superClasses", null, OType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      document.field("superClass", superClasses.get(0).getName(), OType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (OClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      document.field("superClasses", superClassesNames, OType.EMBEDDEDLIST);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        OType.EMBEDDEDMAP);

    return document;
  }

  @Override
  public int getClusterForNewInstance(final ODocument doc) {
    acquireSchemaReadLock();
    try {
      return clusterSelection.getCluster(this, doc);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int getDefaultClusterId() {
    acquireSchemaReadLock();
    try {
      return defaultClusterId;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int[] getClusterIds() {
    acquireSchemaReadLock();
    try {
      return clusterIds;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int[] getPolymorphicClusterIds() {
    acquireSchemaReadLock();
    try {
      return Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length);
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void setPolymorphicClusterIds(final int[] iClusterIds) {
    Set<Integer> set = new TreeSet<Integer>();
    for (int iClusterId : iClusterIds) {
      set.add(iClusterId);
    }
    polymorphicClusterIds = new int[set.size()];
    int i = 0;
    for (Integer clusterId : set) {
      polymorphicClusterIds[i] = clusterId;
      i++;
    }
  }

  public void renameProperty(final String iOldName, final String iNewName) {
    final OProperty p = properties.remove(iOldName);
    if (p != null) properties.put(iNewName, p);
  }

  public static OClass addClusters(final OClass cls, final int iClusters) {
    final String clusterBase = cls.getName().toLowerCase(Locale.ENGLISH) + "_";
    for (int i = 0; i < iClusters; ++i) {
      cls.addCluster(clusterBase + i);
    }
    return cls;
  }

  protected void truncateClusterInternal(
      final String clusterName, final ODatabaseDocumentInternal database) {
    database.checkForClusterPermissions(clusterName);

    final ORecordIteratorCluster<ORecord> iteratorCluster = database.browseCluster(clusterName);
    if (iteratorCluster == null) {
      throw new ODatabaseException("Cluster with name " + clusterName + " does not exist");
    }
    while (iteratorCluster.hasNext()) {
      final ORecord record = iteratorCluster.next();
      record.delete();
    }
  }

  public Collection<OClass> getSubclasses() {
    acquireSchemaReadLock();
    try {
      if (subclasses == null || subclasses.size() == 0) return Collections.emptyList();

      return Collections.unmodifiableCollection(subclasses);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<OClass> getAllSubclasses() {
    acquireSchemaReadLock();
    try {
      final Set<OClass> set = new HashSet<OClass>();
      if (subclasses != null) {
        set.addAll(subclasses);

        for (OClass c : subclasses) set.addAll(c.getAllSubclasses());
      }
      return set;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Deprecated
  public Collection<OClass> getBaseClasses() {
    return getSubclasses();
  }

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
    for (OClassImpl superClass : superClasses) {
      superClass.getAllSuperClasses(set);
    }
  }

  protected abstract OClass removeBaseClassInternal(final OClass baseClass);

  public float getOverSize() {
    acquireSchemaReadLock();
    try {
      if (overSize > 0)
        // CUSTOM OVERSIZE SET
        return overSize;

      // NO OVERSIZE by default
      float maxOverSize = 0;
      float thisOverSize;
      for (OClassImpl superClass : superClasses) {
        thisOverSize = superClass.getOverSize();
        if (thisOverSize > maxOverSize) maxOverSize = thisOverSize;
      }
      return maxOverSize;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public float getClassOverSize() {
    acquireSchemaReadLock();
    try {
      return overSize;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isAbstract() {
    acquireSchemaReadLock();
    try {
      return abstractClass;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isStrictMode() {
    acquireSchemaReadLock();
    try {
      return strictMode;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public String toString() {
    acquireSchemaReadLock();
    try {
      return name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public boolean equals(Object obj) {
    acquireSchemaReadLock();
    try {
      if (this == obj) return true;
      if (obj == null) return false;
      if (!OClass.class.isAssignableFrom(obj.getClass())) return false;
      final OClass other = (OClass) obj;
      if (name == null) {
        if (other.getName() != null) return false;
      } else if (!name.equals(other.getName())) return false;

      return true;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public int hashCode() {
    String name = this.name;
    if (name != null) {
      return name.hashCode();
    }
    return 0;
  }

  public int compareTo(final OClass o) {
    acquireSchemaReadLock();
    try {
      return name.compareTo(o.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public long count() {
    return count(true);
  }

  public long count(final boolean isPolymorphic) {
    acquireSchemaReadLock();
    try {
      return getDatabase().countClass(getName(), isPolymorphic);
    } finally {
      releaseSchemaReadLock();
    }
  }

  /** Truncates all the clusters the class uses. */
  public void truncate() {
    ODatabaseDocumentInternal db = getDatabase();
    db.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_UPDATE);

    if (isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {
      throw new OSecurityException(
          "Class '"
              + getName()
              + "' cannot be truncated because has record level security enabled (extends '"
              + OSecurityShared.RESTRICTED_CLASSNAME
              + "')");
    }

    acquireSchemaReadLock();
    try {

      for (int id : clusterIds) {
        if (id < 0) continue;
        final String clusterName = db.getClusterNameById(id);
        if (clusterName == null) continue;
        db.checkForClusterPermissions(clusterName);

        final ORecordIteratorCluster<ORecord> iteratorCluster = db.browseCluster(clusterName);
        if (iteratorCluster == null) {
          throw new ODatabaseException("Cluster with name " + clusterName + " does not exist");
        }
        while (iteratorCluster.hasNext()) {
          final ORecord record = iteratorCluster.next();
          record.delete();
        }
      }
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Check if the current instance extends specified schema class.
   *
   * @param iClassName of class that should be checked
   * @return Returns true if the current instance extends the passed schema class (iClass)
   * @see #isSuperClassOf(OClass)
   */
  public boolean isSubClassOf(final String iClassName) {
    acquireSchemaReadLock();
    try {
      if (iClassName == null) return false;

      if (iClassName.equalsIgnoreCase(getName()) || iClassName.equalsIgnoreCase(getShortName()))
        return true;
      for (OClassImpl superClass : superClasses) {
        if (superClass.isSubClassOf(iClassName)) return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Check if the current instance extends specified schema class.
   *
   * @param clazz to check
   * @return true if the current instance extends the passed schema class (iClass)
   * @see #isSuperClassOf(OClass)
   */
  public boolean isSubClassOf(final OClass clazz) {
    acquireSchemaReadLock();
    try {
      if (clazz == null) return false;
      if (equals(clazz)) return true;
      for (OClassImpl superClass : superClasses) {
        if (superClass.isSubClassOf(clazz)) return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   *
   * @param clazz to check
   * @return Returns true if the passed schema class extends the current instance
   * @see #isSubClassOf(OClass)
   */
  public boolean isSuperClassOf(final OClass clazz) {
    return clazz != null && clazz.isSubClassOf(this);
  }

  public Object get(final ATTRIBUTES iAttribute) {
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

  public OClass set(final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null) throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;
    final boolean isNull = stringValue == null || stringValue.equalsIgnoreCase("NULL");

    switch (attribute) {
      case NAME:
        setName(decodeClassName(stringValue));
        break;
      case SHORTNAME:
        setShortName(decodeClassName(stringValue));
        break;
      case SUPERCLASS:
        if (stringValue == null) throw new IllegalArgumentException("Superclass is null");

        if (stringValue.startsWith("+")) {
          addSuperClass(
              getDatabase()
                  .getMetadata()
                  .getSchema()
                  .getClass(decodeClassName(stringValue.substring(1))));
        } else if (stringValue.startsWith("-")) {
          removeSuperClass(
              getDatabase()
                  .getMetadata()
                  .getSchema()
                  .getClass(decodeClassName(stringValue.substring(1))));
        } else {
          setSuperClass(
              getDatabase().getMetadata().getSchema().getClass(decodeClassName(stringValue)));
        }
        break;
      case SUPERCLASSES:
        setSuperClassesByNames(
            stringValue != null ? Arrays.asList(stringValue.split(",\\s*")) : null);
        break;
      case OVERSIZE:
        setOverSize(Float.parseFloat(stringValue));
        break;
      case STRICTMODE:
        setStrictMode(Boolean.parseBoolean(stringValue));
        break;
      case ABSTRACT:
        setAbstract(Boolean.parseBoolean(stringValue));
        break;
      case ADDCLUSTER:
        {
          addCluster(stringValue);
          break;
        }
      case REMOVECLUSTER:
        int clId = owner.getClusterId(getDatabase(), stringValue);
        if (clId == NOT_EXISTENT_CLUSTER_ID)
          throw new IllegalArgumentException("Cluster id '" + stringValue + "' cannot be removed");
        removeClusterId(clId);
        break;
      case CLUSTERSELECTION:
        setClusterSelection(stringValue);
        break;
      case CUSTOM:
        int indx = stringValue != null ? stringValue.indexOf('=') : -1;
        if (indx < 0) {
          if (isNull || "clear".equalsIgnoreCase(stringValue)) {
            clearCustom();
          } else
            throw new IllegalArgumentException(
                "Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
        } else {
          String customName = stringValue.substring(0, indx).trim();
          String customValue = stringValue.substring(indx + 1).trim();
          if (isQuoted(customValue)) {
            customValue = removeQuotes(customValue);
          }
          if (customValue.isEmpty()) removeCustom(customName);
          else setCustom(customName, customValue);
        }
        break;
      case DESCRIPTION:
        setDescription(stringValue);
        break;
      case ENCRYPTION:
        setEncryption(stringValue);
        break;
    }
    return this;
  }

  private String removeQuotes(String s) {
    s = s.trim();
    return s.substring(1, s.length() - 1);
  }

  private boolean isQuoted(String s) {
    s = s.trim();
    if (s.startsWith("\"") && s.endsWith("\"")) return true;
    if (s.startsWith("'") && s.endsWith("'")) return true;
    if (s.startsWith("`") && s.endsWith("`")) return true;

    return false;
  }

  public abstract OClassImpl setEncryption(final String iValue);

  protected void setEncryptionInternal(ODatabaseDocumentInternal database, final String value) {
    for (int cl : getClusterIds()) {
      final OStorage storage = database.getStorage();
      storage.setClusterAttribute(cl, OCluster.ATTRIBUTES.ENCRYPTION, value);
    }
  }

  public OIndex createIndex(final String iName, final INDEX_TYPE iType, final String... fields) {
    return createIndex(iName, iType.name(), fields);
  }

  public OIndex createIndex(final String iName, final String iType, final String... fields) {
    return createIndex(iName, iType, null, null, fields);
  }

  public OIndex createIndex(
      final String iName,
      final INDEX_TYPE iType,
      final OProgressListener iProgressListener,
      final String... fields) {
    return createIndex(iName, iType.name(), iProgressListener, null, fields);
  }

  public OIndex createIndex(
      String iName,
      String iType,
      OProgressListener iProgressListener,
      ODocument metadata,
      String... fields) {
    return createIndex(iName, iType, iProgressListener, metadata, null, fields);
  }

  public OIndex createIndex(
      final String name,
      String type,
      final OProgressListener progressListener,
      ODocument metadata,
      String algorithm,
      final String... fields) {
    if (type == null) throw new IllegalArgumentException("Index type is null");

    type = type.toUpperCase(Locale.ENGLISH);

    if (fields.length == 0) {
      throw new OIndexException("List of fields to index cannot be empty.");
    }

    final String localName = this.name;
    final int[] localPolymorphicClusterIds = polymorphicClusterIds;

    for (final String fieldToIndex : fields) {
      final String fieldName =
          decodeClassName(OIndexDefinitionFactory.extractFieldName(fieldToIndex));

      if (!fieldName.equals("@rid") && !existsProperty(fieldName))
        throw new OIndexException(
            "Index with name '"
                + name
                + "' cannot be created on class '"
                + localName
                + "' because the field '"
                + fieldName
                + "' is absent in class definition");
    }

    final OIndexDefinition indexDefinition =
        OIndexDefinitionFactory.createIndexDefinition(
            this, Arrays.asList(fields), extractFieldTypes(fields), null, type, algorithm);

    return getDatabase()
        .getMetadata()
        .getIndexManagerInternal()
        .createIndex(
            getDatabase(),
            name,
            type,
            indexDefinition,
            localPolymorphicClusterIds,
            progressListener,
            metadata,
            algorithm);
  }

  public boolean areIndexed(final String... fields) {
    return areIndexed(Arrays.asList(fields));
  }

  public boolean areIndexed(final Collection<String> fields) {
    final OIndexManagerAbstract indexManager =
        getDatabase().getMetadata().getIndexManagerInternal();

    acquireSchemaReadLock();
    try {
      final boolean currentClassResult = indexManager.areIndexed(name, fields);

      if (currentClassResult) return true;
      for (OClassImpl superClass : superClasses) {
        if (superClass.areIndexed(fields)) return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getInvolvedIndexes(final String... fields) {
    return getInvolvedIndexes(Arrays.asList(fields));
  }

  public Set<OIndex> getInvolvedIndexes(final Collection<String> fields) {
    acquireSchemaReadLock();
    try {
      final Set<OIndex> result = new HashSet<OIndex>(getClassInvolvedIndexes(fields));

      for (OClassImpl superClass : superClasses) {
        result.addAll(superClass.getInvolvedIndexes(fields));
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getClassInvolvedIndexes(final Collection<String> fields) {

    final ODatabaseDocumentInternal database = getDatabase();
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    acquireSchemaReadLock();
    try {
      return indexManager.getClassInvolvedIndexes(database, name, fields);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getClassInvolvedIndexes(final String... fields) {
    return getClassInvolvedIndexes(Arrays.asList(fields));
  }

  public OIndex getClassIndex(final String name) {
    acquireSchemaReadLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      return database
          .getMetadata()
          .getIndexManagerInternal()
          .getClassIndex(database, this.name, name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getClassIndexes() {
    acquireSchemaReadLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) return new HashSet<>();

      return idxManager.getClassIndexes(database, name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public void getClassIndexes(final Collection<OIndex> indexes) {
    acquireSchemaReadLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) return;

      idxManager.getClassIndexes(database, name, indexes);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OIndex getAutoShardingIndex() {
    final ODatabaseDocumentInternal db = getDatabase();
    return db != null
        ? db.getMetadata().getIndexManagerInternal().getClassAutoShardingIndex(db, name)
        : null;
  }

  @Override
  public boolean isEdgeType() {
    return isSubClassOf(EDGE_CLASS_NAME);
  }

  @Override
  public boolean isVertexType() {
    return isSubClassOf(VERTEX_CLASS_NAME);
  }

  public void onPostIndexManagement() {
    final OIndex autoShardingIndex = getAutoShardingIndex();
    if (autoShardingIndex != null) {
      if (!getDatabase().isRemote()) {
        // OVERRIDE CLUSTER SELECTION
        acquireSchemaWriteLock();
        try {
          this.clusterSelection =
              new OAutoShardingClusterSelectionStrategy(this, autoShardingIndex);
        } finally {
          releaseSchemaWriteLock();
        }
      }
    } else if (clusterSelection instanceof OAutoShardingClusterSelectionStrategy) {
      // REMOVE AUTO SHARDING CLUSTER SELECTION
      acquireSchemaWriteLock();
      try {
        this.clusterSelection = owner.getClusterSelectionFactory().newInstanceOfDefaultClass();
      } finally {
        releaseSchemaWriteLock();
      }
    }
  }

  @Override
  public void getIndexes(final Collection<OIndex> indexes) {
    acquireSchemaReadLock();
    try {
      getClassIndexes(indexes);
      for (OClass superClass : superClasses) {
        superClass.getIndexes(indexes);
      }
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getIndexes() {
    final Set<OIndex> indexes = new HashSet<OIndex>();
    getIndexes(indexes);
    return indexes;
  }

  public void acquireSchemaReadLock() {
    owner.acquireSchemaReadLock();
  }

  public void releaseSchemaReadLock() {
    owner.releaseSchemaReadLock();
  }

  public void acquireSchemaWriteLock() {
    owner.acquireSchemaWriteLock(getDatabase());
  }

  public void releaseSchemaWriteLock() {
    releaseSchemaWriteLock(true);
  }

  public void releaseSchemaWriteLock(final boolean iSave) {
    calculateHashCode();
    owner.releaseSchemaWriteLock(getDatabase(), iSave);
  }

  public void checkEmbedded() {
    owner.checkEmbedded();
  }

  public void setClusterSelectionInternal(final OClusterSelectionStrategy iClusterSelection) {
    // AVOID TO CHECK THIS IN LOCK TO AVOID RE-GENERATION OF IMMUTABLE SCHEMAS
    if (this.clusterSelection.getClass().equals(iClusterSelection.getClass()))
      // NO CHANGES
      return;

    // DON'T GET THE SCHEMA LOCK BECAUSE THIS CHANGE IS USED ONLY TO WRAP THE SELECTION STRATEGY
    checkEmbedded();
    this.clusterSelection = iClusterSelection;
  }

  public void fireDatabaseMigration(
      final ODatabaseDocument database, final String propertyName, final OType type) {
    final boolean strictSQL =
        ((ODatabaseInternal) database).getStorageInfo().getConfiguration().isStrictSql();

    try (OResultSet result =
        database.query(
            "select from "
                + getEscapedName(name, strictSQL)
                + " where "
                + getEscapedName(propertyName, strictSQL)
                + ".type() <> \""
                + type.name()
                + "\"")) {
      while (result.hasNext()) {
        ODocument record = (ODocument) result.next().getElement().get();
        record.field(propertyName, record.field(propertyName), type);
        database.save(record);
      }
    }
  }

  public void firePropertyNameMigration(
      final ODatabaseDocument database,
      final String propertyName,
      final String newPropertyName,
      final OType type) {
    final boolean strictSQL =
        ((ODatabaseInternal) database).getStorageInfo().getConfiguration().isStrictSql();

    try (OResultSet result =
        database.query(
            "select from "
                + getEscapedName(name, strictSQL)
                + " where "
                + getEscapedName(propertyName, strictSQL)
                + " is not null ")) {
      while (result.hasNext()) {
        ODocument record = (ODocument) result.next().getElement().get();
        record.setFieldType(propertyName, type);
        record.field(newPropertyName, record.field(propertyName), type);
        database.save(record);
      }
    }
  }

  public void checkPersistentPropertyType(
      final ODatabaseInternal<ORecord> database,
      final String propertyName,
      final OType type,
      OClass linkedClass) {
    if (OType.ANY.equals(type)) {
      return;
    }
    final boolean strictSQL = database.getStorageInfo().getConfiguration().isStrictSql();

    final StringBuilder builder = new StringBuilder(256);
    builder.append("select from ");
    builder.append(getEscapedName(name, strictSQL));
    builder.append(" where ");
    builder.append(getEscapedName(propertyName, strictSQL));
    builder.append(".type() not in [");

    final Iterator<OType> cur = type.getCastable().iterator();
    while (cur.hasNext()) {
      builder.append('"').append(cur.next().name()).append('"');
      if (cur.hasNext()) builder.append(",");
    }
    builder
        .append("] and ")
        .append(getEscapedName(propertyName, strictSQL))
        .append(" is not null ");
    if (type.isMultiValue())
      builder
          .append(" and ")
          .append(getEscapedName(propertyName, strictSQL))
          .append(".size() <> 0 limit 1");

    try (final OResultSet res = database.command(builder.toString())) {
      if (res.hasNext())
        throw new OSchemaException(
            "The database contains some schema-less data in the property '"
                + name
                + "."
                + propertyName
                + "' that is not compatible with the type "
                + type
                + ". Fix those records and change the schema again");
    }

    if (linkedClass != null) {
      checkAllLikedObjects(database, propertyName, type, linkedClass);
    }
  }

  protected void checkAllLikedObjects(
      ODatabaseInternal<ORecord> database, String propertyName, OType type, OClass linkedClass) {
    final StringBuilder builder = new StringBuilder(256);
    builder.append("select from ");
    builder.append(getEscapedName(name, true));
    builder.append(" where ");
    builder.append(getEscapedName(propertyName, true)).append(" is not null ");
    if (type.isMultiValue())
      builder.append(" and ").append(getEscapedName(propertyName, true)).append(".size() > 0");

    try (final OResultSet res = database.command(builder.toString())) {
      while (res.hasNext()) {
        OResult item = res.next();
        switch (type) {
          case EMBEDDEDLIST:
          case LINKLIST:
          case EMBEDDEDSET:
          case LINKSET:
            try {
              Collection emb = item.getElement().get().getProperty(propertyName);
              emb.stream()
                  .filter(x -> !matchesType(x, linkedClass))
                  .findFirst()
                  .ifPresent(
                      x -> {
                        throw new OSchemaException(
                            "The database contains some schema-less data in the property '"
                                + name
                                + "."
                                + propertyName
                                + "' that is not compatible with the type "
                                + type
                                + " "
                                + linkedClass.getName()
                                + ". Fix those records and change the schema again. "
                                + x);
                      });
            } catch (OSchemaException e1) {
              throw e1;
            } catch (Exception e) {
            }
            break;
          case EMBEDDED:
          case LINK:
            Object elem = item.getProperty(propertyName);
            if (!matchesType(elem, linkedClass)) {
              throw new OSchemaException(
                  "The database contains some schema-less data in the property '"
                      + name
                      + "."
                      + propertyName
                      + "' that is not compatible with the type "
                      + type
                      + " "
                      + linkedClass.getName()
                      + ". Fix those records and change the schema again!");
            }
            break;
        }
      }
    }
  }

  protected boolean matchesType(Object x, OClass linkedClass) {
    if (x instanceof OResult) {
      x = ((OResult) x).toElement();
    }
    if (x instanceof ORID) {
      x = ((ORID) x).getRecord();
    }
    if (x == null) {
      return true;
    }
    if (!(x instanceof OElement)) {
      return false;
    }
    if (x instanceof ODocument
        && !linkedClass.getName().equalsIgnoreCase(((ODocument) x).getClassName())) {
      return false;
    }
    return true;
  }

  protected String getEscapedName(final String iName, final boolean iStrictSQL) {
    if (iStrictSQL)
      // ESCAPE NAME
      return "`" + iName + "`";
    return iName;
  }

  public OSchemaShared getOwner() {
    return owner;
  }

  private void calculateHashCode() {
    int result = super.hashCode();
    result = 31 * result + (name != null ? name.hashCode() : 0);
    hashCode = result;
  }

  protected void renameCluster(String oldName, String newName) {
    oldName = oldName.toLowerCase(Locale.ENGLISH);
    newName = newName.toLowerCase(Locale.ENGLISH);

    final ODatabaseDocumentInternal database = getDatabase();

    if (database.getClusterIdByName(newName) != -1) return;

    final int clusterId = database.getClusterIdByName(oldName);
    if (clusterId == -1) return;

    if (!hasClusterId(clusterId)) return;

    database.command("alter cluster `" + oldName + "` NAME \"" + newName + "\"").close();
  }

  protected void addPolymorphicClusterId(int clusterId) {
    if (Arrays.binarySearch(polymorphicClusterIds, clusterId) >= 0) return;

    polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
    polymorphicClusterIds[polymorphicClusterIds.length - 1] = clusterId;
    Arrays.sort(polymorphicClusterIds);

    addClusterIdToIndexes(clusterId);

    for (OClassImpl superClass : superClasses) {
      superClass.addPolymorphicClusterId(clusterId);
    }
  }

  protected abstract OProperty addProperty(
      final String propertyName,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe);

  protected void validatePropertyName(final String propertyName) {}

  private int getClusterId(final String stringValue) {
    int clId;
    if (!stringValue.isEmpty() && Character.isDigit(stringValue.charAt(0)))
      try {
        clId = Integer.parseInt(stringValue);
      } catch (NumberFormatException ignore) {
        clId = getDatabase().getClusterIdByName(stringValue);
      }
    else clId = getDatabase().getClusterIdByName(stringValue);

    return clId;
  }

  private void addClusterIdToIndexes(int iId) {
    ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null && database.getStorage() instanceof OAbstractPaginatedStorage) {
      final String clusterName = getDatabase().getClusterNameById(iId);
      final List<String> indexesToAdd = new ArrayList<String>();

      for (OIndex index : getIndexes()) indexesToAdd.add(index.getName());

      final OIndexManagerAbstract indexManager =
          getDatabase().getMetadata().getIndexManagerInternal();
      for (String indexName : indexesToAdd) indexManager.addClusterToIndex(clusterName, indexName);
    }
  }

  /**
   * Adds a base class to the current one. It adds also the base class cluster ids to the
   * polymorphic cluster ids array.
   *
   * @param iBaseClass The base class to add.
   */
  protected OClass addBaseClass(final OClassImpl iBaseClass) {
    checkRecursion(iBaseClass);

    if (subclasses == null) subclasses = new ArrayList<OClass>();

    if (subclasses.contains(iBaseClass)) return this;

    subclasses.add(iBaseClass);
    addPolymorphicClusterIdsWithInheritance(iBaseClass);
    return this;
  }

  protected void checkParametersConflict(final OClass baseClass) {
    final Collection<OProperty> baseClassProperties = baseClass.properties();
    for (OProperty property : baseClassProperties) {
      OProperty thisProperty = getProperty(property.getName());
      if (thisProperty != null && !thisProperty.getType().equals(property.getType())) {
        throw new OSchemaException(
            "Cannot add base class '"
                + baseClass.getName()
                + "', because of property conflict: '"
                + thisProperty
                + "' vs '"
                + property
                + "'");
      }
    }
  }

  public static void checkParametersConflict(List<OClass> classes) {
    final Map<String, OProperty> comulative = new HashMap<String, OProperty>();
    final Map<String, OProperty> properties = new HashMap<String, OProperty>();

    for (OClass superClass : classes) {
      if (superClass == null) continue;
      OClassImpl impl;

      if (superClass instanceof OClassAbstractDelegate)
        impl = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      else impl = (OClassImpl) superClass;
      impl.propertiesMap(properties);
      for (Map.Entry<String, OProperty> entry : properties.entrySet()) {
        if (comulative.containsKey(entry.getKey())) {
          final String property = entry.getKey();
          final OProperty existingProperty = comulative.get(property);
          if (!existingProperty.getType().equals(entry.getValue().getType())) {
            throw new OSchemaException(
                "Properties conflict detected: '"
                    + existingProperty
                    + "] vs ["
                    + entry.getValue()
                    + "]");
          }
        }
      }

      comulative.putAll(properties);
      properties.clear();
    }
  }

  private void checkRecursion(final OClass baseClass) {
    if (isSubClassOf(baseClass)) {
      throw new OSchemaException(
          "Cannot add base class '" + baseClass.getName() + "', because of recursion");
    }
  }

  protected void removePolymorphicClusterIds(final OClassImpl iBaseClass) {
    for (final int clusterId : iBaseClass.polymorphicClusterIds)
      removePolymorphicClusterId(clusterId);
  }

  protected void removePolymorphicClusterId(final int clusterId) {
    final int index = Arrays.binarySearch(polymorphicClusterIds, clusterId);
    if (index < 0) return;

    if (index < polymorphicClusterIds.length - 1)
      System.arraycopy(
          polymorphicClusterIds,
          index + 1,
          polymorphicClusterIds,
          index,
          polymorphicClusterIds.length - (index + 1));

    polymorphicClusterIds = Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length - 1);

    removeClusterFromIndexes(clusterId);
    for (OClassImpl superClass : superClasses) {
      superClass.removePolymorphicClusterId(clusterId);
    }
  }

  private void removeClusterFromIndexes(final int iId) {
    if (getDatabase().getStorage() instanceof OAbstractPaginatedStorage) {
      final String clusterName = getDatabase().getClusterNameById(iId);
      final List<String> indexesToRemove = new ArrayList<String>();

      for (final OIndex index : getIndexes()) indexesToRemove.add(index.getName());

      final OIndexManagerAbstract indexManager =
          getDatabase().getMetadata().getIndexManagerInternal();
      for (final String indexName : indexesToRemove)
        indexManager.removeClusterFromIndex(clusterName, indexName);
    }
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  /** Add different cluster id to the "polymorphic cluster ids" array. */
  protected void addPolymorphicClusterIds(final OClassImpl iBaseClass) {
    Set<Integer> clusters = new TreeSet<Integer>();

    for (int clusterId : polymorphicClusterIds) {
      clusters.add(clusterId);
    }
    for (int clusterId : iBaseClass.polymorphicClusterIds) {
      if (clusters.add(clusterId)) {
        try {
          addClusterIdToIndexes(clusterId);
        } catch (RuntimeException e) {
          OLogManager.instance()
              .warn(
                  this,
                  "Error adding clusterId '%d' to index of class '%s'",
                  e,
                  clusterId,
                  getName());
          clusters.remove(clusterId);
        }
      }
    }
    polymorphicClusterIds = new int[clusters.size()];
    int i = 0;
    for (Integer cluster : clusters) {
      polymorphicClusterIds[i] = cluster;
      i++;
    }
  }

  private void addPolymorphicClusterIdsWithInheritance(final OClassImpl iBaseClass) {
    addPolymorphicClusterIds(iBaseClass);
    for (OClassImpl superClass : superClasses) {
      superClass.addPolymorphicClusterIdsWithInheritance(iBaseClass);
    }
  }

  public List<OType> extractFieldTypes(final String[] fieldNames) {
    final List<OType> types = new ArrayList<OType>(fieldNames.length);

    for (String fieldName : fieldNames) {
      if (!fieldName.equals("@rid"))
        types.add(
            getProperty(decodeClassName(OIndexDefinitionFactory.extractFieldName(fieldName)))
                .getType());
      else types.add(OType.LINK);
    }
    return types;
  }

  protected OClass setClusterIds(final int[] iClusterIds) {
    clusterIds = iClusterIds;
    Arrays.sort(clusterIds);

    return this;
  }

  public static String decodeClassName(String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    if (s.startsWith("`") && s.endsWith("`")) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  public void fromStream(ODocument document) {
    this.document = document;
    fromStream();
  }

  public ODocument toNetworkStream() {
    ODocument document = new ODocument();
    document.setTrackingChanges(false);
    document.field("name", name);
    document.field("shortName", shortName);
    document.field("description", description);
    document.field("defaultClusterId", defaultClusterId);
    document.field("clusterIds", clusterIds);
    document.field("clusterSelection", clusterSelection.getName());
    document.field("overSize", overSize);
    document.field("strictMode", strictMode);
    document.field("abstract", abstractClass);

    final Set<ODocument> props = new LinkedHashSet<ODocument>();
    for (final OProperty p : properties.values()) {
      props.add(((OPropertyImpl) p).toNetworkStream());
    }
    document.field("properties", props, OType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      document.field("superClass", null, OType.STRING);
      document.field("superClasses", null, OType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      document.field("superClass", superClasses.get(0).getName(), OType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (OClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      document.field("superClasses", superClassesNames, OType.EMBEDDEDLIST);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        OType.EMBEDDEDMAP);

    return document;
  }
}
