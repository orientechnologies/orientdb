/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.metadata.schema;

import java.io.IOException;
import java.util.*;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Schema Class implementation.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OClassImpl extends ODocumentWrapperNoClass implements OClass {
  private static final long                  serialVersionUID        = 1L;
  private static final int                   NOT_EXISTENT_CLUSTER_ID = -1;
  final OSchemaShared                        owner;
  private final Map<String, OProperty>       properties              = new HashMap<String, OProperty>();
  private int                                defaultClusterId        = NOT_EXISTENT_CLUSTER_ID;
  private String                             name;
  private Class<?>                           javaClass;
  private int[]                              clusterIds;
  private List<OClassImpl>                   superClasses            = new ArrayList<OClassImpl>();
  private int[]                              polymorphicClusterIds;
  private List<OClass>                       subclasses;
  private float                              overSize                = 0f;
  private String                             shortName;
  private boolean                            strictMode              = false;                           // @SINCE v1.0rc8
  private boolean                            abstractClass           = false;                           // @SINCE v1.2.0
  private Map<String, String>                customFields;
  private volatile OClusterSelectionStrategy clusterSelection;                                          // @SINCE 1.7
  private volatile int                       hashCode;

  private static Set<String>                 reserved                = new HashSet<String>();
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

  /**
   * Constructor used in unmarshalling.
   */
  protected OClassImpl(final OSchemaShared iOwner) {
    this(iOwner, new ODocument().setTrackingChanges(false));
  }

  protected OClassImpl(final OSchemaShared iOwner, final String iName, final int[] iClusterIds) {
    this(iOwner);
    name = iName;
    setClusterIds(iClusterIds);
    defaultClusterId = iClusterIds[0];
    if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID)
      abstractClass = true;

    if (abstractClass)
      setPolymorphicClusterIds(OCommonConst.EMPTY_INT_ARRAY);
    else
      setPolymorphicClusterIds(iClusterIds);

    clusterSelection = owner.getClusterSelectionFactory().newInstanceOfDefaultClass();
  }

  /**
   * Constructor used in unmarshalling.
   */
  protected OClassImpl(final OSchemaShared iOwner, final ODocument iDocument) {
    document = iDocument;
    owner = iOwner;
  }

  public static int[] readableClusters(final ODatabaseDocument iDatabase, final int[] iClusterIds) {
    List<Integer> listOfReadableIds = new ArrayList<Integer>();

    boolean all = true;
    for (int clusterId : iClusterIds) {
      try {
        String clusterName = iDatabase.getClusterNameById(clusterId);
        iDatabase.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, clusterName);
        listOfReadableIds.add(clusterId);
      } catch (OSecurityAccessException securityException) {
        all = false;
        // if the cluster is inaccessible it's simply not processed in the list.add
      }
    }

    if (all)
      // JUST RETURN INPUT ARRAY (FASTER)
      return iClusterIds;

    int[] readableClusterIds = new int[listOfReadableIds.size()];
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
  public OClass setClusterSelection(OClusterSelectionStrategy clusterSelection) {
    return setClusterSelection(clusterSelection.getName());
  }

  @Override
  public OClass setClusterSelection(final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s clusterselection %s", name, value);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s clusterselection %s", name, value);
        OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
        database.command(new OCommandSQL(cmd)).execute();

        setClusterSelectionInternal(value);
      } else
        setClusterSelectionInternal(value);

      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public <T> T newInstance() throws InstantiationException, IllegalAccessException {
    acquireSchemaReadLock();
    try {
      if (javaClass == null)
        throw new IllegalArgumentException("Cannot create an instance of class '" + name + "' since no Java class was specified");

      return (T) javaClass.newInstance();
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public <RET extends ODocumentWrapper> RET reload() {
    return (RET) owner.reload();
  }

  public String getCustom(final String iName) {
    acquireSchemaReadLock();
    try {
      if (customFields == null)
        return null;

      return customFields.get(iName);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClassImpl setCustom(final String name, final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s custom %s=%s", getName(), name, value);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s custom %s=%s", getName(), name, value);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(new OCommandSQL(cmd)).execute();

        setCustomInternal(name, value);
      } else
        setCustomInternal(name, value);

      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public Map<String, String> getCustomInternal() {
    acquireSchemaReadLock();
    try {
      if (customFields != null)
        return Collections.unmodifiableMap(customFields);
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void removeCustom(final String name) {
    setCustom(name, null);
  }

  public void clearCustom() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s custom clear", getName());
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s custom clear", getName());
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
        database.command(new OCommandSQL(cmd)).execute();

        clearCustomInternal();
      } else
        clearCustomInternal();

    } finally {
      releaseSchemaWriteLock();
    }
  }

  public Set<String> getCustomKeys() {
    acquireSchemaReadLock();
    try {
      if (customFields != null)
        return Collections.unmodifiableSet(customFields.keySet());
      return new HashSet<String>();
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public boolean hasClusterId(int clusterId) {
    return Arrays.binarySearch(clusterIds, clusterId) >= 0;
  }

  @Override
  public OClass getSuperClass() {
    acquireSchemaReadLock();
    try {
      return superClasses.isEmpty() ? null : superClasses.get(0);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OClass setSuperClass(OClass iSuperClass) {
    setSuperClasses(iSuperClass != null ? Arrays.asList(iSuperClass) : Collections.EMPTY_LIST);
    return this;
  }

  /**
   * Set the super class.
   *
   * @param superClass
   *          Super class as OClass instance
   * @return the object itself.
   */
  /*
   * public OClass setSuperClass(final OClass superClass) { getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA,
   * ORole.PERMISSION_UPDATE); acquireSchemaWriteLock(); try { final ODatabaseDocumentInternal database = getDatabase(); final
   * OStorage storage = database.getStorage();
   * 
   * if (storage instanceof OStorageProxy) { final String cmd = String.format("alter class %s superclass %s", name, superClass !=
   * null ? superClass.getName() : null); database.command(new OCommandSQL(cmd)).execute(); } else if (isDistributedCommand()) {
   * final String cmd = String.format("alter class %s superclass %s", name, superClass != null ? superClass.getName() : null); final
   * OCommandSQL commandSQL = new OCommandSQL(cmd); commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
   * 
   * database.command(commandSQL).execute();
   * 
   * setSuperClassInternal(superClass); } else setSuperClassInternal(superClass);
   * 
   * } finally { releaseSchemaWriteLock(); } return this; }
   */

  /*
   * void setSuperClassInternal(final OClass superClass) { acquireSchemaWriteLock(); try { final OClassImpl cls;
   * 
   * if (superClass instanceof OClassAbstractDelegate) cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate; else cls =
   * (OClassImpl) superClass;
   * 
   * if (cls != null) cls.addBaseClasses(this); else if (this.superClass != null) // REMOVE THE PREVIOUS ONE
   * this.superClass.removeBaseClassInternal(this);
   * 
   * this.superClass = cls; } finally { releaseSchemaWriteLock(); } }
   */

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
    if (classNames == null)
      classNames = Collections.EMPTY_LIST;

    final List<OClass> classes = new ArrayList<OClass>(classNames.size());
    final OSchema schema = getDatabase().getMetadata().getSchema();
    if (classNames != null) {
      for (String className : classNames) {
        classes.add(schema.getClass(className));
      }
    }
    return setSuperClasses(classes);
  }

  @Override
  public OClass setSuperClasses(final List<? extends OClass> classes) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      final StringBuilder sb = new StringBuilder();
      if (classes != null && classes.size() > 0) {
        for (OClass superClass : classes) {
          sb.append(superClass.getName()).append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
      } else
        sb.append("null");

      final String cmd = String.format("alter class %s superclasses %s", name, sb);
      if (storage instanceof OStorageProxy) {
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setSuperClassesInternal(classes);
      } else
        setSuperClassesInternal(classes);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  void setSuperClassesInternal(final List<? extends OClass> classes) {
    acquireSchemaWriteLock();
    try {
      List<OClassImpl> newSuperClasses = new ArrayList<OClassImpl>();
      OClassImpl cls;
      for (OClass superClass : classes) {
        if (superClass instanceof OClassAbstractDelegate)
          cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
        else
          cls = (OClassImpl) superClass;
        newSuperClasses.add(cls);
      }
      checkParametersConflict(newSuperClasses);
      List<OClassImpl> toAddList = new ArrayList<OClassImpl>(newSuperClasses);
      toAddList.removeAll(superClasses);
      List<OClassImpl> toRemoveList = new ArrayList<OClassImpl>(superClasses);
      toRemoveList.removeAll(newSuperClasses);

      for (OClassImpl addTo : toAddList) {
        addTo.addBaseClass(this);
      }
      for (OClassImpl toRemove : toRemoveList) {
        toRemove.removeBaseClassInternal(this);
      }
      superClasses.clear();
      superClasses.addAll(newSuperClasses);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass addSuperClass(final OClass superClass) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s superclass +%s", name, superClass != null ? superClass.getName() : null);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s superclass +%s", name, superClass != null ? superClass.getName() : null);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        addSuperClassInternal(superClass);
      } else
        addSuperClassInternal(superClass);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  void addSuperClassInternal(final OClass superClass) {
    acquireSchemaWriteLock();
    try {
      final OClassImpl cls;

      if (superClass instanceof OClassAbstractDelegate)
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      else
        cls = (OClassImpl) superClass;

      if (cls != null) {
        cls.checkParametersConflict(this);
        cls.addBaseClass(this);
        superClasses.add(cls);
      }
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass removeSuperClass(OClass superClass) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s superclass -%s", name, superClass != null ? superClass.getName() : null);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s superclass -%s", name, superClass != null ? superClass.getName() : null);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        removeSuperClassInternal(superClass);
      } else
        removeSuperClassInternal(superClass);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  void removeSuperClassInternal(final OClass superClass) {
    acquireSchemaWriteLock();
    try {
      final OClassImpl cls;

      if (superClass instanceof OClassAbstractDelegate)
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      else
        cls = (OClassImpl) superClass;

      if (superClasses.contains(cls)) {
        if (cls != null)
          cls.removeBaseClassInternal(this);

        superClasses.remove(superClass);
      }
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClass setName(final String name) {
    if (getName().equals(name))
      return this;

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OClass oClass = getDatabase().getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error = String.format("Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null)
      throw new OSchemaException("Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '"
          + name + "'");
    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s name %s", this.name, name);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s name %s", this.name, name);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setNameInternal(name);
      } else
        setNameInternal(name);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public long getSize() {
    acquireSchemaReadLock();
    try {
      long size = 0;
      for (int clusterId : clusterIds)
        size += getDatabase().getClusterRecordSizeById(clusterId);

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

  public OClass setShortName(String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty())
        shortName = null;
    }
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s shortname %s", name, shortName);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {

        final String cmd = String.format("alter class %s shortname %s", name, shortName);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setShortNameInternal(shortName);
      } else
        setShortNameInternal(shortName);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
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
      propertiesMap(props, true);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void propertiesMap(Map<String, OProperty> propertiesMap, boolean keepCase) {
    for (OProperty p : properties.values()) {
      String propName = p.getName();
      if (!keepCase)
        propName = propName.toLowerCase();
      if (!propertiesMap.containsKey(propName))
        propertiesMap.put(propName, p);
    }
    for (OClassImpl superClass : superClasses) {
      superClass.propertiesMap(propertiesMap, keepCase);
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
    for (OProperty p : properties.values())
      if (areIndexed(p.getName()))
        indexedProperties.add(p);
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
      propertyName = propertyName.toLowerCase();

      OProperty p = properties.get(propertyName);
      if (p != null)
        return p;
      for (int i = 0; i < superClasses.size() && p == null; i++) {
        p = superClasses.get(i).getProperty(propertyName);
      }
      return p;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OProperty createProperty(final String iPropertyName, final OType iType) {
    return addProperty(iPropertyName, iType, null, null, true);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    if (iLinkedClass == null)
      throw new OSchemaException("Missing linked class");

    return addProperty(iPropertyName, iType, null, iLinkedClass, true);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass,
      boolean iChackExistingData) {
    if (iLinkedClass == null)
      throw new OSchemaException("Missing linked class");

    return addProperty(iPropertyName, iType, null, iLinkedClass, iChackExistingData);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
    return addProperty(iPropertyName, iType, iLinkedType, null, true);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType, boolean iChackExistingData) {
    return addProperty(iPropertyName, iType, iLinkedType, null, iChackExistingData);
  }

  @Override
  public boolean existsProperty(String propertyName) {
    acquireSchemaReadLock();
    try {
      propertyName = propertyName.toLowerCase();
      boolean result = properties.containsKey(propertyName);
      if (result)
        return true;
      for (OClassImpl superClass : superClasses) {
        result = superClass.existsProperty(propertyName);
        if (result)
          return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void dropProperty(final String propertyName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    final String lowerName = propertyName.toLowerCase();

    acquireSchemaWriteLock();
    try {
      if (!properties.containsKey(lowerName))
        throw new OSchemaException("Property '" + propertyName + "' not found in class " + name + "'");

      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (storage instanceof OStorageProxy) {
        database.command(new OCommandSQL("drop property " + name + '.' + propertyName)).execute();
      } else if (isDistributedCommand()) {
        final OCommandSQL commandSQL = new OCommandSQL("drop property " + name + '.' + propertyName);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        dropPropertyInternal(propertyName);
      } else
        dropPropertyInternal(propertyName);

    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public void fromStream() {
    subclasses = null;
    superClasses.clear();

    name = document.field("name");
    if (document.containsField("shortName"))
      shortName = document.field("shortName");
    else
      shortName = null;
    defaultClusterId = document.field("defaultClusterId");
    if (document.containsField("strictMode"))
      strictMode = document.field("strictMode");
    else
      strictMode = false;

    if (document.containsField("abstract"))
      abstractClass = document.field("abstract");
    else
      abstractClass = false;

    if (document.field("overSize") != null)
      overSize = document.field("overSize");
    else
      overSize = 0f;

    final Object cc = document.field("clusterIds");
    if (cc instanceof Collection<?>) {
      final Collection<Integer> coll = document.field("clusterIds");
      clusterIds = new int[coll.size()];
      int i = 0;
      for (final Integer item : coll)
        clusterIds[i++] = item;
    } else
      clusterIds = (int[]) cc;
    Arrays.sort(clusterIds);

    if (clusterIds.length == 1 && clusterIds[0] == -1)
      setPolymorphicClusterIds(OCommonConst.EMPTY_INT_ARRAY);
    else
      setPolymorphicClusterIds(clusterIds);

    // READ PROPERTIES
    OPropertyImpl prop;

    final Map<String, OProperty> newProperties = new HashMap<String, OProperty>();
    final Collection<ODocument> storedProperties = document.field("properties");

    if (storedProperties != null)
      for (ODocument p : storedProperties) {
        prop = new OPropertyImpl(this, p);
        prop.fromStream();

        if (properties.containsKey(prop.getName())) {
          prop = (OPropertyImpl) properties.get(prop.getName().toLowerCase());
          prop.fromStream(p);
        }

        newProperties.put(prop.getName().toLowerCase(), prop);
      }

    properties.clear();
    properties.putAll(newProperties);
    customFields = document.field("customFields", OType.EMBEDDEDMAP);
    clusterSelection = owner.getClusterSelectionFactory().getStrategy((String) document.field("clusterSelection"));
  }

  @Override
  @OBeforeSerialization
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

    try {
      document.field("name", name);
      document.field("shortName", shortName);
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

      document.field("customFields", customFields != null && customFields.size() > 0 ? customFields : null, OType.EMBEDDEDMAP);

    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }

    return document;
  }

  public Class<?> getJavaClass() {
    acquireSchemaReadLock();
    try {
      return javaClass;
    } finally {
      releaseSchemaReadLock();
    }
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

  public void setDefaultClusterId(final int defaultClusterId) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();
      this.defaultClusterId = defaultClusterId;
    } finally {
      releaseSchemaWriteLock();
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
      return polymorphicClusterIds;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void setPolymorphicClusterIds(final int[] iClusterIds) {
    polymorphicClusterIds = iClusterIds;
    Arrays.sort(polymorphicClusterIds);
  }

  public void renameProperty(final String iOldName, final String iNewName) {
    final OProperty p = properties.remove(iOldName.toLowerCase());
    if (p != null)
      properties.put(iNewName.toLowerCase(), p);
  }

  public OClass addClusterId(final int clusterId) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {

      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s addcluster %d", name, clusterId);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {

        final String cmd = String.format("alter class %s addcluster %d", name, clusterId);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        addClusterIdInternal(clusterId);
      } else
        addClusterIdInternal(clusterId);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public static OClass addClusters(final OClass cls, final int iClusters) {
    final String clusterBase = cls.getName().toLowerCase() + "_";
    for (int i = 1; i < iClusters; ++i) {
      cls.addCluster(clusterBase + i);
    }
    return cls;
  }

  @Override
  public OClass addCluster(final String clusterNameOrId) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s addcluster %s", name, clusterNameOrId);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final int clusterId = createClusterIfNeeded(clusterNameOrId);
        addClusterIdInternal(clusterId);

        final String cmd = String.format("alter class %s addcluster %s", name, clusterNameOrId);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();
      } else {
        final int clusterId = createClusterIfNeeded(clusterNameOrId);
        addClusterIdInternal(clusterId);
      }
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass removeClusterId(final int clusterId) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s removecluster %d", name, clusterId);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s removecluster %d", name, clusterId);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        removeClusterIdInternal(clusterId);
      } else
        removeClusterIdInternal(clusterId);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public Collection<OClass> getSubclasses() {
    acquireSchemaReadLock();
    try {
      if (subclasses == null || subclasses.size() == 0)
        return Collections.emptyList();

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

        for (OClass c : subclasses)
          set.addAll(c.getAllSubclasses());
      }
      return set;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<OClass> getBaseClasses() {
    return getSubclasses();
  }

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

  OClass removeBaseClassInternal(final OClass baseClass) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (subclasses == null)
        return this;

      if (subclasses.remove(baseClass))
        removePolymorphicClusterIds((OClassImpl) baseClass);

      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

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
        if (thisOverSize > maxOverSize)
          maxOverSize = thisOverSize;
      }
      return maxOverSize;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass setOverSize(final float overSize) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        // FORMAT FLOAT LOCALE AGNOSTIC
        final String cmd = String.format("alter class %s oversize %s", name, new Float(overSize).toString());
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        // FORMAT FLOAT LOCALE AGNOSTIC
        final String cmd = String.format("alter class %s oversize %s", name, new Float(overSize).toString());
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setOverSizeInternal(overSize);
      } else
        setOverSizeInternal(overSize);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
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

  public OClass setAbstract(boolean isAbstract) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s abstract %s", name, isAbstract);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s abstract %s", name, isAbstract);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(new OCommandSQL(cmd)).execute();

        setAbstractInternal(isAbstract);
      } else
        setAbstractInternal(isAbstract);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public boolean isStrictMode() {
    acquireSchemaReadLock();
    try {
      return strictMode;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass setStrictMode(final boolean isStrict) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s strictmode %s", name, isStrict);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s strictmode %s", name, isStrict);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setStrictModeInternal(isStrict);
      } else
        setStrictModeInternal(isStrict);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
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
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public int hashCode() {
    int sh = hashCode;
    if (sh != 0)
      return sh;

    acquireSchemaReadLock();
    try {
      sh = hashCode;
      if (sh != 0)
        return sh;

      calculateHashCode();
      return hashCode;
    } finally {
      releaseSchemaReadLock();
    }
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
      if (isPolymorphic)
        return getDatabase().countClusterElements(readableClusters(getDatabase(), polymorphicClusterIds));

      return getDatabase().countClusterElements(readableClusters(getDatabase(), clusterIds));
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Truncates all the clusters the class uses.
   *
   * @throws IOException
   */
  public void truncate() throws IOException {

    getDatabase().checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_UPDATE);

    if (isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {
      throw new OSecurityException("Class " + getName()
          + " cannot be truncated because has record level security enabled (extends " + OSecurityShared.RESTRICTED_CLASSNAME + ")");
    }

    final OStorage storage = getDatabase().getStorage();
    acquireSchemaReadLock();
    try {

      for (int id : clusterIds)
        storage.getClusterById(id).truncate();

      for (OIndex<?> index : getClassIndexes())
        index.clear();

      Set<OIndex<?>> superclassIndexes = new HashSet<OIndex<?>>();
      superclassIndexes.addAll(getIndexes());
      superclassIndexes.removeAll(getClassIndexes());
      for (OIndex index : superclassIndexes) {
        index.rebuild();
      }
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Check if the current instance extends specified schema class.
   *
   * @param iClassName
   *          of class that should be checked
   * @return Returns true if the current instance extends the passed schema class (iClass)
   * @see #isSuperClassOf(OClass)
   */
  public boolean isSubClassOf(final String iClassName) {
    acquireSchemaReadLock();
    try {
      if (iClassName == null)
        return false;

      if (iClassName.equalsIgnoreCase(getName()) || iClassName.equalsIgnoreCase(getShortName()))
        return true;
      for (OClassImpl superClass : superClasses) {
        if (superClass.isSubClassOf(iClassName))
          return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Check if the current instance extends specified schema class.
   *
   * @param clazz
   *          to check
   * @return true if the current instance extends the passed schema class (iClass)
   * @see #isSuperClassOf(OClass)
   */
  public boolean isSubClassOf(final OClass clazz) {
    acquireSchemaReadLock();
    try {
      if (clazz == null)
        return false;
      if (equals(clazz))
        return true;
      for (OClassImpl superClass : superClasses) {
        if (superClass.isSubClassOf(clazz))
          return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   *
   * @param clazz
   *          to check
   * @return Returns true if the passed schema class extends the current instance
   * @see #isSubClassOf(OClass)
   */
  public boolean isSuperClassOf(final OClass clazz) {
    return clazz != null && clazz.isSubClassOf(this);
  }

  public Object get(final ATTRIBUTES iAttribute) {
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

  public OClass set(final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;
    final boolean isNull = stringValue == null || stringValue.equalsIgnoreCase("NULL");

    switch (attribute) {
    case NAME:
      setName(stringValue);
      break;
    case SHORTNAME:
      setShortName(stringValue);
      break;
    case SUPERCLASS:
      if (stringValue.startsWith("+")) {
        addSuperClass(getDatabase().getMetadata().getSchema().getClass(stringValue.substring(1)));
      } else if (stringValue.startsWith("-")) {
        removeSuperClass(getDatabase().getMetadata().getSchema().getClass(stringValue.substring(1)));
      } else {
        setSuperClass(getDatabase().getMetadata().getSchema().getClass(stringValue));
      }
      break;
    case SUPERCLASSES:
      setSuperClassesByNames(stringValue != null ? Arrays.asList(stringValue.split(",\\s*")) : null);
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
    case ADDCLUSTER: {
      addCluster(stringValue);
      break;
    }
    case REMOVECLUSTER:
      int clId = getClusterId(stringValue);
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
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        String customName = stringValue.substring(0, indx).trim();
        String customValue = stringValue.substring(indx + 1).trim();
        if (customValue.isEmpty())
          removeCustom(customName);
        else
          setCustom(customName, customValue);
      }
      break;
    }
    return this;
  }

  public OPropertyImpl addPropertyInternal(final String name, final OType type, final OType linkedType, final OClass linkedClass,
      final boolean iCheckExistentRecords) {
    if (name == null || name.length() == 0)
      throw new OSchemaException("Found property name null");

    final Character wrongCharacter = OSchemaShared.checkFieldNameIfValid(name);
    if (wrongCharacter != null)
      throw new OSchemaException("Invalid property name '" + name + "'. Character '" + wrongCharacter + "' cannot be used");

    if (iCheckExistentRecords)
      checkPersistentPropertyType(getDatabase(), name, type);

    final String lowerName = name.toLowerCase();

    final OPropertyImpl prop;

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (properties.containsKey(lowerName))
        throw new OSchemaException("Class '" + this.name + "' already has property '" + name + "'");

      OGlobalProperty global = owner.findOrCreateGlobalProperty(name, type);

      prop = new OPropertyImpl(this, global);

      properties.put(lowerName, prop);

      if (linkedType != null)
        prop.setLinkedTypeInternal(linkedType);
      else if (linkedClass != null)
        prop.setLinkedClassInternal(linkedClass);
    } finally {
      releaseSchemaWriteLock();
    }

    if (prop != null && iCheckExistentRecords)
      fireDatabaseMigration(getDatabase(), name, type);

    return prop;
  }

  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final String... fields) {
    return createIndex(iName, iType.name(), fields);
  }

  public OIndex<?> createIndex(final String iName, final String iType, final String... fields) {
    return createIndex(iName, iType, null, null, fields);
  }

  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final OProgressListener iProgressListener,
      final String... fields) {
    return createIndex(iName, iType.name(), iProgressListener, null, fields);
  }

  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata,
      String... fields) {
    return createIndex(iName, iType, iProgressListener, metadata, null, fields);
  }

  public OIndex<?> createIndex(final String name, String type, final OProgressListener progressListener, ODocument metadata,
      String algorithm, final String... fields) {
    if (type == null)
      throw new IllegalArgumentException("Index type is null");

    type = type.toUpperCase();

    if (fields.length == 0) {
      throw new OIndexException("List of fields to index cannot be empty.");
    }

    acquireSchemaReadLock();
    try {
      for (final String fieldToIndex : fields) {
        final String fieldName = OIndexDefinitionFactory.extractFieldName(fieldToIndex);

        if (!fieldName.equals("@rid") && !existsProperty(fieldName))
          throw new OIndexException("Index with name : '" + name + "' cannot be created on class : '" + this.name
              + "' because field: '" + fieldName + "' is absent in class definition.");
      }

      final OIndexDefinition indexDefinition = OIndexDefinitionFactory.createIndexDefinition(this, Arrays.asList(fields),
          extractFieldTypes(fields), null, type, algorithm);

      return getDatabase().getMetadata().getIndexManager()
          .createIndex(name, type, indexDefinition, polymorphicClusterIds, progressListener, metadata, algorithm);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean areIndexed(final String... fields) {
    return areIndexed(Arrays.asList(fields));
  }

  public boolean areIndexed(final Collection<String> fields) {
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

    acquireSchemaReadLock();
    try {
      final boolean currentClassResult = indexManager.areIndexed(name, fields);

      if (currentClassResult)
        return true;
      for (OClassImpl superClass : superClasses) {
        if (superClass.areIndexed(fields))
          return true;
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex<?>> getInvolvedIndexes(final String... fields) {
    return getInvolvedIndexes(Arrays.asList(fields));
  }

  public Set<OIndex<?>> getInvolvedIndexes(final Collection<String> fields) {
    acquireSchemaReadLock();
    try {
      final Set<OIndex<?>> result = new HashSet<OIndex<?>>(getClassInvolvedIndexes(fields));

      for (OClassImpl superClass : superClasses) {
        result.addAll(superClass.getInvolvedIndexes(fields));
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final Collection<String> fields) {

    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

    acquireSchemaReadLock();
    try {
      return indexManager.getClassInvolvedIndexes(name, fields);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final String... fields) {
    return getClassInvolvedIndexes(Arrays.asList(fields));
  }

  public OIndex<?> getClassIndex(final String name) {
    acquireSchemaReadLock();
    try {
      return getDatabase().getMetadata().getIndexManager().getClassIndex(this.name, name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex<?>> getClassIndexes() {
    acquireSchemaReadLock();
    try {
      final OIndexManagerProxy idxManager = getDatabase().getMetadata().getIndexManager();
      if (idxManager == null)
        return new HashSet<OIndex<?>>();

      return idxManager.getClassIndexes(name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public void getClassIndexes(final Collection<OIndex<?>> indexes) {
    acquireSchemaReadLock();
    try {
      final OIndexManagerProxy idxManager = getDatabase().getMetadata().getIndexManager();
      if (idxManager == null)
        return;

      idxManager.getClassIndexes(name, indexes);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public void getIndexes(Collection<OIndex<?>> indexes) {
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

  public Set<OIndex<?>> getIndexes() {
    Set<OIndex<?>> indexes = new HashSet<OIndex<?>>();
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
    owner.acquireSchemaWriteLock();
  }

  public void releaseSchemaWriteLock() {
    releaseSchemaWriteLock(true);
  }

  public void releaseSchemaWriteLock(final boolean iSave) {
    calculateHashCode();
    owner.releaseSchemaWriteLock(iSave);
  }

  public void checkEmbedded() {
    owner.checkEmbedded(getDatabase().getStorage().getUnderlying().getUnderlying());
  }

  public void setClusterSelectionInternal(final String clusterSelection) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.clusterSelection = owner.getClusterSelectionFactory().newInstance(clusterSelection);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public void setClusterSelectionInternal(final OClusterSelectionStrategy clusterSelection) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();
      this.clusterSelection = clusterSelection;
    } finally {
      releaseSchemaWriteLock(false);
    }
  }

  public void fireDatabaseMigration(final ODatabaseDocument database, final String propertyName, final OType type) {
    final boolean strictSQL = ((ODatabaseInternal) database).getStorage().getConfiguration().isStrictSql();

    database.query(new OSQLAsynchQuery<Object>("select from " + getEscapedName(name, strictSQL) + " where "
        + getEscapedName(propertyName, strictSQL) + ".type() <> \"" + type.name() + "\"", new OCommandResultListener() {

      @Override
      public boolean result(Object iRecord) {
        final ODocument record = ((OIdentifiable) iRecord).getRecord();
        record.field(propertyName, record.field(propertyName), type);
        database.save(record);
        return true;
      }

      @Override
      public void end() {
      }
    }));
  }

  public void firePropertyNameMigration(final ODatabaseDocument database, final String propertyName, final String newPropertyName,
      final OType type) {
    final boolean strictSQL = ((ODatabaseInternal) database).getStorage().getConfiguration().isStrictSql();

    database.query(new OSQLAsynchQuery<Object>("select from " + getEscapedName(name, strictSQL) + " where "
        + getEscapedName(propertyName, strictSQL) + " is not null ", new OCommandResultListener() {

      @Override
      public boolean result(Object iRecord) {
        final ODocument record = ((OIdentifiable) iRecord).getRecord();
        record.setFieldType(propertyName, type);
        record.field(newPropertyName, record.field(propertyName), type);
        database.save(record);
        return true;
      }

      @Override
      public void end() {
      }
    }));

  }

  public void checkPersistentPropertyType(final ODatabaseInternal<ORecord> database, final String propertyName, final OType type) {
    final boolean strictSQL = database.getStorage().getConfiguration().isStrictSql();

    final StringBuilder builder = new StringBuilder(256);
    builder.append("select count(*) from ");
    builder.append(getEscapedName(name, strictSQL));
    builder.append(" where ");
    builder.append(getEscapedName(propertyName, strictSQL));
    builder.append(".type() not in [");

    final Iterator<OType> cur = type.getCastable().iterator();
    while (cur.hasNext()) {
      builder.append('"').append(cur.next().name()).append('"');
      if (cur.hasNext())
        builder.append(",");
    }
    builder.append("] and ").append(getEscapedName(propertyName, strictSQL)).append(" is not null ");
    if (type.isMultiValue())
      builder.append(" and ").append(getEscapedName(propertyName, strictSQL)).append(".size() <> 0 limit 1");

    final List<ODocument> res = database.command(new OCommandSQL(builder.toString())).execute();
    if (((Long) res.get(0).field("count")) > 0)
      throw new OSchemaException("The database contains some schema-less data in the property '" + name + "." + propertyName
          + "' that is not compatible with the type " + type + ". Fix those records and change the schema again");
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

  private void setOverSizeInternal(final float overSize) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.overSize = overSize;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setCustomInternal(final String name, final String value) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (customFields == null)
        customFields = new HashMap<String, String>();
      if (value == null || "null".equalsIgnoreCase(value))
        customFields.remove(name);
      else
        customFields.put(name, value);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void clearCustomInternal() {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      customFields = null;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setNameInternal(final String name) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      final String oldName = this.name;

      owner.changeClassName(this.name, name, this);
      this.name = name;

      ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (!database.getStorageVersions().classesAreDetectedByClusterId()) {
        for (int clusterId : clusterIds) {
          long[] range = storage.getClusterDataRange(clusterId);

          OPhysicalPosition[] positions = storage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(range[0]));
          do {
            for (OPhysicalPosition position : positions) {
              final ORecordId identity = new ORecordId(clusterId, position.clusterPosition);
              final ORawBuffer record = storage.readRecord(identity, null, true, null).getResult();

              if (record.recordType == ODocument.RECORD_TYPE) {
                final ORecordSerializerSchemaAware2CSV serializer = (ORecordSerializerSchemaAware2CSV) ORecordSerializerFactory
                    .instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);

                if (serializer.getClassName(OBinaryProtocol.bytes2string(record.buffer)).equalsIgnoreCase(name)) {
                  final ODocument document = new ODocument();
                  document.setLazyLoad(false);
                  document.fromStream(record.buffer);
                  document.getRecordVersion().copyFrom(record.version);
                  ORecordInternal.setIdentity(document, identity);
                  document.setClassName(name);
                  document.setDirty();
                  document.save();
                }
              }

              if (positions.length > 0)
                positions = storage.higherPhysicalPositions(clusterId, positions[positions.length - 1]);
            }
          } while (positions.length > 0);
        }
      }

      renameCluster(oldName, this.name);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void renameCluster(String oldName, String newName) {
    oldName = oldName.toLowerCase();
    newName = newName.toLowerCase();

    final ODatabaseDocumentInternal database = getDatabase();
    final OStorage storage = database.getStorage();

    if (storage.getClusterIdByName(newName) != -1)
      return;

    final int clusterId = storage.getClusterIdByName(oldName);
    if (clusterId == -1)
      return;

    if (!hasClusterId(clusterId))
      return;

    database.command(new OCommandSQL("alter cluster " + oldName + " name " + newName)).execute();
  }

  private void setShortNameInternal(final String iShortName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      String oldName = null;

      if (this.shortName != null)
        oldName = this.shortName;

      owner.changeClassName(oldName, iShortName, this);

      this.shortName = iShortName;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void dropPropertyInternal(final String iPropertyName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      final OProperty prop = properties.remove(iPropertyName.toLowerCase());

      if (prop == null)
        throw new OSchemaException("Property '" + iPropertyName + "' not found in class " + name + "'");
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private int createClusterIfNeeded(String nameOrId) {
    String[] parts = nameOrId.split(" ");
    int clId = getClusterId(parts[0]);

    if (clId == NOT_EXISTENT_CLUSTER_ID) {
      try {
        clId = Integer.parseInt(parts[0]);
        throw new IllegalArgumentException("Cluster id '" + clId + "' cannot be added");
      } catch (NumberFormatException e) {
        clId = getDatabase().addCluster(parts[0]);
      }
    }

    return clId;
  }

  private OClass addClusterIdInternal(final int clusterId) {
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

      if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID)
        defaultClusterId = clusterId;

      owner.addClusterForClass(clusterId, this);
      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void addPolymorphicClusterId(int clusterId) {
    if (Arrays.binarySearch(polymorphicClusterIds, clusterId) >= 0)
      return;

    polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
    polymorphicClusterIds[polymorphicClusterIds.length - 1] = clusterId;
    Arrays.sort(polymorphicClusterIds);

    addClusterIdToIndexes(clusterId);

    for (OClassImpl superClass : superClasses) {
      superClass.addPolymorphicClusterId(clusterId);
    }
  }

  private OClass removeClusterIdInternal(final int clusterToRemove) {

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
        if (clusterIds.length >= 1)
          defaultClusterId = clusterIds[0];
        else
          defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
      }

      owner.removeClusterForClass(clusterToRemove, this);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  private void setAbstractInternal(final boolean isAbstract) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      if (isAbstract) {
        // SWITCH TO ABSTRACT
        if (defaultClusterId != NOT_EXISTENT_CLUSTER_ID) {
          // CHECK
          if (count() > 0)
            throw new IllegalStateException("Cannot set the class as abstract because contains records.");

          tryDropCluster(defaultClusterId);
          for (int clusterId : getClusterIds()) {
            tryDropCluster(clusterId);
            removePolymorphicClusterId(clusterId);
            owner.removeClusterForClass(clusterId, this);
          }

          setClusterIds(new int[] { NOT_EXISTENT_CLUSTER_ID });

          defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
        }
      } else {
        if (!abstractClass)
          return;

        int clusterId = getDatabase().getClusterIdByName(name);
        if (clusterId == -1)
          clusterId = getDatabase().addCluster(name);

        this.defaultClusterId = clusterId;
        this.clusterIds[0] = this.defaultClusterId;
      }

      this.abstractClass = isAbstract;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setStrictModeInternal(final boolean iStrict) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.strictMode = iStrict;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private OProperty addProperty(final String propertyName, final OType type, final OType linkedType, final OClass linkedClass,
      boolean iCheckExistentRecords) {
    if (type == null)
      throw new OSchemaException("Property type not defined.");

    if (propertyName == null || propertyName.length() == 0)
      throw new OSchemaException("Property name is null or empty");

    if (Character.isDigit(propertyName.charAt(0)))
      throw new OSchemaException("Found invalid name for property '" + propertyName + "': it cannot start with numbers");

    if (reserved.contains(propertyName.toLowerCase())) {
      throw new OSchemaException("Error creating schema: '" + propertyName
          + "' is a reserved keyword, it cannot be used as a property name");
    }

    if (getDatabase().getTransaction().isActive())
      throw new OSchemaException("Cannot create property '" + propertyName + "' inside a transaction");

    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    OProperty property = null;
    acquireSchemaWriteLock();
    try {
      final StringBuilder cmd = new StringBuilder("create property ");
      // CLASS.PROPERTY NAME
      // if (getDatabase().getStorage().getConfiguration().isStrictSql())
      // cmd.append('`');
      cmd.append(name);
      cmd.append('.');
      cmd.append(propertyName);
      // if (getDatabase().getStorage().getConfiguration().isStrictSql())
      // cmd.append('`');

      // TYPE
      cmd.append(' ');
      cmd.append(type.name);

      if (linkedType != null) {
        // TYPE
        cmd.append(' ');
        cmd.append(linkedType.name);

      } else if (linkedClass != null) {
        // TYPE
        cmd.append(' ');
        cmd.append(linkedClass.getName());
      }

      if (!iCheckExistentRecords)
        cmd.append(" unsafe ");

      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        database.command(new OCommandSQL(cmd.toString())).execute();
        reload();

        return getProperty(propertyName);
      } else if (isDistributedCommand()) {
        final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        property = addPropertyInternal(propertyName, type, linkedType, linkedClass, iCheckExistentRecords);
      } else
        property = addPropertyInternal(propertyName, type, linkedType, linkedClass, iCheckExistentRecords);

    } finally {
      releaseSchemaWriteLock();
    }

    return property;
  }

  private int getClusterId(final String stringValue) {
    int clId;
    try {
      clId = Integer.parseInt(stringValue);
    } catch (NumberFormatException e) {
      clId = getDatabase().getClusterIdByName(stringValue);
    }
    return clId;
  }

  private void addClusterIdToIndexes(int iId) {
    if (getDatabase().getStorage().getUnderlying() instanceof OAbstractPaginatedStorage) {
      final String clusterName = getDatabase().getClusterNameById(iId);
      final List<String> indexesToAdd = new ArrayList<String>();

      for (OIndex<?> index : getIndexes())
        indexesToAdd.add(index.getName());

      final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();
      for (String indexName : indexesToAdd)
        indexManager.addClusterToIndex(clusterName, indexName);
    }
  }

  /**
   * Adds a base class to the current one. It adds also the base class cluster ids to the polymorphic cluster ids array.
   *
   * @param iBaseClass
   *          The base class to add.
   */
  private OClass addBaseClass(final OClassImpl iBaseClass) {
    checkRecursion(iBaseClass);
    checkParametersConflict(iBaseClass);

    if (subclasses == null)
      subclasses = new ArrayList<OClass>();

    if (subclasses.contains(iBaseClass))
      return this;

    subclasses.add(iBaseClass);
    addPolymorphicClusterIdsWithInheritance(iBaseClass);
    return this;
  }

  private void checkParametersConflict(final OClass baseClass) {
    final Collection<OProperty> baseClassProperties = baseClass.properties();
    for (OProperty property : baseClassProperties) {
      OProperty thisProperty = getProperty(property.getName());
      if (thisProperty != null && thisProperty.getType().equals(property.getType())) {
        throw new OSchemaException("Cannot add base class '" + baseClass.getName() + "', because of parameter conflict: "
            + thisProperty + " vs " + property);
      }
    }
  }

  private void checkParametersConflict(List<OClassImpl> classes) {
    final Map<String, OProperty> commulative = new HashMap<String, OProperty>();
    final Map<String, OProperty> properties = new HashMap<String, OProperty>();

    for (OClassImpl superClass : classes) {
      superClass.propertiesMap(properties, false);
      for (Map.Entry<String, OProperty> entry : properties.entrySet()) {
        if (commulative.containsKey(entry.getKey())) {
          final String property = entry.getKey();
          final OProperty existingProperty = commulative.get(property);
          if (!existingProperty.getType().equals(entry.getValue().getType())) {
            throw new OSchemaException("Properties conflict detected: [" + existingProperty + "] vs [" + entry.getValue() + "]");
          }
        }
      }

      commulative.putAll(properties);
      properties.clear();
    }
  }

  private void checkRecursion(final OClass baseClass) {
    if (isSubClassOf(baseClass)) {
      throw new OSchemaException("Can't add base class '" + baseClass.getName() + "', because of recursion");
    }
  }

  private void removePolymorphicClusterIds(final OClassImpl iBaseClass) {
    for (final int clusterId : iBaseClass.polymorphicClusterIds)
      removePolymorphicClusterId(clusterId);
  }

  private void removePolymorphicClusterId(final int clusterId) {
    final int index = Arrays.binarySearch(polymorphicClusterIds, clusterId);
    if (index == -1)
      return;

    if (index < polymorphicClusterIds.length - 1)
      System.arraycopy(polymorphicClusterIds, index + 1, polymorphicClusterIds, index, polymorphicClusterIds.length - (index + 1));

    polymorphicClusterIds = Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length - 1);

    removeClusterFromIndexes(clusterId);
    for (OClassImpl superClass : superClasses) {
      superClass.removePolymorphicClusterId(clusterId);
    }
  }

  private void removeClusterFromIndexes(final int iId) {
    if (getDatabase().getStorage().getUnderlying() instanceof OAbstractPaginatedStorage) {
      final String clusterName = getDatabase().getClusterNameById(iId);
      final List<String> indexesToRemove = new ArrayList<String>();

      for (final OIndex<?> index : getIndexes())
        indexesToRemove.add(index.getName());

      final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();
      for (final String indexName : indexesToRemove)
        indexManager.removeClusterFromIndex(clusterName, indexName);
    }
  }

  private void tryDropCluster(final int defaultClusterId) {
    if (name.toLowerCase().equals(getDatabase().getClusterNameById(defaultClusterId))) {
      // DROP THE DEFAULT CLUSTER CALLED WITH THE SAME NAME ONLY IF EMPTY
      if (getDatabase().getClusterRecordSizeById(defaultClusterId) == 0)
        getDatabase().dropCluster(defaultClusterId, true);
    }
  }

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  /**
   * Add different cluster id to the "polymorphic cluster ids" array.
   */
  private void addPolymorphicClusterIds(final OClassImpl iBaseClass) {
    boolean found;
    for (int i : iBaseClass.polymorphicClusterIds) {
      found = false;
      for (int k : polymorphicClusterIds) {
        if (i == k) {
          found = true;
          break;
        }
      }

      if (!found) {
        // ADD IT
        polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
        polymorphicClusterIds[polymorphicClusterIds.length - 1] = i;
        Arrays.sort(polymorphicClusterIds);
      }
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
        types.add(getProperty(OIndexDefinitionFactory.extractFieldName(fieldName).toLowerCase()).getType());
      else
        types.add(OType.LINK);
    }
    return types;
  }

  private OClass setClusterIds(final int[] iClusterIds) {
    clusterIds = iClusterIds;
    Arrays.sort(clusterIds);

    return this;
  }

  private boolean isDistributedCommand() {
    return getDatabase().getStorage() instanceof OAutoshardedStorage
        && OScenarioThreadLocal.INSTANCE.get() != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED;
  }

}
