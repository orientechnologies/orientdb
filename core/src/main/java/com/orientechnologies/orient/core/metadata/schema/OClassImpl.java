/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

import java.io.IOException;
import java.util.*;

/**
 * Schema Class implementation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OClassImpl extends ODocumentWrapperNoClass implements OClass {
  private static final long            serialVersionUID        = 1L;
  private static final int             NOT_EXISTENT_CLUSTER_ID = -1;

  private int                          defaultClusterId        = NOT_EXISTENT_CLUSTER_ID;
  private final Map<String, OProperty> properties              = new HashMap<String, OProperty>();

  private String                       name;
  private Class<?>                     javaClass;
  private int[]                        clusterIds;
  private OClassImpl                   superClass;
  private int[]                        polymorphicClusterIds;
  private List<OClass>                 baseClasses;
  private float                        overSize                = 0f;
  private String                       shortName;
  private boolean                      strictMode              = false;                           // @SINCE v1.0rc8
  private boolean                      abstractClass           = false;                           // @SINCE v1.2.0
  private Map<String, String>          customFields;
  private OClusterSelectionStrategy    clusterSelection;                                          // @SINCE 1.7

  final OSchemaShared                  owner;

  /**
   * Constructor used in unmarshalling.
   */
  protected OClassImpl(final OSchemaShared iOwner) {
    this(iOwner, new ODocument());
  }

  protected OClassImpl(final OSchemaShared iOwner, final String iName, final int[] iClusterIds) {
    this(iOwner);
    name = iName;
    setClusterIds(iClusterIds);
    defaultClusterId = iClusterIds[0];
    if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID)
      abstractClass = true;

    if (abstractClass)
      setPolymorphicClusterIds(new int[0]);
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

  public static int[] readableClusters(final ODatabaseRecord iDatabase, final int[] iClusterIds) {
    List<Integer> listOfReadableIds = new ArrayList<Integer>();

    boolean all = true;
    for (int clusterId : iClusterIds) {
      try {
        String clusterName = iDatabase.getClusterNameById(clusterId);
        iDatabase.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, clusterName);
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
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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

  private void setClusterSelectionInternal(final String clusterSelection) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.clusterSelection = owner.getClusterSelectionFactory().newInstance(clusterSelection);
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

  public OClassImpl setCustom(final String name, final String value) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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

  private void clearCustomInternal() {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      customFields = null;
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

  public OClass getSuperClass() {
    acquireSchemaReadLock();
    try {
      return superClass;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Set the super class.
   * 
   * @param superClass
   *          Super class as OClass instance
   * @return the object itself.
   */
  public OClass setSuperClass(final OClass superClass) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s superclass %s", name, superClass != null ? superClass.getName() : null);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s superclass %s", name, superClass != null ? superClass.getName() : null);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setSuperClassInternal(superClass);
      } else
        setSuperClassInternal(superClass);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  void setSuperClassInternal(final OClass superClass) {
    acquireSchemaWriteLock();
    try {
      final OClassImpl cls;

      if (superClass instanceof OClassAbstractDelegate)
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      else
        cls = (OClassImpl) superClass;

      if (cls != null)
        cls.addBaseClasses(this);
      else if (this.superClass != null)
        // REMOVE THE PREVIOUS ONE
        this.superClass.removeBaseClassInternal(this);

      this.superClass = cls;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public String getName() {
    acquireSchemaReadLock();
    try {
      return name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass setName(final String name) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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

  private void setNameInternal(final String name) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      final String oldName = this.name;

      owner.changeClassName(this.name, name, this);
      this.name = name;

      ODatabaseRecord database = getDatabase();
      final OStorage storage = database.getStorage();

      if (!database.getStorageVersions().classesAreDetectedByClusterId()) {
        for (int clusterId : clusterIds) {
          OClusterPosition[] range = storage.getClusterDataRange(clusterId);

          OPhysicalPosition[] positions = storage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(range[0]));
          do {
            for (OPhysicalPosition position : positions) {
              final ORecordId identity = new ORecordId(clusterId, position.clusterPosition);
              final ORawBuffer record = storage.readRecord(identity, null, true, null, false, OStorage.LOCKING_STRATEGY.DEFAULT)
                  .getResult();

              if (record.recordType == ODocument.RECORD_TYPE) {
                final ORecordSerializerSchemaAware2CSV serializer = (ORecordSerializerSchemaAware2CSV) ORecordSerializerFactory
                    .instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);

                if (serializer.getClassName(OBinaryProtocol.bytes2string(record.buffer)).equalsIgnoreCase(name)) {
                  final ODocument document = new ODocument();
                  document.setLazyLoad(false);
                  document.fromStream(record.buffer);
                  document.getRecordVersion().copyFrom(record.version);
                  document.setIdentity(identity);
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
    final ODatabaseRecord database = getDatabase();
    if (checkClusterRenameOk(database.getStorage().getClusterIdByName(newName), newName)) {
      database.command(new OCommandSQL("alter cluster " + oldName + " name " + newName)).execute();
    }
  }

  private boolean checkClusterRenameOk(int clusterId, String newName) {
    for (OClass clazz : owner.getClasses()) {
      if (!clazz.getName().equals(newName)
          && (clazz.getDefaultClusterId() == clusterId || Arrays.asList(clazz.getClusterIds()).contains(clusterId))) {
        return false;
      }
    }
    return true;
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
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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

  private void setShortNameInternal(final String iShortName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      String oldName = null;

      if (this.shortName != null)
        oldName = this.shortName.toLowerCase();

      this.shortName = iShortName;

      owner.changeClassName(oldName, shortName, this);
    } finally {
      releaseSchemaWriteLock();
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

  public Collection<OProperty> properties() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      Collection<OProperty> props = null;

      OClassImpl currentClass = this;

      do {
        if (props == null)
          props = new ArrayList<OProperty>();
        props.addAll(currentClass.properties.values());

        currentClass = (OClassImpl) currentClass.getSuperClass();

      } while (currentClass != null);

      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<OProperty> getIndexedProperties() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      Collection<OProperty> indexedProps = null;

      OClassImpl currentClass = this;

      do {
        for (OProperty p : currentClass.properties.values())
          if (areIndexed(p.getName())) {
            if (indexedProps == null)
              indexedProps = new ArrayList<OProperty>();
            indexedProps.add(p);
          }

        currentClass = (OClassImpl) currentClass.getSuperClass();

      } while (currentClass != null);

      return (Collection<OProperty>) (indexedProps != null ? indexedProps : Collections.emptyList());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OProperty getProperty(String propertyName) {
    acquireSchemaReadLock();
    try {
      propertyName = propertyName.toLowerCase();

      OClassImpl currentClass = this;
      do {
        final OProperty p = currentClass.properties.get(propertyName);

        if (p != null)
          return p;

        currentClass = (OClassImpl) currentClass.getSuperClass();

      } while (currentClass != null);

      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OProperty createProperty(final String iPropertyName, final OType iType) {
    return addProperty(iPropertyName, iType, null, null);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    if (iLinkedClass == null)
      throw new OSchemaException("Missing linked class");

    return addProperty(iPropertyName, iType, null, iLinkedClass);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
    return addProperty(iPropertyName, iType, iLinkedType, null);
  }

  public boolean existsProperty(final String iPropertyName) {
    acquireSchemaReadLock();
    try {
      return properties.containsKey(iPropertyName.toLowerCase());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void dropProperty(final String propertyName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");

    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

    final String lowerName = propertyName.toLowerCase();

    acquireSchemaWriteLock();
    try {
      if (!properties.containsKey(lowerName))
        throw new OSchemaException("Property '" + propertyName + "' not found in class " + name + "'");

      final ODatabaseRecord database = getDatabase();
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

  private void dropPropertyInternal(final String iPropertyName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

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

  @Override
  public void fromStream() {
    baseClasses = null;
    superClass = null;

    name = document.field("name");
    if (document.containsField("shortName"))
      shortName = document.field("shortName");
    else
      shortName = null;
    defaultClusterId = (Integer) document.field("defaultClusterId");
    if (document.containsField("strictMode"))
      strictMode = (Boolean) document.field("strictMode");
    else
      strictMode = false;

    if (document.containsField("abstract"))
      abstractClass = (Boolean) document.field("abstract");
    else
      abstractClass = false;

    if (document.field("overSize") != null)
      overSize = (Float) document.field("overSize");
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
      setPolymorphicClusterIds(new int[0]);
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

      document.field("superClass", superClass != null ? superClass.getName() : null);
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
  public int getClusterForNewInstance() {
    acquireSchemaReadLock();
    try {
      return clusterSelection.getCluster(this);
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

  public OClass addClusterId(final int clusterId) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {

      final ODatabaseRecord database = getDatabase();
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

  @Override
  public OClass addCluster(final String clusterNameOrId) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s addcluster %s", name, clusterNameOrId);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s addcluster %s", name, clusterNameOrId);

        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        final int clusterId = createClusterIfNeeded(clusterNameOrId);
        addClusterIdInternal(clusterId);
      } else {
        final int clusterId = createClusterIfNeeded(clusterNameOrId);
        addClusterIdInternal(clusterId);
      }
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  private int createClusterIfNeeded(String nameOrId) {
    String[] parts = nameOrId.split(" ");
    int clId = getClusterId(parts[0]);

    if (clId == NOT_EXISTENT_CLUSTER_ID) {
      try {
        clId = Integer.parseInt(parts[0]);
        throw new IllegalArgumentException("Cluster id '" + nameOrId + "' cannot be added");
      } catch (NumberFormatException e) {
        // CREATE THE CLUSTER AT THE FLY
        if (parts.length == 1)
          // SET THE DEFAULT TYPE
          parts = new String[] {
              parts[0],
              getDatabase().getURL().startsWith(OEngineMemory.NAME.toLowerCase()) ? OStorage.CLUSTER_TYPE.MEMORY.toString()
                  : OStorage.CLUSTER_TYPE.PHYSICAL.toString() };

        clId = getDatabase().addCluster(parts[0], OStorage.CLUSTER_TYPE.valueOf(parts[1]));
      }
    }

    return clId;
  }

  @Override
  public OClass addCluster(final String clusterName, final OStorage.CLUSTER_TYPE iClusterType) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s addcluster %s %s", name, clusterName, iClusterType);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s addcluster %s %s", name, clusterName, iClusterType);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        final int clusterId = database.getClusterIdByName(clusterName);

        addClusterIdInternal(clusterId);
      } else {
        final int clusterId = database.getClusterIdByName(clusterName);
        addClusterIdInternal(clusterId);
      }
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
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

    if (superClass != null)
      superClass.addPolymorphicClusterId(clusterId);
  }

  public OClass removeClusterId(final int clusterId) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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

      if (defaultClusterId == clusterToRemove)
        defaultClusterId = NOT_EXISTENT_CLUSTER_ID;

      owner.removeClusterForClass(clusterToRemove, this);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public Collection<OClass> getBaseClasses() {
    acquireSchemaReadLock();
    try {
      if (baseClasses == null || baseClasses.size() == 0)
        return Collections.emptyList();

      return Collections.unmodifiableCollection(baseClasses);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<OClass> getAllBaseClasses() {
    acquireSchemaReadLock();
    try {
      final Set<OClass> set = new HashSet<OClass>();
      if (baseClasses != null) {
        set.addAll(baseClasses);

        for (OClass c : baseClasses)
          set.addAll(c.getAllBaseClasses());
      }
      return set;
    } finally {
      releaseSchemaReadLock();
    }
  }

  OClass removeBaseClassInternal(final OClass baseClass) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (baseClasses == null)
        return this;

      if (baseClasses.remove(baseClass))
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
        // CUSTOM OVERSIZE SETTED
        return overSize;

      if (superClass != null)
        // RETURN THE OVERSIZE OF THE SUPER CLASS
        return superClass.getOverSize();

      // NO OVERSIZE
      return 0;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass setOverSize(final float overSize) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter class %s oversize %f", name, overSize);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter class %s oversize %f", name, overSize);
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

  public float getOverSizeInternal() {
    acquireSchemaReadLock();
    try {
      return overSize;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void setOverSizeInternal(final float overSize) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.overSize = overSize;
    } finally {
      releaseSchemaWriteLock();
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
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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

  private void setAbstractInternal(final boolean isAbstract) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

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
        // SWITCH TO NOT ABSTRACT
        this.defaultClusterId = getDatabase().getDefaultClusterId();
        this.clusterIds[0] = this.defaultClusterId;
      }

      this.abstractClass = isAbstract;
    } finally {
      releaseSchemaWriteLock();
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

  public OClass setStrictMode(final boolean isStrict) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseRecord database = getDatabase();
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

  private void setStrictModeInternal(final boolean iStrict) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.strictMode = iStrict;
    } finally {
      releaseSchemaWriteLock();
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
  public int hashCode() {
    acquireSchemaReadLock();
    try {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((owner == null) ? 0 : owner.hashCode());
      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public boolean equals(final Object obj) {
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
    getDatabase().checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE);

    if (isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME))
      throw new OSecurityException("Class " + getName()
          + " cannot be truncated because has record level security enabled (extends " + OSecurityShared.RESTRICTED_CLASSNAME + ")");

    acquireSchemaReadLock();
    try {
      for (int id : clusterIds) {
        final OStorage storage = getDatabase().getStorage();
        storage.getClusterById(id).truncate();
        storage.getLevel2Cache().freeCluster(id);
      }

      for (OIndex<?> index : getClassIndexes())
        index.clear();
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

      OClass cls = this;
      do {
        if (iClassName.equalsIgnoreCase(cls.getName()) || iClassName.equalsIgnoreCase(cls.getShortName()))
          return true;

        cls = cls.getSuperClass();
      } while (cls != null);

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

      OClass cls = this;
      while (cls != null) {
        if (cls.equals(clazz))
          return true;
        cls = cls.getSuperClass();
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
      setSuperClass(getDatabase().getMetadata().getSchema().getClass(stringValue));
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
      if (isNull || stringValue.equalsIgnoreCase("clear"))
        clearCustom();
      else if (stringValue.contains("=")) {
        final List<String> words = OStringSerializerHelper.smartSplit(iValue.toString(), '=');
        setCustom(words.get(0).trim(), words.get(1).trim());
      } else {
        throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      }
      break;
    }
    return this;
  }

  public OPropertyImpl addPropertyInternal(final String name, final OType type, final OType linkedType, final OClass linkedClass) {
    if (name == null || name.length() == 0)
      throw new OSchemaException("Found property name null");

    final Character wrongCharacter = OSchemaShared.checkNameIfValid(name);
    if (wrongCharacter != null)
      throw new OSchemaException("Invalid property name found. Character '" + wrongCharacter + "' cannot be used in property name.");

    final String lowerName = name.toLowerCase();

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (properties.containsKey(lowerName))
        throw new OSchemaException("Class " + this.name + " already has property '" + name + "'");

      final OPropertyImpl prop = new OPropertyImpl(this, name, type);

      properties.put(lowerName, prop);

      if (linkedType != null)
        prop.setLinkedTypeInternal(linkedType);
      else if (linkedClass != null)
        prop.setLinkedClassInternal(linkedClass);
      return prop;
    } finally {
      releaseSchemaWriteLock();
    }
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
      final Set<String> existingFieldNames = new HashSet<String>();
      OClassImpl currentClass = this;
      do {
        existingFieldNames.addAll(currentClass.properties.keySet());
        currentClass = (OClassImpl) currentClass.getSuperClass();
      } while (currentClass != null);

      for (final String fieldToIndex : fields) {
        final String fieldName = OIndexDefinitionFactory.extractFieldName(fieldToIndex);

        if (!fieldName.equals("@rid") && !existingFieldNames.contains(fieldName.toLowerCase()))
          throw new OIndexException("Index with name : '" + name + "' cannot be created on class : '" + this.name
              + "' because field: '" + fieldName + "' is absent in class definition.");
      }

      final OIndexDefinition indexDefinition = OIndexDefinitionFactory.createIndexDefinition(this, Arrays.asList(fields),
          extractFieldTypes(fields), null);

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

      if (superClass != null)
        return currentClassResult || superClass.areIndexed(fields);
      return currentClassResult;
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

      if (superClass != null)
        result.addAll(superClass.getInvolvedIndexes(fields));

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
      return getDatabase().getMetadata().getIndexManager().getClassIndexes(name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public void getClassIndexes(final Collection<OIndex<?>> indexes) {
    acquireSchemaReadLock();
    try {
      getDatabase().getMetadata().getIndexManager().getClassIndexes(name, indexes);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex<?>> getIndexes() {
    acquireSchemaReadLock();
    try {
      final Set<OIndex<?>> indexes = getClassIndexes();
      for (OClass s = superClass; s != null; s = s.getSuperClass()) {
        s.getClassIndexes(indexes);
      }
      return indexes;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private OProperty addProperty(final String propertyName, final OType type, final OType linkedType, final OClass linkedClass) {
    if (type == null)
      throw new OSchemaException("Property type not defined.");

    if (propertyName == null || propertyName.length() == 0)
      throw new OSchemaException("Property name is null or empty");

    if (Character.isDigit(propertyName.charAt(0)))
      throw new OSchemaException("Found invalid property name. Cannot start with numbers");

    if (getDatabase().getTransaction().isActive())
      throw new OSchemaException("Cannot create a new property inside a transaction");

    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final StringBuilder cmd = new StringBuilder("create property ");
      // CLASS.PROPERTY NAME
      cmd.append(name);
      cmd.append('.');
      cmd.append(propertyName);

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

      final ODatabaseRecord database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        database.command(new OCommandSQL(cmd.toString())).execute();
        reload();

        return getProperty(propertyName);
      } else if (isDistributedCommand()) {
        final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        return addPropertyInternal(propertyName, type, linkedType, linkedClass);
      } else
        return addPropertyInternal(propertyName, type, linkedType, linkedClass);

    } finally {
      releaseSchemaWriteLock();
    }
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
    owner.releaseSchemaWriteLock();
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
    if (getDatabase().getStorage().getUnderlying() instanceof OStorageEmbedded) {
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
  private OClass addBaseClasses(final OClass iBaseClass) {
    if (baseClasses == null)
      baseClasses = new ArrayList<OClass>();

    if (baseClasses.contains(iBaseClass))
      return this;

    baseClasses.add(iBaseClass);

    // ADD CLUSTER IDS OF BASE CLASS TO THIS CLASS AND ALL SUPER-CLASSES
    OClassImpl currentClass = this;
    while (currentClass != null) {
      currentClass.addPolymorphicClusterIds((OClassImpl) iBaseClass);
      currentClass = (OClassImpl) currentClass.getSuperClass();
    }

    return this;
  }

  private void removePolymorphicClusterIds(final OClassImpl iBaseClass) {
    for (final int clusterId : iBaseClass.polymorphicClusterIds)
      removePolymorphicClusterId(clusterId);
  }

  private void removePolymorphicClusterId(int clusterId) {
    final int index = Arrays.binarySearch(polymorphicClusterIds, clusterId);
    if (index == -1)
      return;

    if (index < polymorphicClusterIds.length - 1)
      System.arraycopy(polymorphicClusterIds, index + 1, polymorphicClusterIds, index, polymorphicClusterIds.length - (index + 1));

    polymorphicClusterIds = Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length - 1);

    removeClusterFromIndexes(clusterId);

    if (superClass != null)
      superClass.removePolymorphicClusterId(clusterId);
  }

  private void removeClusterFromIndexes(int iId) {
    if (getDatabase().getStorage().getUnderlying() instanceof OStorageEmbedded) {
      final String clusterName = getDatabase().getClusterNameById(iId);
      final List<String> indexesToRemove = new ArrayList<String>();

      for (final OIndex<?> index : getIndexes())
        indexesToRemove.add(index.getName());

      final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();
      for (final String indexName : indexesToRemove)
        indexManager.removeClusterFromIndex(clusterName, indexName);
    }
  }

  private void tryDropCluster(int defaultClusterId) {
    if (name.toLowerCase().equals(getDatabase().getClusterNameById(defaultClusterId))) {
      // DROP THE DEFAULT CLUSTER CALLED WITH THE SAME NAME ONLY IF EMPTY
      if (getDatabase().getClusterRecordSizeById(defaultClusterId) == 0)
        getDatabase().dropCluster(defaultClusterId, true);
    }
  }

  private ODatabaseRecord getDatabase() {
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

  private List<OType> extractFieldTypes(String[] fieldNames) {
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

  public void checkEmbedded() {
    if (!(getDatabase().getStorage().getUnderlying() instanceof OStorageEmbedded))
      throw new OSchemaException("'Internal' schema modification methods can be used only inside of embedded database");
  }

  private boolean isDistributedCommand() {
    return getDatabase().getStorage() instanceof OAutoshardedStorage
        && OScenarioThreadLocal.INSTANCE.get() != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED;
  }

}
