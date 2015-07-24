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

import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared schema class. It's shared by all the database instances that point to the same storage.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OSchemaShared extends ODocumentWrapperNoClass implements OSchema, OCloseable {
  public static final int                       CURRENT_VERSION_NUMBER  = 4;
  public static final int                       VERSION_NUMBER_V4       = 4;
  // this is needed for guarantee the compatibility to 2.0-M1 and 2.0-M2 no changed associated with it
  public static final int                       VERSION_NUMBER_V5       = 5;
  private static final long                     serialVersionUID        = 1L;

  private final boolean                         clustersCanNotBeSharedAmongClasses;

  private final OReadersWriterSpinLock          rwSpinLock              = new OReadersWriterSpinLock();

  private final Map<String, OClass>             classes                 = new HashMap<String, OClass>();
  private final Map<Integer, OClass>            clustersToClasses       = new HashMap<Integer, OClass>();

  private final OClusterSelectionFactory        clusterSelectionFactory = new OClusterSelectionFactory();

  private final ThreadLocal<OModifiableInteger> modificationCounter     = new ThreadLocal<OModifiableInteger>() {
                                                                          @Override
                                                                          protected OModifiableInteger initialValue() {
                                                                            return new OModifiableInteger(0);
                                                                          }
                                                                        };
  private final List<OGlobalProperty>           properties              = new ArrayList<OGlobalProperty>();
  private final Map<String, OGlobalProperty>    propertiesByNameType    = new HashMap<String, OGlobalProperty>();
  private volatile int                          version                 = 0;
  private volatile boolean                      fullCheckpointOnChange  = false;

  private static final class ClusterIdsAreEmptyException extends Exception {
  }

  public OSchemaShared(boolean clustersCanNotBeSharedAmongClasses) {
    super(new ODocument());
    this.clustersCanNotBeSharedAmongClasses = clustersCanNotBeSharedAmongClasses;
  }

  public static Character checkClassNameIfValid(String iName) {
    if (iName == null)
      throw new IllegalArgumentException("Name is null");

    iName = iName.trim();

    final int nameSize = iName.length();

    if (nameSize == 0)
      throw new IllegalArgumentException("Name is empty");

    for (int i = 0; i < nameSize; ++i) {
      final char c = iName.charAt(i);
      if (c == ':' || c == ',' || c == ';' || c == ' ' || c == '%' || c == '@' || c == '=' || c == '.')
        // INVALID CHARACTER
        return c;
    }

    return null;
  }

  public static Character checkFieldNameIfValid(String iName) {
    if (iName == null)
      throw new IllegalArgumentException("Name is null");

    iName = iName.trim();

    final int nameSize = iName.length();

    if (nameSize == 0)
      throw new IllegalArgumentException("Name is empty");

    for (int i = 0; i < nameSize; ++i) {
      final char c = iName.charAt(i);
      if (c == ':' || c == ',' || c == ';' || c == ' ' || c == '%' || c == '=')
        // INVALID CHARACTER
        return c;
    }

    return null;
  }

  public boolean isFullCheckpointOnChange() {
    return fullCheckpointOnChange;
  }

  public void setFullCheckpointOnChange(boolean fullCheckpointOnChange) {
    this.fullCheckpointOnChange = fullCheckpointOnChange;
  }

  @Override
  public OImmutableSchema makeSnapshot() {
    acquireSchemaReadLock();
    try {
      return new OImmutableSchema(this);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isClustersCanNotBeSharedAmongClasses() {
    return clustersCanNotBeSharedAmongClasses;
  }

  public OClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  public int countClasses() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      return classes.size();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass createClass(final Class<?> clazz) {
    OClass result;

    int[] clusterIds = null;
    int retry = 0;

    while (true)
      try {
        acquireSchemaWriteLock();
        try {
          final Class<?> superClass = clazz.getSuperclass();
          final OClass cls;
          if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
            cls = getClass(superClass.getSimpleName());
          else
            cls = null;

          result = doCreateClass(clazz.getSimpleName(), cls, clusterIds, retry);
          break;
        } finally {
          releaseSchemaWriteLock();
        }

      } catch (ClusterIdsAreEmptyException e) {
        clusterIds = createClusters(clazz.getSimpleName());
        retry++;
      }

    return result;
  }

  public OClass createClass(final Class<?> clazz, final int iDefaultClusterId) {
    OClass result;

    int[] clusterIds = new int[] { iDefaultClusterId };
    int retry = 0;

    while (true)
      try {
        acquireSchemaWriteLock();
        try {
          final Class<?> superClass = clazz.getSuperclass();
          final OClass cls;
          if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
            cls = getClass(superClass.getSimpleName());
          else
            cls = null;

          result = doCreateClass(clazz.getSimpleName(), cls, clusterIds, retry);
        } finally {
          releaseSchemaWriteLock();
        }

        break;
      } catch (ClusterIdsAreEmptyException e) {
        clusterIds = createClusters(clazz.getSimpleName());
        retry++;
      }

    return result;
  }

  public OClass createClass(final String className) {
    return createClass(className, (OClass) null, (int[]) null);
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass) {
    return createClass(iClassName, iSuperClass, (int[]) null);
  }

  public OClass createClass(final String className, final int iDefaultClusterId) {
    return createClass(className, null, new int[] { iDefaultClusterId });
  }

  public OClass createClass(final String className, final OClass iSuperClass, final int iDefaultClusterId) {
    return createClass(className, iSuperClass, new int[] { iDefaultClusterId });
  }

  public OClass getOrCreateClass(final String iClassName) {
    return getOrCreateClass(iClassName, null);
  }

  public OClass getOrCreateClass(final String iClassName, final OClass superClass) {
    if (iClassName == null)
      return null;

    acquireSchemaReadLock();
    try {
      OClass cls = classes.get(iClassName.toLowerCase());
      if (cls != null)
        return cls;
    } finally {
      releaseSchemaReadLock();
    }

    OClass cls;

    int[] clusterIds = null;
    int retry = 0;

    while (true)
      try {
        acquireSchemaWriteLock();
        try {
          cls = classes.get(iClassName.toLowerCase());
          if (cls != null)
            return cls;

          cls = doCreateClass(iClassName, superClass, clusterIds, retry);
          if (superClass != null && !cls.isSubClassOf(superClass))
            throw new IllegalArgumentException("Class '" + iClassName + "' is not an instance of " + superClass.getShortName());

          addClusterClassMap(cls);
        } finally {
          releaseSchemaWriteLock();
        }
        break;
      } catch (ClusterIdsAreEmptyException e) {
        clusterIds = createClusters(iClassName);
        retry++;
      }

    return cls;
  }

  @Override
  public OClass createAbstractClass(final Class<?> iClass) {
    OClass cls;
    int[] clusterIds = new int[] { -1 };
    int retry = 0;

    while (true)
      try {
        acquireSchemaWriteLock();
        try {
          final Class<?> superClass = iClass.getSuperclass();
          if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
            cls = getClass(superClass.getSimpleName());
          else
            cls = null;

          cls = doCreateClass(iClass.getSimpleName(), cls, clusterIds, retry);
        } finally {
          releaseSchemaWriteLock();
        }

        break;
      } catch (ClusterIdsAreEmptyException e) {
        clusterIds = createClusters(iClass.getSimpleName());
        retry++;
      }

    return cls;
  }

  @Override
  public OClass createAbstractClass(final String className) {
    return createClass(className, null, -1);
  }

  @Override
  public OClass createAbstractClass(final String className, final OClass superClass) {
    return createClass(className, superClass, -1);
  }

  public OClass createClass(final String className, final OClass superClass, int[] clusterIds) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    if (wrongCharacter != null)
      throw new OSchemaException("Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '"
          + className + "'");

    OClass result;
    int retry = 0;

    while (true)
      try {
        result = doCreateClass(className, superClass, clusterIds, retry);
        break;
      } catch (ClusterIdsAreEmptyException e) {
        classes.remove(className.toLowerCase());
        clusterIds = createClusters(className);
        retry++;
      }

    return result;
  }

  public void checkEmbedded(OStorage storage) {
    if (!(storage.getUnderlying() instanceof OAbstractPaginatedStorage))
      throw new OSchemaException("'Internal' schema modification methods can be used only inside of embedded database");
  }

  void addClusterForClass(final int clusterId, final OClass cls) {
    acquireSchemaWriteLock();
    try {
      if (!clustersCanNotBeSharedAmongClasses)
        return;

      if (clusterId < 0)
        return;

      final OStorage storage = getDatabase().getStorage();
      checkEmbedded(storage);

      final OClass existingCls = clustersToClasses.get(clusterId);
      if (existingCls != null && !cls.equals(existingCls))
        throw new OSchemaException("Cluster with id " + clusterId + " already belongs to class " + clustersToClasses.get(clusterId));

      clustersToClasses.put(clusterId, cls);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  void removeClusterForClass(int clusterId, OClass cls) {
    acquireSchemaWriteLock();
    try {
      if (!clustersCanNotBeSharedAmongClasses)
        return;

      if (clusterId < 0)
        return;

      final OStorage storage = getDatabase().getStorage();
      checkEmbedded(storage);

      clustersToClasses.remove(clusterId);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  void checkClusterCanBeAdded(int clusterId, OClass cls) {
    acquireSchemaReadLock();
    try {
      if (!clustersCanNotBeSharedAmongClasses)
        return;

      if (clusterId < 0)
        return;

      final OClass existingCls = clustersToClasses.get(clusterId);

      if (existingCls != null && !cls.equals(existingCls))
        throw new OSchemaException("Cluster with id " + clusterId + " already belongs to class " + clustersToClasses.get(clusterId));

    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass getClassByClusterId(int clusterId) {
    acquireSchemaReadLock();
    try {
      if (!clustersCanNotBeSharedAmongClasses)
        throw new OSchemaException("This feature is not supported in current version of binary format.");

      return clustersToClasses.get(clusterId);
    } finally {
      releaseSchemaReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#dropClass(java.lang.String)
   */
  public void dropClass(final String className) {
    final ODatabaseDocumentInternal db = getDatabase();
    final OStorage storage = db.getStorage();
    final StringBuilder cmd;

    acquireSchemaWriteLock();
    try {
      if (getDatabase().getTransaction().isActive())
        throw new IllegalStateException("Cannot drop a class inside a transaction");

      if (className == null)
        throw new IllegalArgumentException("Class name is null");

      getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = className.toLowerCase();

      OClass cls = classes.get(key);

      if (cls == null)
        throw new OSchemaException("Class '" + className + "' was not found in current database");

      if (!cls.getBaseClasses().isEmpty())
        throw new OSchemaException("Class '" + className
            + "' cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");

      cmd = new StringBuilder("drop class ");
      cmd.append(className);
      cmd.append(" unsafe");

      if (isDistributedCommand()) {
        final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
        OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(autoshardedStorage.getNodeId());
        db.command(commandSQL).execute();

        dropClassInternal(className);
      } else if (storage instanceof OStorageProxy) {
        final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        db.command(commandSQL).execute();
        reload();
      } else
        dropClassInternal(className);

    } finally {
      releaseSchemaWriteLock();
    }
  }

  /**
   * Reloads the schema inside a storage's shared lock.
   */
  @Override
  public <RET extends ODocumentWrapper> RET reload() {
    rwSpinLock.acquireWriteLock();
    try {
      reload(null);

      return (RET) this;
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  public boolean existsClass(final String iClassName) {
    if (iClassName == null)
      return false;

    acquireSchemaReadLock();
    try {
      return classes.containsKey(iClassName.toLowerCase());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.Class)
   */
  public OClass getClass(final Class<?> iClass) {
    if (iClass == null)
      return null;

    return getClass(iClass.getSimpleName());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.String)
   */
  public OClass getClass(final String iClassName) {
    if (iClassName == null)
      return null;

    acquireSchemaReadLock();
    try {
      return classes.get(iClassName.toLowerCase());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void acquireSchemaReadLock() {
    rwSpinLock.acquireReadLock();
  }

  public void releaseSchemaReadLock() {
    rwSpinLock.releaseReadLock();
  }

  public void acquireSchemaWriteLock() {
    rwSpinLock.acquireWriteLock();
    modificationCounter.get().increment();
  }

  public void releaseSchemaWriteLock() {
    releaseSchemaWriteLock(true);
  }

  public void releaseSchemaWriteLock(final boolean iSave) {
    try {
      if (modificationCounter.get().intValue() == 1) {
        // if it is embedded storage modification of schema is done by internal methods otherwise it is done by
        // by sql commands and we need to reload local replica

        if (iSave)
          if (getDatabase().getStorage().getUnderlying() instanceof OAbstractPaginatedStorage)
            saveInternal();
          else
            reload();

        version++;
      }
    } finally {
      rwSpinLock.releaseWriteLock();
      modificationCounter.get().decrement();
    }

    assert modificationCounter.get().intValue() >= 0;

    if (modificationCounter.get().intValue() == 0 && getDatabase().getStorage().getUnderlying() instanceof OStorageProxy) {
      getDatabase().getStorage().reload();
    }
  }

  void changeClassName(final String oldName, final String newName, OClass cls) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded(getDatabase().getStorage());

      if (oldName != null)
        classes.remove(oldName.toLowerCase());
      if (newName != null)
        classes.put(newName.toLowerCase(), cls);

    } finally {
      releaseSchemaWriteLock();
    }
  }

  /**
   * Binds ODocument to POJO.
   */
  @Override
  public void fromStream() {
    rwSpinLock.acquireWriteLock();
    modificationCounter.get().increment();
    try {
      // READ CURRENT SCHEMA VERSION
      final Integer schemaVersion = (Integer) document.field("schemaVersion");
      if (schemaVersion == null) {
        OLogManager
            .instance()
            .error(
                this,
                "Database's schema is empty! Recreating the system classes and allow the opening of the database but double check the integrity of the database");
        return;
      } else if (schemaVersion != CURRENT_VERSION_NUMBER && VERSION_NUMBER_V5 != schemaVersion) {
        // VERSION_NUMBER_V5 is needed for guarantee the compatibility to 2.0-M1 and 2.0-M2 no changed associated with it
        // HANDLE SCHEMA UPGRADE
        throw new OConfigurationException(
            "Database schema is different. Please export your old database with the previous version of OrientDB and reimport it using the current one.");
      }

      properties.clear();
      propertiesByNameType.clear();
      List<ODocument> globalProperties = document.field("globalProperties");
      boolean hasGlobalProperties = false;
      if (globalProperties != null) {
        hasGlobalProperties = true;
        for (ODocument oDocument : globalProperties) {
          OGlobalPropertyImpl prop = new OGlobalPropertyImpl();
          prop.fromDocument(oDocument);
          ensurePropertiesSize(prop.getId());
          properties.set(prop.getId(), prop);
          propertiesByNameType.put(prop.getName() + "|" + prop.getType().name(), prop);
        }
      }
      // REGISTER ALL THE CLASSES
      clustersToClasses.clear();

      final Map<String, OClass> newClasses = new HashMap<String, OClass>();

      OClassImpl cls;
      Collection<ODocument> storedClasses = document.field("classes");
      for (ODocument c : storedClasses) {

        cls = new OClassImpl(this, c);
        cls.fromStream();

        if (classes.containsKey(cls.getName().toLowerCase())) {
          cls = (OClassImpl) classes.get(cls.getName().toLowerCase());
          cls.fromStream(c);
        }

        newClasses.put(cls.getName().toLowerCase(), cls);

        if (cls.getShortName() != null)
          newClasses.put(cls.getShortName().toLowerCase(), cls);

        addClusterClassMap(cls);
      }

      classes.clear();
      classes.putAll(newClasses);

      // REBUILD THE INHERITANCE TREE
      String superClassName;
      OClass superClass;
      for (ODocument c : storedClasses) {
        superClassName = c.field("superClass");

        if (superClassName != null) {
          // HAS A SUPER CLASS
          cls = (OClassImpl) classes.get(((String) c.field("name")).toLowerCase());

          superClass = classes.get(superClassName.toLowerCase());

          if (superClass == null)
            throw new OConfigurationException("Super class '" + superClassName + "' was declared in class '" + cls.getName()
                + "' but was not found in schema. Remove the dependency or create the class to continue.");

          cls.setSuperClassInternal(superClass);
        }
      }

      if (!hasGlobalProperties) {
        if (getDatabase().getStorage().getUnderlying() instanceof OAbstractPaginatedStorage)
          saveInternal();
      }

    } finally {
      version++;
      modificationCounter.get().decrement();
      rwSpinLock.releaseWriteLock();
    }
  }

  /**
   * Binds POJO to ODocument.
   */
  @Override
  @OBeforeSerialization
  public ODocument toStream() {
    rwSpinLock.acquireReadLock();
    try {
      document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      try {
        document.field("schemaVersion", CURRENT_VERSION_NUMBER);

        Set<ODocument> cc = new HashSet<ODocument>();
        for (OClass c : classes.values())
          cc.add(((OClassImpl) c).toStream());

        document.field("classes", cc, OType.EMBEDDEDSET);

        List<ODocument> globalProperties = new ArrayList<ODocument>();
        for (OGlobalProperty globalProperty : properties) {
          if (globalProperty != null)
            globalProperties.add(((OGlobalPropertyImpl) globalProperty).toDocument());
        }
        document.field("globalProperties", globalProperties, OType.EMBEDDEDLIST);
      } finally {
        document.setInternalStatus(ORecordElement.STATUS.LOADED);
      }

      return document;
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  public Collection<OClass> getClasses() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    acquireSchemaReadLock();
    try {
      return new HashSet<OClass>(classes.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(final String clusterName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final int clusterId = getDatabase().getClusterIdByName(clusterName);
      final Set<OClass> result = new HashSet<OClass>();
      for (OClass c : classes.values()) {
        if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId))
          result.add(c);
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OSchemaShared load() {
    rwSpinLock.acquireWriteLock();
    try {
      getDatabase();
      ((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().schemaRecordId);
      reload("*:-1 index:0");

      return this;
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  public void create() {
    rwSpinLock.acquireWriteLock();
    try {
      final ODatabaseDocumentInternal db = getDatabase();
      super.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      db.getStorage().getConfiguration().schemaRecordId = document.getIdentity().toString();
      db.getStorage().getConfiguration().update();
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  public void close(boolean onDelete) {
    rwSpinLock.acquireWriteLock();
    try {
      classes.clear();
      document.clear();
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  @Deprecated
  public int getVersion() {
    return version;
  }

  public ORID getIdentity() {
    acquireSchemaReadLock();
    try {
      return document.getIdentity();
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Avoid to handle this by user API.
   */
  @Override
  public <RET extends ODocumentWrapper> RET save() {
    return (RET) this;
  }

  /**
   * Avoid to handle this by user API.
   */
  @Override
  public <RET extends ODocumentWrapper> RET save(final String iClusterName) {
    return (RET) this;
  }

  public OSchemaShared setDirty() {
    rwSpinLock.acquireWriteLock();
    try {
      document.setDirty();
      return this;
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  public OGlobalProperty getGlobalPropertyById(int id) {
    if (id >= properties.size())
      return null;
    return properties.get(id);
  }

  public OGlobalProperty createGlobalProperty(String name, OType type, Integer id) {
    OGlobalProperty global;
    if (id < properties.size() && (global = properties.get(id)) != null) {
      if (!global.getName().equals(name) || !global.getType().equals(type))
        throw new OSchemaException("A property with id " + id + " already exist ");
      return global;
    }

    global = new OGlobalPropertyImpl(name, type, id);
    ensurePropertiesSize(id);
    properties.set(id, global);
    propertiesByNameType.put(global.getName() + "|" + global.getType().name(), global);
    return global;
  }

  public List<OGlobalProperty> getGlobalProperties() {
    return Collections.unmodifiableList(properties);
  }

  protected OGlobalProperty findOrCreateGlobalProperty(String name, OType type) {
    OGlobalProperty global = propertiesByNameType.get(name + "|" + type.name());
    if (global == null) {
      int id = properties.size();
      global = new OGlobalPropertyImpl(name, type, id);
      properties.add(id, global);
      propertiesByNameType.put(global.getName() + "|" + global.getType().name(), global);
    }
    return global;
  }

  private OClass doCreateClass(final String className, final OClass superClass, final int[] clusterIds, int retry)
      throws ClusterIdsAreEmptyException {
    OClass result;

    final ODatabaseDocumentInternal db = getDatabase();
    final OStorage storage = db.getStorage();
    StringBuilder cmd = null;

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    acquireSchemaWriteLock();
    try {

      final String key = className.toLowerCase();
      if (classes.containsKey(key) && retry == 0)
        throw new OSchemaException("Class " + className + " already exists in current database");

      if (!isDistributedCommand())
        checkClustersAreAbsent(clusterIds);

      cmd = new StringBuilder("create class ");
      cmd.append(className);

      if (superClass != null) {
        cmd.append(" extends ");
        cmd.append(superClass.getName());
      }

      if (clusterIds != null) {
        if (clusterIds.length == 1 && clusterIds[0] == -1)
          cmd.append(" abstract");
        else {
          cmd.append(" cluster ");
          for (int i = 0; i < clusterIds.length; ++i) {
            if (i > 0)
              cmd.append(',');
            else
              cmd.append(' ');

            cmd.append(clusterIds[i]);
          }
        }
      }

      if (isDistributedCommand()) {
        createClassInternal(className, superClass, clusterIds);

        final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
        OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(autoshardedStorage.getNodeId());

        final Object res = db.command(commandSQL).execute();

      } else if (storage instanceof OStorageProxy) {
        db.command(new OCommandSQL(cmd.toString())).execute();
        reload();
      } else
        createClassInternal(className, superClass, clusterIds);

      result = classes.get(className.toLowerCase());

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext();)
        it.next().onCreateClass(getDatabase(), result);

    } finally {
      releaseSchemaWriteLock();
    }

    return result;
  }

  private boolean isDistributedCommand() {
    return getDatabase().getStorage() instanceof OAutoshardedStorage
        && OScenarioThreadLocal.INSTANCE.get() != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED;
  }

  private OClass createClassInternal(final String className, final OClass superClass, final int[] clusterIdsToAdd)
      throws ClusterIdsAreEmptyException {
    acquireSchemaWriteLock();
    try {
      if (className == null || className.length() == 0)
        throw new OSchemaException("Found class name null or empty");

      if (Character.isDigit(className.charAt(0)))
        throw new OSchemaException("Found invalid class name. Cannot start with numbers");

      final Character wrongCharacter = checkClassNameIfValid(className);
      if (wrongCharacter != null)
        throw new OSchemaException("Found invalid class name. Character '" + wrongCharacter + "' cannot be used in class name.");

      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      checkEmbedded(storage);

      checkClustersAreAbsent(clusterIdsToAdd);

      final int[] clusterIds;
      if (clusterIdsToAdd == null || clusterIdsToAdd.length == 0) {
        throw new ClusterIdsAreEmptyException();

      } else
        clusterIds = clusterIdsToAdd;

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);

      final String key = className.toLowerCase();

      if (classes.containsKey(key))
        throw new OSchemaException("Class " + className + " already exists in current database");

      OClassImpl cls = new OClassImpl(this, className, clusterIds);

      classes.put(key, cls);
      if (cls.getShortName() != null)
        // BIND SHORT NAME TOO
        classes.put(cls.getShortName().toLowerCase(), cls);

      if (superClass != null) {
        cls.setSuperClassInternal(superClass);

        // UPDATE INDEXES
        final int[] clustersToIndex = superClass.getPolymorphicClusterIds();
        final String[] clusterNames = new String[clustersToIndex.length];
        for (int i = 0; i < clustersToIndex.length; i++)
          clusterNames[i] = database.getClusterNameById(clustersToIndex[i]);

        for (OIndex<?> index : superClass.getIndexes())
          for (String clusterName : clusterNames)
            if (clusterName != null)
              database.getMetadata().getIndexManager().addClusterToIndex(clusterName, index.getName());
      }

      addClusterClassMap(cls);

      return cls;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private int[] createClusters(String className) {
    className = className.toLowerCase();

    final ODatabaseDocumentInternal database = getDatabase();
    final OStorage storage = database.getStorage();

    int[] clusterIds;// CREATE A NEW CLUSTER(S)
    final int minimumClusters = storage.getConfiguration().getMinimumClusters();

    clusterIds = new int[minimumClusters];
    if (minimumClusters <= 1) {
      clusterIds[0] = database.getClusterIdByName(className);
      if (clusterIds[0] == -1)
        clusterIds[0] = database.addCluster(className);
    } else
      for (int i = 0; i < minimumClusters; ++i) {
        clusterIds[i] = database.getClusterIdByName(className + "_" + i);
        if (clusterIds[i] == -1)
          clusterIds[i] = database.addCluster(className + "_" + i);
      }
    return clusterIds;
  }

  private void dropClassInternal(final String className) {
    acquireSchemaWriteLock();
    try {
      if (getDatabase().getTransaction().isActive())
        throw new IllegalStateException("Cannot drop a class inside a transaction");

      if (className == null)
        throw new IllegalArgumentException("Class name is null");

      getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = className.toLowerCase();

      final OClass cls = classes.get(key);
      if (cls == null)
        throw new OSchemaException("Class '" + className + "' was not found in current database");

      if (!cls.getBaseClasses().isEmpty())
        throw new OSchemaException("Class '" + className
            + "' cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");

      checkEmbedded(getDatabase().getStorage());

      if (cls.getSuperClass() != null)
        // REMOVE DEPENDENCY FROM SUPERCLASS
        ((OClassImpl) cls.getSuperClass()).removeBaseClassInternal(cls);

      dropClassIndexes(cls);

      classes.remove(key);

      if (cls.getShortName() != null)
        // REMOVE THE ALIAS TOO
        classes.remove(cls.getShortName().toLowerCase());

      removeClusterClassMap(cls);

      deleteDefaultCluster(cls);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void deleteDefaultCluster(OClass clazz) {
    final ODatabaseDocumentInternal database = getDatabase();
    final int clusterId = clazz.getDefaultClusterId();
    final OCluster cluster = database.getStorage().getClusterById(clusterId);

    if (cluster.getName().equalsIgnoreCase(clazz.getName()))
      database.getStorage().dropCluster(clusterId, true);
  }

  private void saveInternal() {
    final ODatabaseDocument db = getDatabase();

    if (db.getTransaction().isActive()) {
      reload(null, true);
      throw new OSchemaException("Cannot change the schema while a transaction is active. Schema changes are not transactional");
    }

    setDirty();

    try {
      super.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      if (fullCheckpointOnChange)
        getDatabase().getStorage().synch();
    } catch (OConcurrentModificationException e) {
      reload(null, true);
      throw e;
    }
  }

  private void addClusterClassMap(final OClass cls) {
    if (!clustersCanNotBeSharedAmongClasses)
      return;

    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0)
        continue;

      clustersToClasses.put(clusterId, cls);
    }

  }

  private void removeClusterClassMap(final OClass cls) {
    if (!clustersCanNotBeSharedAmongClasses)
      return;

    for (int clusterId : cls.getClusterIds()) {
      if (clusterId < 0)
        continue;

      clustersToClasses.remove(clusterId);
    }

  }

  private void checkClustersAreAbsent(final int[] iClusterIds) {
    if (!clustersCanNotBeSharedAmongClasses || iClusterIds == null)
      return;

    for (int clusterId : iClusterIds) {
      if (clusterId < 0)
        continue;

      if (clustersToClasses.containsKey(clusterId))
        throw new OSchemaException("Cluster with id " + clusterId + " already belongs to class " + clustersToClasses.get(clusterId));
    }
  }

  private void dropClassIndexes(final OClass cls) {
    final ODatabaseDocument database = getDatabase();
    final OIndexManager indexManager = database.getMetadata().getIndexManager();

    for (final OIndex<?> index : indexManager.getClassIndexes(cls.getName()))
      indexManager.dropIndex(index.getName());
  }

  private OClass cascadeCreate(final Class<?> javaClass) {
    final OClassImpl cls = (OClassImpl) createClass(javaClass.getSimpleName());

    final Class<?> javaSuperClass = javaClass.getSuperclass();
    if (javaSuperClass != null && !javaSuperClass.getName().equals("java.lang.Object")
        && !javaSuperClass.getName().startsWith("com.orientechnologies")) {
      OClass superClass = classes.get(javaSuperClass.getSimpleName().toLowerCase());
      if (superClass == null)
        superClass = cascadeCreate(javaSuperClass);
      cls.setSuperClass(superClass);
    }

    return cls;
  }

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private void ensurePropertiesSize(int size) {
    while (properties.size() <= size)
      properties.add(null);
  }
}
