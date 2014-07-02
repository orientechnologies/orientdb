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

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Shared schema class. It's shared by all the database instances that point to the same storage.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OSchemaShared extends ODocumentWrapperNoClass implements OSchema, OCloseable {
  public static final int                       CURRENT_VERSION_NUMBER  = 4;
  private static final long                     serialVersionUID        = 1L;

  private static final String                   DROP_INDEX_QUERY        = "drop index ";

  private final boolean                         clustersCanNotBeSharedAmongClasses;

  private final ReadWriteLock                   readWriteLock           = new ReentrantReadWriteLock();

  private final Map<String, OClass>             classes                 = new HashMap<String, OClass>();
  private final Map<Integer, OClass>            clustersToClasses       = new HashMap<Integer, OClass>();

  private final OClusterSelectionFactory        clusterSelectionFactory = new OClusterSelectionFactory();

  private final ThreadLocal<OModifiableInteger> reloadCounter           = new ThreadLocal<OModifiableInteger>() {
                                                                          @Override
                                                                          protected OModifiableInteger initialValue() {
                                                                            return new OModifiableInteger(0);
                                                                          }
                                                                        };

  public OSchemaShared(boolean clustersCanNotBeSharedAmongClasses) {
    super(new ODocument());
    this.clustersCanNotBeSharedAmongClasses = clustersCanNotBeSharedAmongClasses;
  }

  public static Character checkNameIfValid(String iName) {
    if (iName == null)
      throw new IllegalArgumentException("Name is null");

    iName = iName.trim();

    final int nameSize = iName.length();

    if (nameSize == 0)
      throw new IllegalArgumentException("Name is empty");

    for (int i = 0; i < nameSize; ++i) {
      final char c = iName.charAt(i);
      if (c == ':' || c == ',' || c == ' ' || c == '%')
        // INVALID CHARACTER
        return c;
    }

    return null;
  }

  public OClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  public int countClasses() {
    readWriteLock.readLock().lock();
    try {
      getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
      return classes.size();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public OClass createClass(final Class<?> clazz) {
    final OClass result;

    readWriteLock.writeLock().lock();
    startReloadRequest();
    try {
      final Class<?> superClass = clazz.getSuperclass();
      final OClass cls;
      if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
        cls = getClass(superClass.getSimpleName());
      else
        cls = null;

      result = createClass(clazz.getSimpleName(), cls, OStorage.CLUSTER_TYPE.PHYSICAL);
    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();

    return result;
  }

  public OClass createClass(final Class<?> clazz, final int iDefaultClusterId) {
    final OClass result;

    readWriteLock.writeLock().lock();
    startReloadRequest();
    try {
      final Class<?> superClass = clazz.getSuperclass();
      final OClass cls;
      if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
        cls = getClass(superClass.getSimpleName());
      else
        cls = null;

      result = createClass(clazz.getSimpleName(), cls, iDefaultClusterId);
    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();

    return result;
  }

  public OClass createClass(final String className) {
    return createClass(className, (OClass) null, (int[]) null);
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass) {
    return createClass(iClassName, iSuperClass, OStorage.CLUSTER_TYPE.PHYSICAL);
  }

  public OClass createClass(final String className, final OClass superClass, final OStorage.CLUSTER_TYPE type) {
    OClass result;

    readWriteLock.writeLock().lock();
    startReloadRequest();
    try {
      if (getDatabase().getTransaction().isActive())
        throw new IllegalStateException("Cannot create class " + className + " inside a transaction");

      int clusterId = getDatabase().getClusterIdByName(className);
      if (clusterId == -1)
        // CREATE A NEW CLUSTER
        clusterId = createCluster(type.toString(), className);

      result = createClass(className, superClass, clusterId);
    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();

    return result;
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

  public OClass getOrCreateClass(final String iClassName, final OClass iSuperClass) {
    readWriteLock.readLock().lock();
    try {
      OClass cls = classes.get(iClassName.toLowerCase());
      if (cls != null)
        return cls;
    } finally {
      readWriteLock.readLock().unlock();
    }

    OClass cls;

    readWriteLock.writeLock().lock();
    startReloadRequest();
    try {
      cls = classes.get(iClassName.toLowerCase());
      if (cls != null)
        return cls;

      cls = createClass(iClassName, iSuperClass);
      if (iSuperClass != null && !cls.isSubClassOf(iSuperClass))
        throw new IllegalArgumentException("Class '" + iClassName + "' is not an instance of " + iSuperClass.getShortName());

      addClusterClassMap(cls);
    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();

    return cls;
  }

  @Override
  public OClass createAbstractClass(final Class<?> iClass) {
    OClass cls;

    startReloadRequest();
    readWriteLock.writeLock().lock();
    try {
      final Class<?> superClass = iClass.getSuperclass();
      if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
        cls = getClass(superClass.getSimpleName());
      else
        cls = null;

      cls = createClass(iClass.getSimpleName(), cls, -1);
    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();

    return cls;
  }

  @Override
  public OClass createAbstractClass(final String сlassName) {
    return createClass(сlassName, null, -1);
  }

  @Override
  public OClass createAbstractClass(final String сlassName, final OClass superClass) {
    return createClass(сlassName, superClass, -1);
  }

  public OClass createClass(final String className, final OClass superClass, final int[] clusterIds) {
    OClass result;

    startReloadRequest();
    readWriteLock.writeLock().lock();
    try {
      getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_CREATE);

      StringBuilder cmd = null;

      final String key = className.toLowerCase();
      if (classes.containsKey(key))
        throw new OSchemaException("Class " + className + " already exists in current database");

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

      final ODatabaseRecord db = getDatabase();
      final OStorage storage = db.getStorage();

      if (storage instanceof OAutoshardedStorage) {
        final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
        OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(autoshardedStorage.getNodeId());

        db.command(commandSQL).execute();

        commandSQL = db.command(commandSQL);
        storage.getUnderlying().command(commandSQL);
      } else
        db.command(new OCommandSQL(cmd.toString())).execute();

      if (storage instanceof OStorageProxy)
        reload();

      result = classes.get(className.toLowerCase());
    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();

    return result;
  }

  public OClass createClassInternal(final String className, final OClass superClass, final int[] clusterIdsToAdd) {
    readWriteLock.writeLock().lock();
    try {
      if (className == null || className.length() == 0)
        throw new OSchemaException("Found class name null or empty");

      if (Character.isDigit(className.charAt(0)))
        throw new OSchemaException("Found invalid class name. Cannot start with numbers");

      final Character wrongCharacter = checkNameIfValid(className);
      if (wrongCharacter != null)
        throw new OSchemaException("Found invalid class name. Character '" + wrongCharacter + "' cannot be used in class name.");

      final ODatabaseRecord database = getDatabase();

      checkClustersAreAbsent(clusterIdsToAdd);

      final int[] clusterIds;
      if (clusterIdsToAdd == null || clusterIdsToAdd.length == 0) {
        // CREATE A NEW CLUSTER(S)
        final int minimumClusters = database.getStorage().getConfiguration().getMinimumClusters();

        clusterIds = new int[minimumClusters];
        if (minimumClusters <= 1)
          clusterIds[0] = database.addCluster(CLUSTER_TYPE.PHYSICAL.toString(), className, null, null);
        else
          for (int i = 0; i < minimumClusters; ++i) {
            clusterIds[i] = database.getClusterIdByName(className + "_" + i);
            if (clusterIds[i] == -1)
              clusterIds[i] = database.addCluster(CLUSTER_TYPE.PHYSICAL.toString(), className + "_" + i, null, null);
          }
      } else
        clusterIds = clusterIdsToAdd;

      database.checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_CREATE);

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
        if (!(getDatabase().getStorage() instanceof OStorageProxy)) {
          final int[] clustersToIndex = superClass.getPolymorphicClusterIds();
          final String[] clusterNames = new String[clustersToIndex.length];
          for (int i = 0; i < clustersToIndex.length; i++)
            clusterNames[i] = database.getClusterNameById(clustersToIndex[i]);

          for (OIndex<?> index : superClass.getIndexes())
            for (String clusterName : clusterNames)
              if (clusterName != null)
                database.getMetadata().getIndexManager().addClusterToIndex(clusterName, index.getName());
        }
      }

      addClusterClassMap(cls);

      saveInternal();

      return cls;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  void addClusterForClass(final int clusterId, final OClass cls) {
    if (!clustersCanNotBeSharedAmongClasses)
      return;

    if (clusterId < 0)
      return;

    final OClass existingCls = clustersToClasses.get(clusterId);
    if (existingCls != null && !cls.equals(existingCls))
      throw new OSchemaException("Cluster with id " + clusterId + " already belongs to class " + clustersToClasses.get(clusterId));

    clustersToClasses.put(clusterId, cls);
  }

  void removeClusterForClass(int clusterId, OClass cls) {
    if (!clustersCanNotBeSharedAmongClasses)
      return;

    if (clusterId < 0)
      return;

    clustersToClasses.remove(clusterId);
  }

  void checkClusterCanBeAdded(int clusterId, OClass cls) {
    if (!clustersCanNotBeSharedAmongClasses)
      return;

    if (clusterId < 0)
      return;

    final OClass existingCls = clustersToClasses.get(clusterId);

    if (existingCls != null && !cls.equals(existingCls))
      throw new OSchemaException("Cluster with id " + clusterId + " already belongs to class " + clustersToClasses.get(clusterId));
  }

  public OClass getClassByClusterId(int clusterId) {
    readWriteLock.readLock().lock();
    try {
      if (!clustersCanNotBeSharedAmongClasses)
        throw new OSchemaException("This feature is not supported in current version of binary format.");

      return clustersToClasses.get(clusterId);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#dropClass(java.lang.String)
   */
  public void dropClass(final String className) {
    readWriteLock.writeLock().lock();
    startReloadRequest();
    try {
      if (getDatabase().getTransaction().isActive())
        throw new IllegalStateException("Cannot drop a class inside a transaction");

      if (className == null)
        throw new IllegalArgumentException("Class name is null");

      getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = className.toLowerCase();

      OClass cls = classes.get(key);

      if (cls == null)
        throw new OSchemaException("Class " + className + " was not found in current database");

      if (!cls.getBaseClasses().isEmpty())
        throw new OSchemaException("Class " + className
            + " cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");

      final ODatabaseRecord db = getDatabase();
      final OStorage storage = db.getStorage();

      final StringBuilder cmd = new StringBuilder("drop class ");
      cmd.append(className);

      if (storage instanceof OAutoshardedStorage) {
        final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
        OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(autoshardedStorage.getNodeId());
        db.command(commandSQL).execute();

        commandSQL = db.command(commandSQL);
        storage.getUnderlying().command(commandSQL);
      } else {
        final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        db.command(commandSQL).execute();
      }

      if (storage instanceof OStorageProxy)
        reload();

    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();
  }

  public void dropClassInternal(final String iClassName) {
    readWriteLock.writeLock().lock();
    try {
      if (getDatabase().getTransaction().isActive())
        throw new IllegalStateException("Cannot drop a class inside a transaction");

      if (iClassName == null)
        throw new IllegalArgumentException("Class name is null");

      getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = iClassName.toLowerCase();

      final OClass cls = classes.get(key);
      if (cls == null)
        throw new OSchemaException("Class " + iClassName + " was not found in current database");

      if (!cls.getBaseClasses().isEmpty())
        throw new OSchemaException("Class " + iClassName
            + " cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");

      if (cls.getSuperClass() != null) {
        // REMOVE DEPENDENCY FROM SUPERCLASS
        ((OClassImpl) cls.getSuperClass()).removeBaseClassInternal(cls);
      }

      dropClassIndexes(cls);

      classes.remove(key);

      if (cls.getShortName() != null)
        // REMOVE THE ALIAS TOO
        classes.remove(cls.getShortName().toLowerCase());

      removeClusterClassMap(cls);
      saveInternal();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Reloads the schema inside a storage's shared lock.
   */
  @Override
  public <RET extends ODocumentWrapper> RET reload() {
    readWriteLock.writeLock().lock();
    try {
      reload(null);

      return (RET) this;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public boolean existsClass(final String className) {
    readWriteLock.readLock().lock();
    try {
      return classes.containsKey(className.toLowerCase());
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.Class)
   */
  public OClass getClass(final Class<?> iClass) {
    return getClass(iClass.getSimpleName());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#getClass(java.lang.String)
   */
  public OClass getClass(final String iClassName) {
    readWriteLock.readLock().lock();
    try {
      if (iClassName == null)
        return null;

      OClass cls = classes.get(iClassName.toLowerCase());
      if (cls != null)
        return cls;
    } finally {
      readWriteLock.readLock().unlock();
    }

    OClass cls = null;
    startReloadRequest();
    readWriteLock.writeLock().lock();
    try {
      if (getDatabase().getDatabaseOwner() instanceof ODatabaseObject) {
        // CHECK IF CAN AUTO-CREATE IT
        final ODatabase ownerDb = getDatabase().getDatabaseOwner();
        if (ownerDb instanceof ODatabaseObject) {
          final Class<?> javaClass = ((ODatabaseObject) ownerDb).getEntityManager().getEntityClass(iClassName);

          if (javaClass != null) {
            // AUTO REGISTER THE CLASS AT FIRST USE
            cls = cascadeCreate(javaClass);
          }
        }
      }
    } finally {
      readWriteLock.writeLock().unlock();
      endReloadRequest();
    }

    reloadStorageMetadata();

    return cls;
  }

  void changeClassName(final String oldName, final String newName, OClass cls) {
    if (oldName != null)
      classes.remove(oldName.toLowerCase());
    if (newName != null)
      classes.put(newName.toLowerCase(), cls);
  }

  /**
   * Binds ODocument to POJO.
   */
  @Override
  public void fromStream() {
    readWriteLock.writeLock().lock();
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
      } else if (schemaVersion != CURRENT_VERSION_NUMBER) {
        // HANDLE SCHEMA UPGRADE
        throw new OConfigurationException(
            "Database schema is different. Please export your old database with the previous version of OrientDB and reimport it using the current one.");
      }

      // REGISTER ALL THE CLASSES
      classes.clear();
      clustersToClasses.clear();

      OClassImpl cls;
      Collection<ODocument> storedClasses = document.field("classes");
      for (ODocument c : storedClasses) {
        cls = new OClassImpl(this, c);
        cls.fromStream();
        classes.put(cls.getName().toLowerCase(), cls);

        if (cls.getShortName() != null)
          classes.put(cls.getShortName().toLowerCase(), cls);

        addClusterClassMap(cls);
      }

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
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Binds POJO to ODocument.
   */
  @Override
  @OBeforeSerialization
  public ODocument toStream() {
    readWriteLock.writeLock().lock();
    try {
      document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      try {
        document.field("schemaVersion", CURRENT_VERSION_NUMBER);

        Set<ODocument> cc = new HashSet<ODocument>();
        for (OClass c : classes.values())
          cc.add(((OClassImpl) c).toStream());

        document.field("classes", cc, OType.EMBEDDEDSET);

      } finally {
        document.setInternalStatus(ORecordElement.STATUS.LOADED);
      }

      return document;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public Collection<OClass> getClasses() {
    readWriteLock.readLock().lock();
    try {
      getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
      return new HashSet<OClass>(classes.values());
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(final String clusterName) {
    readWriteLock.readLock().lock();
    try {
      getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

      final int clusterId = getDatabase().getClusterIdByName(clusterName);
      final Set<OClass> result = new HashSet<OClass>();
      for (OClass c : classes.values()) {
        if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId))
          result.add(c);
      }

      return result;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public OSchemaShared load() {
    readWriteLock.writeLock().lock();
    try {
      getDatabase();
      ((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().schemaRecordId);
      reload("*:-1 index:0");

      return this;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void create() {
    readWriteLock.writeLock().lock();
    try {
      final ODatabaseRecord db = getDatabase();
      super.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      db.getStorage().getConfiguration().schemaRecordId = document.getIdentity().toString();
      db.getStorage().getConfiguration().update();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void close(boolean onDelete) {
    readWriteLock.writeLock().lock();
    try {
      classes.clear();
      document.clear();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void saveInternal() {
    readWriteLock.writeLock().lock();
    try {
      final ODatabaseRecord db = getDatabase();

      if (db.getTransaction().isActive())
        throw new OSchemaException("Cannot change the schema while a transaction is active. Schema changes are not transactional");

      saveInternal(OMetadataDefault.CLUSTER_INTERNAL_NAME);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Deprecated
  public int getVersion() {
    readWriteLock.readLock().lock();
    try {
      return document.getRecordVersion().getCounter();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public ORID getIdentity() {
    readWriteLock.readLock().lock();
    try {
      return document.getIdentity();
    } finally {
      readWriteLock.readLock().unlock();
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
    readWriteLock.writeLock().lock();
    try {
      document.setDirty();
      return this;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void reloadStorageMetadata() {
    if (reloadCounter.get().intValue() == 0 && getDatabase().getStorage() instanceof OStorageProxy)
      getDatabase().getStorage().reload();
  }

  private void startReloadRequest() {
    reloadCounter.get().increment();
  }

  private void endReloadRequest() {
    reloadCounter.get().decrement();

    assert reloadCounter.get().intValue() >= 0;
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

  private int createCluster(final String iType, final String iClassName) {
    return getDatabase().addCluster(iType, iClassName, null, null);
  }

  private void checkClustersAreAbsent(int[] iClusterIds) {
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
    for (final OIndex<?> index : getDatabase().getMetadata().getIndexManager().getClassIndexes(cls.getName())) {
      getDatabase().command(new OCommandSQL(DROP_INDEX_QUERY + index.getName()));
    }
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

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private void saveInternal(final String iClusterName) {
    document.setDirty();
    for (int retry = 0; retry < 10; retry++)
      try {
        super.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
        break;
      } catch (OConcurrentModificationException e) {
        reload(null, true);
      }

    super.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
  }
}
