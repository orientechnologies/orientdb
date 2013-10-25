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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
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
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Shared schema class. It's shared by all the database instances that point to the same storage.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OSchemaShared extends ODocumentWrapperNoClass implements OSchema, OCloseable {
  private static final long     serialVersionUID       = 1L;

  public static final int       CURRENT_VERSION_NUMBER = 4;
  private static final String   DROP_INDEX_QUERY       = "drop index ";
  protected Map<String, OClass> classes                = new HashMap<String, OClass>();

  public OSchemaShared(final int schemaClusterId) {
    super(new ODocument());
  }

  public int countClasses() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
    return getDatabase().getStorage().callInLock(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return classes.size();
      }
    }, false);
  }

  public OClass createClass(final Class<?> iClass) {
    final Class<?> superClass = iClass.getSuperclass();
    final OClass cls;
    if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
      cls = getClass(superClass.getSimpleName());
    else
      cls = null;

    return createClass(iClass.getSimpleName(), cls, OStorage.CLUSTER_TYPE.PHYSICAL);
  }

  public OClass createClass(final Class<?> iClass, final int iDefaultClusterId) {
    final Class<?> superClass = iClass.getSuperclass();
    final OClass cls;
    if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
      cls = getClass(superClass.getSimpleName());
    else
      cls = null;

    return createClass(iClass.getSimpleName(), cls, iDefaultClusterId);
  }

  public OClass createClass(final String iClassName) {
    return createClass(iClassName, null, OStorage.CLUSTER_TYPE.PHYSICAL);
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass) {
    return createClass(iClassName, iSuperClass, OStorage.CLUSTER_TYPE.PHYSICAL);
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass, final OStorage.CLUSTER_TYPE iType) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot create class " + iClassName + " inside a transaction");

    int clusterId = getDatabase().getClusterIdByName(iClassName);
    if (clusterId == -1)
      // CREATE A NEW CLUSTER
      clusterId = createCluster(iType.toString(), iClassName);

    return createClass(iClassName, iSuperClass, clusterId);
  }

  public OClass createClass(final String iClassName, final int iDefaultClusterId) {
    return createClass(iClassName, null, new int[] { iDefaultClusterId });
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass, final int iDefaultClusterId) {
    return createClass(iClassName, iSuperClass, new int[] { iDefaultClusterId });
  }

  public OClass getOrCreateClass(final String iClassName) {
    return getOrCreateClass(iClassName, null);
  }

  public OClass getOrCreateClass(final String iClassName, final OClass iSuperClass) {
    return getDatabase().getStorage().callInLock(new Callable<OClass>() {
      @Override
      public OClass call() throws Exception {
        OClass cls = classes.get(iClassName.toLowerCase());
        if (cls == null)
          cls = createClass(iClassName, iSuperClass);
        else if (iSuperClass != null && !cls.isSubClassOf(iSuperClass))
          throw new IllegalArgumentException("Class '" + iClassName + "' is not an instance of " + iSuperClass.getShortName());
        return cls;
      }
    }, true);
  }

  @Override
  public OClass createAbstractClass(final Class<?> iClass) {
    final Class<?> superClass = iClass.getSuperclass();
    final OClass cls;
    if (superClass != null && superClass != Object.class && existsClass(superClass.getSimpleName()))
      cls = getClass(superClass.getSimpleName());
    else
      cls = null;

    return createClass(iClass.getSimpleName(), cls, -1);
  }

  @Override
  public OClass createAbstractClass(final String iClassName) {
    return createClass(iClassName, null, -1);
  }

  @Override
  public OClass createAbstractClass(final String iClassName, final OClass iSuperClass) {
    return createClass(iClassName, iSuperClass, -1);
  }

  private int createCluster(String iType, String iClassName) {
    return getDatabase().command(new OCommandSQL("create cluster " + iClassName + " " + iType)).<Integer> execute();
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_CREATE);

    final String key = iClassName.toLowerCase();

    return getDatabase().getStorage().callInLock(new Callable<OClass>() {
      @Override
      public OClass call() throws Exception {
        if (classes.containsKey(key))
          throw new OSchemaException("Class " + iClassName + " already exists in current database");

        final StringBuilder cmd = new StringBuilder("create class ");
        cmd.append(iClassName);

        if (iSuperClass != null) {
          cmd.append(" extends ");
          cmd.append(iSuperClass.getName());
        }

        if (iClusterIds != null) {
          if (iClusterIds.length == 1 && iClusterIds[0] == -1)
            cmd.append(" abstract");
          else {
            cmd.append(" cluster ");
            for (int i = 0; i < iClusterIds.length; ++i) {
              if (i > 0)
                cmd.append(',');
              else
                cmd.append(' ');

              cmd.append(iClusterIds[i]);
            }
          }
        }

        getDatabase().command(new OCommandSQL(cmd.toString())).execute();
        if (!(getDatabase().getStorage() instanceof OStorageEmbedded))
          getDatabase().reload();

        if (classes.containsKey(key))
          return classes.get(key);
        else
          // ADD IT LOCALLY AVOIDING TO RELOAD THE ENTIRE SCHEMA
          createClassInternal(iClassName, iSuperClass, iClusterIds);

        return classes.get(key);
      }
    }, true);
  }

  public OClass createClassInternal(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
    if (iClassName == null || iClassName.length() == 0)
      throw new OSchemaException("Found class name null");

    final Character wrongCharacter = checkNameIfValid(iClassName);
    if (wrongCharacter != null)
      throw new OSchemaException("Found invalid class name. Character '" + wrongCharacter + "' cannot be used in class name.");

    final ODatabaseRecord database = getDatabase();

    final int[] clusterIds;
    if (iClusterIds == null || iClusterIds.length == 0)
      // CREATE A NEW CLUSTER
      clusterIds = new int[] { database.addCluster(CLUSTER_TYPE.PHYSICAL.toString(), iClassName, null, null) };
    else
      clusterIds = iClusterIds;

    database.checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_CREATE);

    final String key = iClassName.toLowerCase();

    final OSchemaShared me = this;
    return getDatabase().getStorage().callInLock(new Callable<OClass>() {
      @Override
      public OClass call() throws Exception {
        if (classes.containsKey(key))
          throw new OSchemaException("Class " + iClassName + " already exists in current database");

        final OClassImpl cls = new OClassImpl(me, iClassName, clusterIds);
        classes.put(key, cls);

        if (cls.getShortName() != null)
          // BIND SHORT NAME TOO
          classes.put(cls.getShortName().toLowerCase(), cls);

        if (iSuperClass != null) {
          cls.setSuperClassInternal(iSuperClass);

          // UPDATE INDEXES
          final int[] clustersToIndex = iSuperClass.getPolymorphicClusterIds();
          final String[] clusterNames = new String[clustersToIndex.length];
          for (int i = 0; i < clustersToIndex.length; i++)
            clusterNames[i] = database.getClusterNameById(clustersToIndex[i]);

          for (OIndex<?> index : iSuperClass.getIndexes())
            for (String clusterName : clusterNames)
              if (clusterName != null)
                index.getInternal().addCluster(clusterName);
        }

        return cls;
      }
    }, true);
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
      if (c == ':' || c == ',' || c == ' ')
        // INVALID CHARACTER
        return c;
    }

    // for (char c : iName.toCharArray())
    // if (!Character.isJavaIdentifierPart(c))
    // return c;
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.metadata.schema.OSchema#dropClass(java.lang.String)
   */
  public void dropClass(final String iClassName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a class inside a transaction");

    if (iClassName == null)
      throw new IllegalArgumentException("Class name is null");

    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

    final String key = iClassName.toLowerCase();

    getDatabase().getStorage().callInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OClass cls = classes.get(key);
        if (cls == null)
          throw new OSchemaException("Class " + iClassName + " was not found in current database");

        if (cls.getBaseClasses().hasNext())
          throw new OSchemaException("Class " + iClassName
              + " cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");

        final StringBuilder cmd = new StringBuilder("drop class ");
        cmd.append(iClassName);

        Object result = getDatabase().command(new OCommandSQL(cmd.toString())).execute();
        if (result instanceof Boolean && (Boolean) result) {
          classes.remove(key);
        }
        getDatabase().reload();
        reload();
        return null;
      }
    }, true);
  }

  public void dropClassInternal(final String iClassName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a class inside a transaction");

    if (iClassName == null)
      throw new IllegalArgumentException("Class name is null");

    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

    final String key = iClassName.toLowerCase();

    getDatabase().getStorage().callInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {

        final OClass cls = classes.get(key);
        if (cls == null)
          throw new OSchemaException("Class " + iClassName + " was not found in current database");

        if (cls.getBaseClasses().hasNext())
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
        return null;
      }
    }, true);
  }

  private void dropClassIndexes(final OClass cls) {
    for (final OIndex<?> index : getDatabase().getMetadata().getIndexManager().getClassIndexes(cls.getName())) {
      getDatabase().command(new OCommandSQL(DROP_INDEX_QUERY + index.getName()));
    }
  }

  /**
   * Reloads the schema inside a storage's shared lock.
   */
  @Override
  public <RET extends ODocumentWrapper> RET reload() {
    getDatabase().getStorage().callInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        reload(null);
        return null;
      }
    }, true);

    return (RET) this;
  }

  public boolean existsClass(final String iClassName) {
    return getDatabase().getStorage().callInLock(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return classes.containsKey(iClassName.toLowerCase());
      }
    }, false);
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
    if (iClassName == null)
      return null;

    OClass cls;

    cls = getDatabase().getStorage().callInLock(new Callable<OClass>() {
      @Override
      public OClass call() throws Exception {
        return classes.get(iClassName.toLowerCase());
      }
    }, false);

    if (cls == null && getDatabase().getDatabaseOwner() instanceof ODatabaseObject) {
      cls = getDatabase().getStorage().callInLock(new Callable<OClass>() {
        @Override
        public OClass call() throws Exception {
          OClass cls = classes.get(iClassName.toLowerCase());
          if (cls == null) {
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
          return cls;
        }
      }, true);
    }
    return cls;
  }

  public void changeClassName(final String iOldName, final String iNewName) {
    getDatabase().getStorage().callInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OClass clazz = classes.remove(iOldName.toLowerCase());
        classes.put(iNewName.toLowerCase(), clazz);
        return null;
      }
    }, true);
  }

  /**
   * Binds ODocument to POJO.
   */
  @Override
  public void fromStream() {
    final OSchemaShared me = this;
    getDatabase().getStorage().callInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        // READ CURRENT SCHEMA VERSION
        final Integer schemaVersion = (Integer) document.field("schemaVersion");
        if (schemaVersion == null) {
          OLogManager
              .instance()
              .error(
                  this,
                  "Database's schema is empty! Recreating the system classes and allow the opening of the database but double check the integrity of the database");
          return null;
        } else if (schemaVersion.intValue() != CURRENT_VERSION_NUMBER) {
          // HANDLE SCHEMA UPGRADE
          throw new OConfigurationException(
              "Database schema is different. Please export your old database with the previous version of OrientDB and reimport it using the current one.");
        }

        // REGISTER ALL THE CLASSES
        classes.clear();
        OClassImpl cls;
        Collection<ODocument> storedClasses = document.field("classes");
        for (ODocument c : storedClasses) {
          cls = new OClassImpl(me, c);
          cls.fromStream();
          classes.put(cls.getName().toLowerCase(), cls);

          if (cls.getShortName() != null)
            classes.put(cls.getShortName().toLowerCase(), cls);
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
        return null;
      }
    }, true);
  }

  /**
   * Binds POJO to ODocument.
   */
  @Override
  @OBeforeSerialization
  public ODocument toStream() {
    return getDatabase().getStorage().callInLock(new Callable<ODocument>() {
      @Override
      public ODocument call() throws Exception {

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
      }
    }, false);
  }

  public Collection<OClass> getClasses() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

    return getDatabase().getStorage().callInLock(new Callable<Collection<OClass>>() {
      @Override
      public HashSet<OClass> call() throws Exception {
        return new HashSet<OClass>(classes.values());
      }
    }, false);
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(final String iClusterName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

    return getDatabase().getStorage().callInLock(new Callable<Set<OClass>>() {
      @Override
      public Set<OClass> call() throws Exception {
        final int clusterId = getDatabase().getClusterIdByName(iClusterName);
        final Set<OClass> result = new HashSet<OClass>();
        for (OClass c : classes.values()) {
          if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId))
            result.add(c);
        }

        return result;
      }
    }, false);
  }

  @Override
  public OSchemaShared load() {
    getDatabase().getStorage().callInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        getDatabase();
        ((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().schemaRecordId);
        reload("*:-1 index:0");
        return null;
      }
    }, true);

    return this;
  }

  public void create() {
    final ODatabaseRecord db = getDatabase();
    super.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
    db.getStorage().getConfiguration().schemaRecordId = document.getIdentity().toString();
    db.getStorage().getConfiguration().update();
  }

  public void close() {
    classes.clear();
    document.clear();
  }

  public void saveInternal() {
    final ODatabaseRecord db = getDatabase();

    if (db.getTransaction().isActive())
      throw new OSchemaException("Cannot change the schema while a transaction is active. Schema changes are not transactional");

    db.getStorage().callInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        saveInternal(OMetadataDefault.CLUSTER_INTERNAL_NAME);
        return null;
      }
    }, true);
  }

  @Deprecated
  public int getVersion() {
    return getDatabase().getStorage().callInLock(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return document.getRecordVersion().getCounter();
      }
    }, false);
  }

  public ORID getIdentity() {
    return document.getIdentity();
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
    document.setDirty();
    return this;
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
