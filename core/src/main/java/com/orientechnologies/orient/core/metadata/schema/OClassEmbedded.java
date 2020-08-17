package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

/** Created by tglman on 14/06/17. */
public class OClassEmbedded extends OClassImpl {
  protected OClassEmbedded(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected OClassEmbedded(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  protected OClassEmbedded(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  public OProperty addProperty(
      final String propertyName,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    if (type == null) throw new OSchemaException("Property type not defined.");

    if (propertyName == null || propertyName.length() == 0)
      throw new OSchemaException("Property name is null or empty");

    final ODatabaseDocumentInternal database = getDatabase();
    validatePropertyName(propertyName);
    if (database.getTransaction().isActive()) {
      throw new OSchemaException(
          "Cannot create property '" + propertyName + "' inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null) OPropertyImpl.checkLinkTypeSupport(type);

    if (linkedClass != null) OPropertyImpl.checkSupportLinkedClass(type);

    acquireSchemaWriteLock();
    try {
      return (OProperty)
          OScenarioThreadLocal.executeAsDistributed(
              new Callable<OProperty>() {
                @Override
                public OProperty call() throws Exception {
                  return addPropertyInternal(propertyName, type, linkedType, linkedClass, unsafe);
                }
              });

    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClassImpl setEncryption(final String iValue) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      setEncryptionInternal(database, iValue);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass setClusterSelection(final String value) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      setClusterSelectionInternal(value);
      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public void setClusterSelectionInternal(final String clusterSelection) {
    // AVOID TO CHECK THIS IN LOCK TO AVOID RE-GENERATION OF IMMUTABLE SCHEMAS
    if (this.clusterSelection.getName().equals(clusterSelection))
      // NO CHANGES
      return;

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.clusterSelection = owner.getClusterSelectionFactory().newInstance(clusterSelection);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClassImpl setCustom(final String name, final String value) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      setCustomInternal(name, value);
      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public void clearCustom() {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      clearCustomInternal();

    } finally {
      releaseSchemaWriteLock();
    }
  }

  protected void clearCustomInternal() {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      customFields = null;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass setSuperClasses(final List<? extends OClass> classes) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    if (classes != null) {
      List<OClass> toCheck = new ArrayList<OClass>(classes);
      toCheck.add(this);
      checkParametersConflict(toCheck);
    }
    acquireSchemaWriteLock();
    try {
      setSuperClassesInternal(classes);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
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
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    checkParametersConflict(superClass);
    acquireSchemaWriteLock();
    try {
      addSuperClassInternal(database, superClass);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  protected void addSuperClassInternal(
      ODatabaseDocumentInternal database, final OClass superClass) {
    acquireSchemaWriteLock();
    try {
      final OClassImpl cls;

      if (superClass instanceof OClassAbstractDelegate)
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      else cls = (OClassImpl) superClass;

      if (cls != null) {

        // CHECK THE USER HAS UPDATE PRIVILEGE AGAINST EXTENDING CLASS
        final OSecurityUser user = database.getUser();
        if (user != null)
          user.allow(ORule.ResourceGeneric.CLASS, cls.getName(), ORole.PERMISSION_UPDATE);

        if (superClasses.contains(superClass)) {
          throw new OSchemaException(
              "Class: '"
                  + this.getName()
                  + "' already has the class '"
                  + superClass.getName()
                  + "' as superclass");
        }

        cls.addBaseClass(this);
        superClasses.add(cls);
      }
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass removeSuperClass(OClass superClass) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      removeSuperClassInternal(superClass);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  protected void removeSuperClassInternal(final OClass superClass) {
    acquireSchemaWriteLock();
    try {
      final OClassImpl cls;

      if (superClass instanceof OClassAbstractDelegate)
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      else cls = (OClassImpl) superClass;

      if (superClasses.contains(cls)) {
        if (cls != null) cls.removeBaseClassInternal(this);

        superClasses.remove(superClass);
      }
    } finally {
      releaseSchemaWriteLock();
    }
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
    if (getName().equals(name)) return this;
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + name
              + "'");
    acquireSchemaWriteLock();
    try {
      setNameInternal(database, name);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected void setNameInternal(ODatabaseDocumentInternal database, final String name) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();
      final String oldName = this.name;
      owner.changeClassName(database, this.name, name, this);
      this.name = name;
      renameCluster(oldName, this.name);
    } finally {
      releaseSchemaWriteLock();
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

      ((OSchemaEmbedded) owner).addClusterForClass(database, clusterId, this);
      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClass setShortName(String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) shortName = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      setShortNameInternal(database, shortName);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected void setShortNameInternal(ODatabaseDocumentInternal database, final String iShortName) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      String oldName = null;

      if (this.shortName != null) oldName = this.shortName;

      owner.changeClassName(database, oldName, iShortName, this);

      this.shortName = iShortName;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  protected OPropertyImpl createPropertyInstance(ODocument p) {
    return new OPropertyEmbedded(this, p);
  }

  public OPropertyImpl addPropertyInternal(
      final String name,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    if (name == null || name.length() == 0) throw new OSchemaException("Found property name null");

    if (!unsafe) checkPersistentPropertyType(getDatabase(), name, type, linkedClass);

    final OPropertyEmbedded prop;

    // This check are doubled because used by sql commands
    if (linkedType != null) OPropertyImpl.checkLinkTypeSupport(type);

    if (linkedClass != null) OPropertyImpl.checkSupportLinkedClass(type);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (properties.containsKey(name))
        throw new OSchemaException("Class '" + this.name + "' already has property '" + name + "'");

      OGlobalProperty global = owner.findOrCreateGlobalProperty(name, type);

      prop = createPropertyInstance(global);

      properties.put(name, prop);

      if (linkedType != null) prop.setLinkedTypeInternal(linkedType);
      else if (linkedClass != null) prop.setLinkedClassInternal(linkedClass);
    } finally {
      releaseSchemaWriteLock();
    }

    if (prop != null && !unsafe) fireDatabaseMigration(getDatabase(), name, type);

    return prop;
  }

  protected OPropertyEmbedded createPropertyInstance(OGlobalProperty global) {
    return new OPropertyEmbedded(this, global);
  }

  /** {@inheritDoc} */
  @Override
  public OClass truncateCluster(String clusterName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);

    acquireSchemaReadLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      truncateClusterInternal(clusterName, database);
    } finally {
      releaseSchemaReadLock();
    }

    return this;
  }

  public OClass setStrictMode(final boolean isStrict) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      setStrictModeInternal(isStrict);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected void setStrictModeInternal(final boolean iStrict) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.strictMode = iStrict;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClass setDescription(String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) iDescription = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      setDescriptionInternal(iDescription);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected void setDescriptionInternal(final String iDescription) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();
      this.description = iDescription;
    } finally {
      releaseSchemaWriteLock();
    }
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
    return removeClusterId(clusterId, false);
  }

  public OClass removeClusterId(final int clusterId, boolean force) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (!force && clusterIds.length == 1 && clusterId == clusterIds[0])
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

  protected OClass removeClusterIdInternal(
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

      ((OSchemaEmbedded) owner).removeClusterForClass(database, clusterToRemove, this);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public void dropProperty(final String propertyName) {
    final ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock();
    try {
      if (!properties.containsKey(propertyName))
        throw new OSchemaException(
            "Property '" + propertyName + "' not found in class " + name + "'");

      OScenarioThreadLocal.executeAsDistributed(
          (Callable<OProperty>)
              () -> {
                dropPropertyInternal(database, propertyName);
                return null;
              });

    } finally {
      releaseSchemaWriteLock();
    }
  }

  protected void dropPropertyInternal(
      ODatabaseDocumentInternal database, final String iPropertyName) {
    if (database.getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      final OProperty prop = properties.remove(iPropertyName);

      if (prop == null)
        throw new OSchemaException(
            "Property '" + iPropertyName + "' not found in class " + name + "'");
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass addCluster(final String clusterNameOrId) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock();
    try {
      final int clusterId = owner.createClusterIfNeeded(database, clusterNameOrId);
      addClusterIdInternal(database, clusterId);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setOverSize(final float overSize) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      setOverSizeInternal(database, overSize);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected void setOverSizeInternal(ODatabaseDocumentInternal database, final float overSize) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.overSize = overSize;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClass setAbstract(boolean isAbstract) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      setAbstractInternal(database, isAbstract);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected void setCustomInternal(final String name, final String value) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (customFields == null) customFields = new HashMap<String, String>();
      if (value == null || "null".equalsIgnoreCase(value)) customFields.remove(name);
      else customFields.put(name, value);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  protected void setAbstractInternal(ODatabaseDocumentInternal database, final boolean isAbstract) {
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      if (isAbstract) {
        // SWITCH TO ABSTRACT
        if (defaultClusterId != NOT_EXISTENT_CLUSTER_ID) {
          // CHECK
          if (count() > 0)
            throw new IllegalStateException(
                "Cannot set the class as abstract because contains records.");

          tryDropCluster(defaultClusterId);
          for (int clusterId : getClusterIds()) {
            tryDropCluster(clusterId);
            removePolymorphicClusterId(clusterId);
            ((OSchemaEmbedded) owner).removeClusterForClass(database, clusterId, this);
          }

          setClusterIds(new int[] {NOT_EXISTENT_CLUSTER_ID});

          defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
        }
      } else {
        if (!abstractClass) return;

        int clusterId = database.getClusterIdByName(name);
        if (clusterId == -1) clusterId = database.addCluster(name);

        this.defaultClusterId = clusterId;
        this.clusterIds[0] = this.defaultClusterId;
        this.polymorphicClusterIds = Arrays.copyOf(clusterIds, clusterIds.length);
        for (OClass clazz : getAllSubclasses()) {
          if (clazz instanceof OClassImpl) {
            addPolymorphicClusterIds((OClassImpl) clazz);
          } else {
            OLogManager.instance()
                .warn(this, "Warning: cannot set polymorphic cluster IDs for class " + name);
          }
        }
      }

      this.abstractClass = isAbstract;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void tryDropCluster(final int clusterId) {
    if (name.toLowerCase(Locale.ENGLISH).equals(getDatabase().getClusterNameById(clusterId))) {
      // DROP THE DEFAULT CLUSTER CALLED WITH THE SAME NAME ONLY IF EMPTY
      if (getDatabase().countClusterElements(clusterId) == 0) {
        getDatabase().dropClusterInternal(clusterId);
      }
    }
  }
}
