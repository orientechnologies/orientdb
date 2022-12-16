package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;

/** Created by tglman on 14/06/17. */
public class OViewRemote extends OViewImpl {

  protected OViewRemote(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  protected OProperty addProperty(
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
      final StringBuilder cmd = new StringBuilder("create property ");
      // CLASS.PROPERTY NAME
      cmd.append('`');
      cmd.append(name);
      cmd.append('`');
      cmd.append('.');
      cmd.append('`');
      cmd.append(propertyName);
      cmd.append('`');

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
        cmd.append('`');
        cmd.append(linkedClass.getName());
        cmd.append('`');
      }

      if (unsafe) cmd.append(" unsafe ");

      database.command(cmd.toString()).close();

      return getProperty(propertyName);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClassImpl setEncryption(final String iValue) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final String cmd = String.format("alter view `%s` encryption %s", name, iValue);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass setClusterSelection(final String value) {
    throw new UnsupportedOperationException();
  }

  public OClassImpl setCustom(final String name, final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final String cmd = String.format("alter view `%s` custom %s = ?", getName(), name);
      database.command(cmd, value).close();
      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public void clearCustom() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final String cmd = String.format("alter view `%s` custom clear", getName());
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass setSuperClasses(final List<? extends OClass> classes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass addSuperClass(final OClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass removeSuperClass(OClass superClass) {
    throw new UnsupportedOperationException();
  }

  public OView setName(final String name) {
    if (getName().equals(name)) return this;
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OView oClass = database.getMetadata().getSchema().getView(name);
    if (oClass != null) {
      String error =
          String.format(
              "Cannot rename view %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in view name '"
              + name
              + "'");
    acquireSchemaWriteLock();
    try {

      final String cmd = String.format("alter view `%s` name `%s`", this.name, name);
      database.command(cmd);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OView setShortName(String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty()) shortName = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final String cmd = String.format("alter view `%s` shortname `%s`", name, shortName);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected OPropertyImpl createPropertyInstance(ODocument p) {
    return new OPropertyRemote(this, p);
  }

  /** {@inheritDoc} */
  @Override
  public OView truncateCluster(String clusterName) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, name);
    acquireSchemaReadLock();
    try {

      final String cmd = String.format("truncate cluster %s", clusterName);
      database.command(cmd).close();
    } finally {
      releaseSchemaReadLock();
    }

    return this;
  }

  public OView setStrictMode(final boolean isStrict) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final String cmd = String.format("alter view `%s` strictmode %s", name, isStrict);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OView setDescription(String iDescription) {
    if (iDescription != null) {
      iDescription = iDescription.trim();
      if (iDescription.isEmpty()) iDescription = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final String cmd = String.format("alter view `%s` description ?", name);
      database.command(cmd, iDescription).close();
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OView addClusterId(final int clusterId) {
    throw new UnsupportedOperationException();
  }

  public OView removeClusterId(final int clusterId) {
    throw new UnsupportedOperationException();
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

      database.command("drop property " + name + '.' + propertyName).close();

    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OView addCluster(final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public OView setOverSize(final float overSize) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      // FORMAT FLOAT LOCALE AGNOSTIC
      final String cmd =
          String.format("alter view `%s` oversize %s", name, new Float(overSize).toString());
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OView setAbstract(boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  protected OView removeBaseClassInternal(final OClass baseClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(final List<? extends OClass> classes) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(final int defaultClusterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OViewRemovedMetadata replaceViewClusterAndIndex(int cluster, List<OIndex> indexes) {
    throw new UnsupportedOperationException();
  }
}
