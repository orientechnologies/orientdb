package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 14/06/17.
 */
public class OClassRemote extends OClassImpl {
  protected OClassRemote(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected OClassRemote(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  protected OClassRemote(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  protected OProperty addProperty(final String propertyName, final OType type, final OType linkedType, final OClass linkedClass,
      final boolean unsafe) {
    if (type == null)
      throw new OSchemaException("Property type not defined.");

    if (propertyName == null || propertyName.length() == 0)
      throw new OSchemaException("Property name is null or empty");

    final ODatabaseDocumentInternal database = getDatabase();
    validatePropertyName(propertyName);
    if (database.getTransaction().isActive()) {
      throw new OSchemaException("Cannot create property '" + propertyName + "' inside a transaction");
    }

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (linkedType != null)
      OPropertyImpl.checkLinkTypeSupport(type);

    if (linkedClass != null)
      OPropertyImpl.checkSupportLinkedClass(type);

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

      if (unsafe)
        cmd.append(" unsafe ");

      database.command(cmd.toString());
      reload();

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
      final String cmd = String.format("alter class `%s` encryption %s", name, iValue);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass setClusterSelection(final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final String cmd = String.format("alter class `%s` clusterselection '%s'", name, value);
      database.command(cmd);
      return this;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OClassImpl setCustom(final String name, final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final String cmd = String.format("alter class `%s` custom %s=%s", getName(), name, value);
      database.command(cmd);
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
      final String cmd = String.format("alter class `%s` custom clear", getName());
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  @Override
  public OClass setSuperClasses(final List<? extends OClass> classes) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    if (classes != null) {
      List<OClass> toCheck = new ArrayList<OClass>(classes);
      toCheck.add(this);
      checkParametersConflict(toCheck);
    }
    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final StringBuilder sb = new StringBuilder();
      if (classes != null && classes.size() > 0) {
        for (OClass superClass : classes) {
          sb.append('`').append(superClass.getName()).append("`,");
        }
        sb.deleteCharAt(sb.length() - 1);
      } else
        sb.append("null");

      final String cmd = String.format("alter class `%s` superclasses %s", name, sb);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass addSuperClass(final OClass superClass) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    checkParametersConflict(superClass);
    acquireSchemaWriteLock();
    try {

      final String cmd = String.format("alter class `%s` superclass +`%s`", name, superClass != null ? superClass.getName() : null);
      database.command(cmd);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  @Override
  public OClass removeSuperClass(OClass superClass) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      final String cmd = String.format("alter class `%s` superclass -`%s`", name, superClass != null ? superClass.getName() : null);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OClass setName(final String name) {
    if (getName().equals(name))
      return this;
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    OClass oClass = database.getMetadata().getSchema().getClass(name);
    if (oClass != null) {
      String error = String.format("Cannot rename class %s to %s. A Class with name %s exists", this.name, name, name);
      throw new OSchemaException(error);
    }
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '" + name + "'");
    acquireSchemaWriteLock();
    try {

      final String cmd = String.format("alter class `%s` name `%s`", this.name, name);
      database.command(cmd);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OClass setShortName(String shortName) {
    if (shortName != null) {
      shortName = shortName.trim();
      if (shortName.isEmpty())
        shortName = null;
    }
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final String cmd = String.format("alter class `%s` shortname `%s`", name, shortName);
      database.command(cmd);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  protected OPropertyImpl createPropertyInstance(ODocument p) {
    return new OPropertyRemote(this, p);
  }

}
