package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;

/**
 * Created by tglman on 14/06/17.
 */
public class OPropertyEmbedded extends OPropertyImpl {
  protected OPropertyEmbedded(OClassImpl owner) {
    super(owner);
  }

  protected OPropertyEmbedded(OClassImpl owner, ODocument document) {
    super(owner, document);
  }

  protected OPropertyEmbedded(OClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }

  public OPropertyImpl setType(final OType type) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    final ODatabaseDocumentInternal database = getDatabase();
    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();

      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s type %s", getFullNameQuoted(), quoteString(type.toString()));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setTypeInternal(type);
      } else
        setTypeInternal(type);
    } finally {
      releaseSchemaWriteLock();
    }
    owner.fireDatabaseMigration(database, globalRef.getName(), globalRef.getType());

    return this;
  }

  public OProperty setName(final String name) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s name %s", getFullNameQuoted(), quoteString(name));
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

  @Override
  public OPropertyImpl setDescription(final String iDescription) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s description %s", getFullNameQuoted(), quoteString(iDescription));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(new OCommandSQL(cmd)).execute();

        setDescriptionInternal(iDescription);
      } else
        setDescriptionInternal(iDescription);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OProperty setCollate(String collate) {
    if (collate == null)
      collate = ODefaultCollate.NAME;

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s collate %s", getFullNameQuoted(), quoteString(collate));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
        database.command(commandSQL).execute();

        setCollateInternal(collate);
      } else
        setCollateInternal(collate);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public void clearCustom() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      final String cmd = String.format("alter property %s custom clear", getFullNameQuoted());
      if (isDistributedCommand()) {
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
        database.command(commandSQL).execute();

        clearCustomInternal();
      } else
        clearCustomInternal();

    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OPropertyImpl setCustom(final String name, final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final String cmd = String.format("alter property %s custom %s=%s", getFullNameQuoted(), name, quoteString(value));

      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setCustomInternal(name, value);
      } else
        setCustomInternal(name, value);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OPropertyImpl setRegexp(final String regexp) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s regexp %s", getFullNameQuoted(), quoteString(regexp));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setRegexpInternal(regexp);
      } else
        setRegexpInternal(regexp);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OPropertyImpl setLinkedClass(final OClass linkedClass) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    checkSupportLinkedClass(getType());

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (isDistributedCommand()) {
        final String cmd = String
            .format("alter property %s linkedclass %s", getFullNameQuoted(), quoteString(linkedClass.getName()));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setLinkedClassInternal(linkedClass);
      } else
        setLinkedClassInternal(linkedClass);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OProperty setLinkedType(final OType linkedType) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    checkLinkTypeSupport(getType());

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String
            .format("alter property %s linkedtype %s", getFullNameQuoted(), quoteString(linkedType.toString()));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setLinkedTypeInternal(linkedType);
      } else
        setLinkedTypeInternal(linkedType);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OPropertyImpl setNotNull(final boolean isNotNull) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s notnull %s", getFullNameQuoted(), isNotNull);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setNotNullInternal(isNotNull);
      } else
        setNotNullInternal(isNotNull);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OPropertyImpl setDefaultValue(final String defaultValue) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s default %s", getFullNameQuoted(), quoteString(defaultValue));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);

        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setDefaultValueInternal(defaultValue);
      } else {
        setDefaultValueInternal(defaultValue);
      }
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OPropertyImpl setMax(final String max) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s max %s", getFullNameQuoted(), quoteString(max));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);

        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setMaxInternal(max);
      } else
        setMaxInternal(max);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OPropertyImpl setMin(final String min) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s min %s", getFullNameQuoted(), quoteString(min));
        final OCommandSQL commandSQL = new OCommandSQL(cmd);

        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setMinInternal(min);
      } else
        setMinInternal(min);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OPropertyImpl setReadonly(final boolean isReadonly) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s readonly %s", getFullNameQuoted(), isReadonly);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setReadonlyInternal(isReadonly);
      } else
        setReadonlyInternal(isReadonly);

    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  public OPropertyImpl setMandatory(final boolean isMandatory) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();
      if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s mandatory %s", getFullNameQuoted(), isMandatory);
        final OCommandSQL commandSQL = new OCommandSQL(cmd);
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());

        database.command(commandSQL).execute();

        setMandatoryInternal(isMandatory);
      } else
        setMandatoryInternal(isMandatory);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

}
