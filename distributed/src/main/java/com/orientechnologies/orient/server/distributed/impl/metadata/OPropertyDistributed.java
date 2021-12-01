package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;

/** Created by tglman on 22/06/17. */
public class OPropertyDistributed extends OPropertyEmbedded {
  public OPropertyDistributed(OClassImpl owner) {
    super(owner);
  }

  public OPropertyDistributed(OClassImpl owner, ODocument document) {
    super(owner, document);
  }

  public OPropertyDistributed(OClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }

  public OPropertyImpl setType(final OType type) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    final ODatabaseDocumentInternal database = getDatabase();
    acquireSchemaWriteLock();
    try {

      if (isDistributedCommand()) {
        final String cmd =
            String.format(
                "alter property %s type %s", getFullNameQuoted(), quoteString(type.toString()));
        owner.getOwner().sendCommand(database, cmd);
      } else setTypeInternal(type);
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

      if (isDistributedCommand()) {
        final String cmd =
            String.format("alter property %s name %s", getFullNameQuoted(), quoteString(name));
        owner.getOwner().sendCommand(database, cmd);
      } else setNameInternal(name);

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
      if (isDistributedCommand()) {
        final String cmd =
            String.format(
                "alter property %s description %s", getFullNameQuoted(), quoteString(iDescription));
        owner.getOwner().sendCommand(database, cmd);
      } else setDescriptionInternal(iDescription);

    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OProperty setCollate(String collate) {
    if (collate == null) collate = ODefaultCollate.NAME;

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();

      if (isDistributedCommand()) {
        final String cmd =
            String.format(
                "alter property %s collate %s", getFullNameQuoted(), quoteString(collate));
        owner.getOwner().sendCommand(database, cmd);
      } else setCollateInternal(collate);

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

      final String cmd = String.format("alter property %s custom clear", getFullNameQuoted());
      if (isDistributedCommand()) {
        owner.getOwner().sendCommand(database, cmd);
      } else clearCustomInternal();

    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OPropertyImpl setCustom(final String name, final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final String cmd =
          String.format(
              "alter property %s custom `%s`=%s", getFullNameQuoted(), name, quoteString(value));

      final ODatabaseDocumentInternal database = getDatabase();
      if (isDistributedCommand()) {
        owner.getOwner().sendCommand(database, cmd);
      } else setCustomInternal(name, value);

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

      if (isDistributedCommand()) {
        final String cmd =
            String.format("alter property %s regexp %s", getFullNameQuoted(), quoteString(regexp));
        owner.getOwner().sendCommand(database, cmd);
      } else setRegexpInternal(regexp);

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

      if (isDistributedCommand()) {
        final String cmd =
            String.format(
                "alter property %s linkedclass %s",
                getFullNameQuoted(), quoteString(linkedClass.getName()));
        owner.getOwner().sendCommand(database, cmd);
      } else setLinkedClassInternal(linkedClass);

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
      if (isDistributedCommand()) {
        final String cmd =
            String.format(
                "alter property %s linkedtype %s",
                getFullNameQuoted(), quoteString(linkedType.toString()));
        owner.getOwner().sendCommand(database, cmd);
      } else setLinkedTypeInternal(linkedType);

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
      if (isDistributedCommand()) {
        final String cmd =
            String.format("alter property %s notnull %s", getFullNameQuoted(), isNotNull);
        owner.getOwner().sendCommand(database, cmd);
      } else setNotNullInternal(isNotNull);

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
      if (isDistributedCommand()) {
        final String cmd =
            String.format(
                "alter property %s default %s", getFullNameQuoted(), quoteString(defaultValue));
        owner.getOwner().sendCommand(database, cmd);
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
      if (isDistributedCommand()) {
        final String cmd =
            String.format("alter property %s max %s", getFullNameQuoted(), quoteString(max));
        owner.getOwner().sendCommand(database, cmd);
      } else setMaxInternal(max);
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
      if (isDistributedCommand()) {
        final String cmd =
            String.format("alter property %s min %s", getFullNameQuoted(), quoteString(min));
        owner.getOwner().sendCommand(database, cmd);
      } else setMinInternal(min);
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
      if (isDistributedCommand()) {
        final String cmd =
            String.format("alter property %s readonly %s", getFullNameQuoted(), isReadonly);
        owner.getOwner().sendCommand(database, cmd);
      } else setReadonlyInternal(isReadonly);

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
      if (isDistributedCommand()) {
        final String cmd =
            String.format("alter property %s mandatory %s", getFullNameQuoted(), isMandatory);
        owner.getOwner().sendCommand(database, cmd);
      } else setMandatoryInternal(isMandatory);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }
}
