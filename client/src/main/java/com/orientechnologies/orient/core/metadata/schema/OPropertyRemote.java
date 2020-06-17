package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;

/** Created by tglman on 14/06/17. */
public class OPropertyRemote extends OPropertyImpl {
  OPropertyRemote(OClassImpl owner) {
    super(owner);
  }

  OPropertyRemote(OClassImpl owner, ODocument document) {
    super(owner, document);
  }

  public OPropertyRemote(OClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }

  public OPropertyImpl setType(final OType type) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    final ODatabaseDocumentInternal database = getDatabase();
    acquireSchemaWriteLock();
    try {
      final String cmd =
          String.format(
              "alter property %s type %s", getFullNameQuoted(), quoteString(type.toString()));
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock();
    }
    return this;
  }

  public OProperty setName(final String name) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final String cmd =
          String.format("alter property %s name %s", getFullNameQuoted(), quoteString(name));
      database.command(cmd).close();

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
      final String cmd =
          String.format(
              "alter property %s description %s", getFullNameQuoted(), quoteString(iDescription));
      database.command(cmd).close();

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
      final String cmd =
          String.format("alter property %s collate %s", getFullNameQuoted(), quoteString(collate));
      database.command(cmd).close();
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
      database.command(cmd).close();
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
              "alter property %s custom %s=%s", getFullNameQuoted(), name, quoteString(value));
      final ODatabaseDocumentInternal database = getDatabase();
      database.command(cmd).close();
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
      final String cmd =
          String.format("alter property %s regexp %s", getFullNameQuoted(), quoteString(regexp));
      database.command(cmd).close();
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
      final String cmd =
          String.format(
              "alter property %s linkedclass %s",
              getFullNameQuoted(), quoteString(linkedClass.getName()));
      database.command(cmd).close();

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
      final String cmd =
          String.format(
              "alter property %s linkedtype %s",
              getFullNameQuoted(), quoteString(linkedType.toString()));
      database.command(cmd).close();

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
      final String cmd =
          String.format("alter property %s notnull %s", getFullNameQuoted(), isNotNull);
      database.command(cmd).close();

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
      final String cmd =
          String.format(
              "alter property %s default %s", getFullNameQuoted(), quoteString(defaultValue));
      database.command(cmd).close();

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
      final String cmd =
          String.format("alter property %s max %s", getFullNameQuoted(), quoteString(max));
      database.command(cmd).close();
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
      final String cmd =
          String.format("alter property %s min %s", getFullNameQuoted(), quoteString(min));
      database.command(cmd).close();
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
      final String cmd =
          String.format("alter property %s readonly %s", getFullNameQuoted(), isReadonly);
      database.command(cmd).close();

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
      final String cmd =
          String.format("alter property %s mandatory %s", getFullNameQuoted(), isMandatory);
      database.command(cmd).close();
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  @Override
  public OIndex createIndex(String iType) {
    return owner.createIndex(getFullName(), iType, globalRef.getName());
  }

  @Override
  public OIndex createIndex(OClass.INDEX_TYPE iType) {
    return createIndex(iType.toString());
  }

  @Override
  public OIndex createIndex(String iType, ODocument metadata) {
    return owner.createIndex(
        getFullName(), iType, null, metadata, new String[] {globalRef.getName()});
  }

  @Override
  public OIndex createIndex(OClass.INDEX_TYPE iType, ODocument metadata) {
    return createIndex(iType.name(), metadata);
  }

  @Override
  public OPropertyImpl dropIndexes() {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final ArrayList<OIndex> relatedIndexes = new ArrayList<OIndex>();
    for (final OIndex index : indexManager.getClassIndexes(database, owner.getName())) {
      final OIndexDefinition definition = index.getDefinition();

      if (OCollections.indexOf(
              definition.getFields(), globalRef.getName(), new OCaseInsentiveComparator())
          > -1) {
        if (definition instanceof OPropertyIndexDefinition) {
          relatedIndexes.add(index);
        } else {
          throw new IllegalArgumentException(
              "This operation applicable only for property indexes. "
                  + index.getName()
                  + " is "
                  + index.getDefinition());
        }
      }
    }

    for (final OIndex index : relatedIndexes)
      database.getMetadata().getIndexManagerInternal().dropIndex(database, index.getName());

    return this;
  }
}
