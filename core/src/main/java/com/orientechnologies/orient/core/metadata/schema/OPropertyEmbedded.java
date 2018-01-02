package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

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

  /**
   * Change the type. It checks for compatibility between the change of type.
   *
   * @param iType
   */
  protected void setTypeInternal(final OType iType) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      if (iType == globalRef.getType())
        // NO CHANGES
        return;

      if (!iType.getCastable().contains(globalRef.getType()))
        throw new IllegalArgumentException("Cannot change property type from " + globalRef.getType() + " to " + iType);

      this.globalRef = owner.owner.findOrCreateGlobalProperty(this.globalRef.getName(), iType);
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setNameInternal(final String name) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    String oldName = this.globalRef.getName();
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      owner.renameProperty(oldName, name);
      this.globalRef = owner.owner.findOrCreateGlobalProperty(name, this.globalRef.getType());
    } finally {
      releaseSchemaWriteLock();
    }
    owner.firePropertyNameMigration(getDatabase(), oldName, name, this.globalRef.getType());
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

  protected void setDescriptionInternal(final String iDescription) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.description = iDescription;
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected OProperty setCollateInternal(String iCollate) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      final OCollate oldCollate = this.collate;

      if (iCollate == null)
        iCollate = ODefaultCollate.NAME;

      collate = OSQLEngine.getCollate(iCollate);

      if ((this.collate != null && !this.collate.equals(oldCollate)) || (this.collate == null && oldCollate != null)) {
        final Set<OIndex<?>> indexes = owner.getClassIndexes();
        final List<OIndex<?>> indexesToRecreate = new ArrayList<OIndex<?>>();

        for (OIndex<?> index : indexes) {
          OIndexDefinition definition = index.getDefinition();

          final List<String> fields = definition.getFields();
          if (fields.contains(getName()))
            indexesToRecreate.add(index);
        }

        if (!indexesToRecreate.isEmpty()) {
          OLogManager.instance().info(this, "Collate value was changed, following indexes will be rebuilt %s", indexesToRecreate);

          final ODatabaseDocument database = getDatabase();
          final OIndexManager indexManager = database.getMetadata().getIndexManager();

          for (OIndex<?> indexToRecreate : indexesToRecreate) {
            final OIndexMetadata indexMetadata = indexToRecreate.getInternal().loadMetadata(indexToRecreate.getConfiguration());

            final ODocument metadata = indexToRecreate.getMetadata();
            final List<String> fields = indexMetadata.getIndexDefinition().getFields();
            final String[] fieldsToIndex = fields.toArray(new String[fields.size()]);

            indexManager.dropIndex(indexMetadata.getName());
            owner.createIndex(indexMetadata.getName(), indexMetadata.getType(), null, metadata, indexMetadata.getAlgorithm(),
                fieldsToIndex);
          }
        }
      }
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

  protected void clearCustomInternal() {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      customFields = null;
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

  protected void setCustomInternal(final String iName, final String iValue) {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      if (customFields == null)
        customFields = new HashMap<String, String>();
      if (iValue == null || "null".equalsIgnoreCase(iValue))
        customFields.remove(iName);
      else
        customFields.put(iName, iValue);
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setRegexpInternal(final String regexp) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      this.regexp = regexp;
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setLinkedClassInternal(final OClass iLinkedClass) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.linkedClass = iLinkedClass;

    } finally {
      releaseSchemaWriteLock();
    }

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

  protected void setLinkedTypeInternal(final OType iLinkedType) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();
      this.linkedType = iLinkedType;

    } finally {
      releaseSchemaWriteLock();
    }

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

  protected void setNotNullInternal(final boolean isNotNull) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      notNull = isNotNull;
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setDefaultValueInternal(final String defaultValue) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.defaultValue = defaultValue;
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setMaxInternal(final String max) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      checkForDateFormat(max);
      this.max = max;
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setMinInternal(final String min) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      checkForDateFormat(min);
      this.min = min;
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setReadonlyInternal(final boolean isReadonly) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.readonly = isReadonly;
    } finally {
      releaseSchemaWriteLock();
    }
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

  protected void setMandatoryInternal(final boolean isMandatory) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.mandatory = isMandatory;
    } finally {
      releaseSchemaWriteLock();
    }
  }

}
