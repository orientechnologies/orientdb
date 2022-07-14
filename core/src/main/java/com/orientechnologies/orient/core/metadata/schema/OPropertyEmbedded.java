package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/** Created by tglman on 14/06/17. */
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
        throw new IllegalArgumentException(
            "Cannot change property type from " + globalRef.getType() + " to " + iType);

      this.globalRef = owner.owner.findOrCreateGlobalProperty(this.globalRef.getName(), iType);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OProperty setName(final String name) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
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
    if (collate == null) collate = ODefaultCollate.NAME;

    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
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

      if (iCollate == null) iCollate = ODefaultCollate.NAME;

      collate = OSQLEngine.getCollate(iCollate);

      if ((this.collate != null && !this.collate.equals(oldCollate))
          || (this.collate == null && oldCollate != null)) {
        final Set<OIndex> indexes = owner.getClassIndexes();
        final List<OIndex> indexesToRecreate = new ArrayList<OIndex>();

        for (OIndex index : indexes) {
          OIndexDefinition definition = index.getDefinition();

          final List<String> fields = definition.getFields();
          if (fields.contains(getName())) indexesToRecreate.add(index);
        }

        if (!indexesToRecreate.isEmpty()) {
          OLogManager.instance()
              .info(
                  this,
                  "Collate value was changed, following indexes will be rebuilt %s",
                  indexesToRecreate);

          final ODatabaseDocumentInternal database = getDatabase();
          final OIndexManagerAbstract indexManager =
              database.getMetadata().getIndexManagerInternal();

          for (OIndex indexToRecreate : indexesToRecreate) {
            final OIndexMetadata indexMetadata =
                indexToRecreate.getInternal().loadMetadata(indexToRecreate.getConfiguration());

            final ODocument metadata = indexToRecreate.getMetadata();
            final List<String> fields = indexMetadata.getIndexDefinition().getFields();
            final String[] fieldsToIndex = fields.toArray(new String[fields.size()]);

            indexManager.dropIndex(database, indexMetadata.getName());
            owner.createIndex(
                indexMetadata.getName(),
                indexMetadata.getType(),
                null,
                metadata,
                indexMetadata.getAlgorithm(),
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

      if (customFields == null) customFields = new HashMap<String, String>();
      if (iValue == null || "null".equalsIgnoreCase(iValue)) customFields.remove(iName);
      else customFields.put(iName, iValue);
    } finally {
      releaseSchemaWriteLock();
    }
  }

  public OPropertyImpl setRegexp(final String regexp) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
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
      setDefaultValueInternal(defaultValue);
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
    checkCorrectLimitValue(max);

    acquireSchemaWriteLock();
    try {
      setMaxInternal(max);
    } finally {
      releaseSchemaWriteLock();
    }

    return this;
  }

  private void checkCorrectLimitValue(final String value) {
    if (value != null) {
      if (this.getType().equals(OType.STRING)
          || this.getType().equals(OType.LINKBAG)
          || this.getType().equals(OType.BINARY)
          || this.getType().equals(OType.EMBEDDEDLIST)
          || this.getType().equals(OType.EMBEDDEDSET)
          || this.getType().equals(OType.LINKLIST)
          || this.getType().equals(OType.LINKSET)
          || this.getType().equals(OType.LINKBAG)
          || this.getType().equals(OType.EMBEDDEDMAP)
          || this.getType().equals(OType.LINKMAP)) {
        OType.convert(value, Integer.class);
      } else if (this.getType().equals(OType.DATE)
          || this.getType().equals(OType.BYTE)
          || this.getType().equals(OType.SHORT)
          || this.getType().equals(OType.INTEGER)
          || this.getType().equals(OType.LONG)
          || this.getType().equals(OType.FLOAT)
          || this.getType().equals(OType.DOUBLE)
          || this.getType().equals(OType.DECIMAL)
          || this.getType().equals(OType.DATETIME)) {
        OType.convert(value, this.getType().getDefaultJavaType());
      }
    }
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
    checkCorrectLimitValue(min);

    acquireSchemaWriteLock();
    try {
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
