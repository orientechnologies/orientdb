/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

import java.text.ParseException;
import java.util.*;

/**
 * Contains the description of a persistent class property.
 * 
 * @author Luca Garulli
 * 
 */
public class OPropertyImpl extends ODocumentWrapperNoClass implements OProperty {
  private final OClassImpl owner;

  // private String name;
  // private OType type;

  private OType            linkedType;
  private OClass           linkedClass;
  transient private String linkedClassName;

  private boolean             mandatory;
  private boolean             notNull = false;
  private String              min;
  private String              max;
  private String              defaultValue;
  private String              regexp;
  private boolean             readonly;
  private Map<String, String> customFields;
  private OCollate            collate = new ODefaultCollate();
  private OGlobalProperty     globalRef;

  private volatile int hashCode;

  @Deprecated
  OPropertyImpl(final OClassImpl owner, final String name, final OType type) {
    this(owner);
    // this.name = name;
    // this.type = type;
  }

  OPropertyImpl(final OClassImpl owner) {
    document = new ODocument().setTrackingChanges(false);
    this.owner = owner;
  }

  OPropertyImpl(final OClassImpl owner, final ODocument document) {
    this(owner);
    this.document = document;
  }

  public OPropertyImpl(OClassImpl oClassImpl, OGlobalProperty global) {
    this(oClassImpl);
    this.globalRef = global;
  }

  public String getName() {
    acquireSchemaReadLock();
    try {
      return globalRef.getName();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getFullName() {
    acquireSchemaReadLock();
    try {
      return owner.getName() + "." + globalRef.getName();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OType getType() {
    acquireSchemaReadLock();
    try {
      return globalRef.getType();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setType(final OType type) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    final ODatabaseDocumentInternal database = getDatabase();
    acquireSchemaWriteLock();
    try {
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s type %s", getFullName(), type.toString());
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s type %s", getFullName(), type.toString());
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

  public int compareTo(final OProperty o) {
    acquireSchemaReadLock();
    try {
      return globalRef.getName().compareTo(o.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update operations. For massive inserts we
   * suggest to remove the index, make the massive insert and recreate it.
   * 
   * @param iType
   *          One of types supported.
   *          <ul>
   *          <li>UNIQUE: Doesn't allow duplicates</li>
   *          <li>NOTUNIQUE: Allow duplicates</li>
   *          <li>FULLTEXT: Indexes single word for full text search</li>
   *          </ul>
   * @return
   * @see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)} instead.
   */
  public OIndex<?> createIndex(final OClass.INDEX_TYPE iType) {
    return createIndex(iType.toString());
  }

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update operations. For massive inserts we
   * suggest to remove the index, make the massive insert and recreate it.
   * 
   * @param iType
   * @return
   * @see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)} instead.
   */
  public OIndex<?> createIndex(final String iType) {
    acquireSchemaReadLock();
    try {
      return owner.createIndex(getFullName(), iType, globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Remove the index on property
   * 
   * @deprecated Use {@link OIndexManager#dropIndex(String)} instead.
   */
  @Deprecated
  public OPropertyImpl dropIndexes() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaReadLock();
    try {
      final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

      final ArrayList<OIndex<?>> relatedIndexes = new ArrayList<OIndex<?>>();
      for (final OIndex<?> index : indexManager.getClassIndexes(owner.getName())) {
        final OIndexDefinition definition = index.getDefinition();

        if (OCollections.indexOf(definition.getFields(), globalRef.getName(), new OCaseInsentiveComparator()) > -1) {
          if (definition instanceof OPropertyIndexDefinition) {
            relatedIndexes.add(index);
          } else {
            throw new IllegalArgumentException(
                "This operation applicable only for property indexes. " + index.getName() + " is " + index.getDefinition());
          }
        }
      }

      for (final OIndex<?> index : relatedIndexes)
        getDatabase().getMetadata().getIndexManager().dropIndex(index.getName());

      return this;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Remove the index on property
   * 
   * @deprecated by {@link #dropIndexes()}
   */
  @Deprecated
  public void dropIndexesInternal() {
    dropIndexes();
  }

  /**
   * Returns the first index defined for the property.
   * 
   * @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead.
   */
  @Deprecated
  public OIndex<?> getIndex() {
    acquireSchemaReadLock();
    try {
      Set<OIndex<?>> indexes = owner.getInvolvedIndexes(globalRef.getName());
      if (indexes != null && !indexes.isEmpty())
        return indexes.iterator().next();
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * 
   * @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead.
   */
  @Deprecated
  public Set<OIndex<?>> getIndexes() {
    acquireSchemaReadLock();
    try {
      return owner.getInvolvedIndexes(globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * 
   * @deprecated Use {@link OClass#areIndexed(String...)} instead.
   */
  @Deprecated
  public boolean isIndexed() {
    acquireSchemaReadLock();
    try {
      return owner.areIndexed(globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OClass getOwnerClass() {
    return owner;
  }

  public OProperty setName(final String name) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s name %s", getFullName(), name);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s name %s", getFullName(), name);
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

  /**
   * Returns the linked class in lazy mode because while unmarshalling the class could be not loaded yet.
   * 
   * @return
   */
  public OClass getLinkedClass() {
    acquireSchemaReadLock();
    try {
      if (linkedClass == null && linkedClassName != null)
        linkedClass = owner.owner.getClass(linkedClassName);
      return linkedClass;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setLinkedClass(final OClass linkedClass) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    checkSupportLinkedClass(getType());

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s linkedclass %s", getFullName(), linkedClass);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s linkedclass %s", getFullName(), linkedClass);
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

  void setLinkedClassInternal(final OClass iLinkedClass) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.linkedClass = iLinkedClass;

    } finally {
      releaseSchemaWriteLock();
    }

  }

  protected static void checkSupportLinkedClass(OType type) {
    if (type != OType.LINK && type != OType.LINKSET && type != OType.LINKLIST && type != OType.LINKMAP && type != OType.EMBEDDED
        && type != OType.EMBEDDEDSET && type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDMAP)
      throw new OSchemaException("Linked class is not supported for type: " + type);
  }

  public OType getLinkedType() {
    acquireSchemaReadLock();
    try {
      return linkedType;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OProperty setLinkedType(final OType linkedType) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    checkLinkTypeSupport(getType());

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s linkedtype %s", getFullName(), linkedType);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s linkedtype %s", getFullName(), linkedType);
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

  void setLinkedTypeInternal(final OType iLinkedType) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);
    acquireSchemaWriteLock();
    try {
      checkEmbedded();
      this.linkedType = iLinkedType;

    } finally {
      releaseSchemaWriteLock();
    }

  }

  protected static void checkLinkTypeSupport(OType type) {
    if (type != OType.EMBEDDEDSET && type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDMAP)
      throw new OSchemaException("Linked type is not supported for type: " + type);
  }

  public boolean isNotNull() {
    acquireSchemaReadLock();
    try {
      return notNull;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setNotNull(final boolean isNotNull) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s notnull %s", getFullName(), isNotNull);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s notnull %s", getFullName(), isNotNull);
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

  public boolean isMandatory() {
    acquireSchemaReadLock();
    try {
      return mandatory;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setMandatory(final boolean isMandatory) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s mandatory %s", getFullName(), isMandatory);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s mandatory %s", getFullName(), isMandatory);
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

  public boolean isReadonly() {
    acquireSchemaReadLock();
    try {
      return readonly;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setReadonly(final boolean isReadonly) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s readonly %s", getFullName(), isReadonly);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s readonly %s", getFullName(), isReadonly);
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

  public String getMin() {
    acquireSchemaReadLock();
    try {
      return min;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setMin(final String min) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s min %s", getFullName(), min);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s min %s", getFullName(), min);
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

  public String getMax() {
    acquireSchemaReadLock();
    try {
      return max;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setMax(final String max) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s max %s", getFullName(), max);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s max %s", getFullName(), max);
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

  public String getDefaultValue() {
    acquireSchemaReadLock();
    try {
      return defaultValue;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setDefaultValue(final String defaultValue) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s default %s", getFullName(), defaultValue);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s default %s", getFullName(), defaultValue);
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

  public String getRegexp() {
    acquireSchemaReadLock();
    try {
      return regexp;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setRegexp(final String regexp) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        final String cmd = String.format("alter property %s regexp %s", getFullName(), regexp);
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
        final String cmd = String.format("alter property %s regexp %s", getFullName(), regexp);
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

  public String getCustom(final String iName) {
    acquireSchemaReadLock();
    try {
      if (customFields == null)
        return null;

      return customFields.get(iName);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OPropertyImpl setCustom(final String name, final String value) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final String cmd = String.format("alter property %s custom %s=%s", getFullName(), name, value);

      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      if (storage instanceof OStorageProxy) {
        database.command(new OCommandSQL(cmd)).execute();
      } else if (isDistributedCommand()) {
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

  public Map<String, String> getCustomInternal() {
    acquireSchemaReadLock();
    try {
      if (customFields != null)
        return Collections.unmodifiableMap(customFields);
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void removeCustom(final String iName) {
    setCustom(iName, null);
  }

  public void clearCustom() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      final ODatabaseDocumentInternal database = getDatabase();
      final OStorage storage = database.getStorage();

      final String cmd = String.format("alter property %s custom clear", getFullName());
      final OCommandSQL commandSQL = new OCommandSQL(cmd);

      if (storage instanceof OStorageProxy) {
        database.command(commandSQL).execute();
      } else if (isDistributedCommand()) {
        commandSQL.addExcludedNode(((OAutoshardedStorage) storage).getNodeId());
        database.command(commandSQL).execute();

        clearCustomInternal();
      } else
        clearCustomInternal();

    } finally {
      releaseSchemaWriteLock();
    }
  }

  public Set<String> getCustomKeys() {
    acquireSchemaReadLock();
    try {
      if (customFields != null)
        return customFields.keySet();

      return new HashSet<String>();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Object get(final ATTRIBUTES attribute) {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

    switch (attribute) {
    case LINKEDCLASS:
      return getLinkedClass();
    case LINKEDTYPE:
      return getLinkedType();
    case MIN:
      return getMin();
    case MANDATORY:
      return isMandatory();
    case READONLY:
      return isReadonly();
    case MAX:
      return getMax();
    case DEFAULT:
      return getDefaultValue();
    case NAME:
      return getName();
    case NOTNULL:
      return isNotNull();
    case REGEXP:
      return getRegexp();
    case TYPE:
      return getType();
    case COLLATE:
      return getCollate();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + attribute + "'");
  }

  public void set(final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    switch (attribute) {
    case LINKEDCLASS:
      setLinkedClass(getDatabase().getMetadata().getSchema().getClass(stringValue));
      break;
    case LINKEDTYPE:
      setLinkedType(OType.valueOf(stringValue));
      break;
    case MIN:
      setMin(stringValue);
      break;
    case MANDATORY:
      setMandatory(Boolean.parseBoolean(stringValue));
      break;
    case READONLY:
      setReadonly(Boolean.parseBoolean(stringValue));
      break;
    case MAX:
      setMax(stringValue);
      break;
    case DEFAULT:
      setDefaultValue(stringValue);
      break;
    case NAME:
      setName(stringValue);
      break;
    case NOTNULL:
      setNotNull(Boolean.parseBoolean(stringValue));
      break;
    case REGEXP:
      setRegexp(stringValue);
      break;
    case TYPE:
      setType(OType.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
      break;
    case COLLATE:
      setCollate(stringValue);
      break;
    case CUSTOM:
      int indx = stringValue != null ? stringValue.indexOf('=') : -1;
      if (indx < 0) {
        if ("clear".equalsIgnoreCase(stringValue)) {
          clearCustom();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        String customName = stringValue.substring(0, indx).trim();
        String customValue = stringValue.substring(indx + 1).trim();
        if (customValue.isEmpty())
          removeCustom(customName);
        else
          setCustom(customName, customValue);
      }
      break;
    }
  }

  public OCollate getCollate() {
    acquireSchemaReadLock();
    try {
      return collate;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public OProperty setCollate(final OCollate collate) {
    setCollate(collate.getName());
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

      final String cmd = String.format("alter property %s collate %s", getFullName(), collate);
      final OCommandSQL commandSQL = new OCommandSQL(cmd);

      if (storage instanceof OStorageProxy) {
        database.command(commandSQL).execute();
      } else if (isDistributedCommand()) {
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

  @Override
  public String toString() {
    acquireSchemaReadLock();
    try {
      return globalRef.getName() + " (type=" + globalRef.getType() + ")";
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public int hashCode() {
    int sh = hashCode;
    if (sh != 0)
      return sh;

    acquireSchemaReadLock();
    try {
      sh = hashCode;
      if (sh != 0)
        return sh;

      calculateHashCode();
      return hashCode;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void calculateHashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((owner == null) ? 0 : owner.hashCode());
    hashCode = result;
  }

  @Override
  public boolean equals(final Object obj) {
    acquireSchemaReadLock();
    try {
      if (this == obj)
        return true;
      if (obj == null || !OProperty.class.isAssignableFrom(obj.getClass()))
        return false;
      OProperty other = (OProperty) obj;
      if (owner == null) {
        if (other.getOwnerClass() != null)
          return false;
      } else if (!owner.equals(other.getOwnerClass()))
        return false;
      return this.getName().equals(other.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromStream() {

    String name = document.field("name");
    OType type = null;
    if (document.field("type") != null)
      type = OType.getById(((Integer) document.field("type")).byteValue());
    Integer globalId = document.field("globalId");
    if (globalId != null)
      globalRef = owner.owner.getGlobalPropertyById(globalId);
    else {
      if (type == null)
        type = OType.ANY;
      globalRef = owner.owner.findOrCreateGlobalProperty(name, type);
    }

    mandatory = document.containsField("mandatory") ? (Boolean) document.field("mandatory") : false;
    readonly = document.containsField("readonly") ? (Boolean) document.field("readonly") : false;
    notNull = document.containsField("notNull") ? (Boolean) document.field("notNull") : false;
    defaultValue = (String) (document.containsField("defaultValue") ? document.field("defaultValue") : null);
    if (document.containsField("collate"))
      collate = OSQLEngine.getCollate((String) document.field("collate"));

    min = (String) (document.containsField("min") ? document.field("min") : null);
    max = (String) (document.containsField("max") ? document.field("max") : null);
    regexp = (String) (document.containsField("regexp") ? document.field("regexp") : null);
    linkedClassName = (String) (document.containsField("linkedClass") ? document.field("linkedClass") : null);
    linkedType = document.field("linkedType") != null ? OType.getById(((Integer) document.field("linkedType")).byteValue()) : null;
    customFields = (Map<String, String>) (document.containsField("customFields") ? document.field("customFields", OType.EMBEDDEDMAP)
        : null);
  }

  public Collection<OIndex<?>> getAllIndexes() {
    acquireSchemaReadLock();
    try {
      final Set<OIndex<?>> indexes = owner.getIndexes();
      final List<OIndex<?>> indexList = new LinkedList<OIndex<?>>();
      for (final OIndex<?> index : indexes) {
        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition.getFields().contains(globalRef.getName()))
          indexList.add(index);
      }

      return indexList;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  @OBeforeSerialization
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

    try {
      document.field("name", getName());
      document.field("type", getType().id);
      document.field("globalId", globalRef.getId());
      document.field("mandatory", mandatory);
      document.field("readonly", readonly);
      document.field("notNull", notNull);
      document.field("defaultValue", defaultValue);

      document.field("min", min);
      document.field("max", max);
      document.field("regexp", regexp);

      if (linkedType != null)
        document.field("linkedType", linkedType.id);
      if (linkedClass != null || linkedClassName != null)
        document.field("linkedClass", linkedClass != null ? linkedClass.getName() : linkedClassName);

      document.field("customFields", customFields != null && customFields.size() > 0 ? customFields : null, OType.EMBEDDEDMAP);
      document.field("collate", collate.getName());

    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
    return document;
  }

  public void acquireSchemaReadLock() {
    owner.acquireSchemaReadLock();
  }

  public void releaseSchemaReadLock() {
    owner.releaseSchemaReadLock();
  }

  public void acquireSchemaWriteLock() {
    owner.acquireSchemaWriteLock();
  }

  public void releaseSchemaWriteLock() {
    calculateHashCode();
    owner.releaseSchemaWriteLock();
  }

  public void checkEmbedded() {
    if (!(getDatabase().getStorage().getUnderlying() instanceof OAbstractPaginatedStorage))
      throw new OSchemaException("'Internal' schema modification methods can be used only inside of embedded database");
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private void setNameInternal(final String name) {
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

  private void setNotNullInternal(final boolean isNotNull) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      notNull = isNotNull;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setMandatoryInternal(final boolean isMandatory) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.mandatory = isMandatory;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setReadonlyInternal(final boolean isReadonly) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.readonly = isReadonly;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setMinInternal(final String min) {
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

  private void setDefaultValueInternal(final String defaultValue) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      this.defaultValue = defaultValue;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setMaxInternal(final String max) {
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

  private void setRegexpInternal(final String regexp) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    acquireSchemaWriteLock();
    try {
      this.regexp = regexp;
    } finally {
      releaseSchemaWriteLock();
    }
  }

  private void setCustomInternal(final String iName, final String iValue) {
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

  private void clearCustomInternal() {
    acquireSchemaWriteLock();
    try {
      checkEmbedded();

      customFields = null;
    } finally {
      releaseSchemaWriteLock();
    }

  }

  /**
   * Change the type. It checks for compatibility between the change of type.
   * 
   * @param iType
   */
  private void setTypeInternal(final OType iType) {
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

  private OProperty setCollateInternal(String iCollate) {
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
            final OIndexInternal.IndexMetadata indexMetadata = indexToRecreate.getInternal()
                .loadMetadata(indexToRecreate.getConfiguration());

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

  private void checkForDateFormat(final String iDateAsString) {
    if (iDateAsString != null)
      if (globalRef.getType() == OType.DATE) {
        try {
          getDatabase().getStorage().getConfiguration().getDateFormatInstance().parse(iDateAsString);
        } catch (ParseException e) {
          throw new OSchemaException("Invalid date format while formatting date '" + iDateAsString + "'", e);
        }
      } else if (globalRef.getType() == OType.DATETIME) {
        try {
          getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance().parse(iDateAsString);
        } catch (ParseException e) {
          throw new OSchemaException("Invalid datetime format while formatting date '" + iDateAsString + "'", e);
        }
      }
  }

  private boolean isDistributedCommand() {
    return getDatabase().getStorage() instanceof OAutoshardedStorage
        && OScenarioThreadLocal.INSTANCE.getRunMode() != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED;
  }

  @Override
  public Integer getId() {
    return globalRef.getId();
  }

}
