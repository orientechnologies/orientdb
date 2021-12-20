/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Contains the description of a persistent class property.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OPropertyImpl implements OProperty {
  protected final OClassImpl owner;
  protected OType linkedType;
  protected OClass linkedClass;
  private transient String linkedClassName;

  protected String description;
  protected boolean mandatory;
  protected boolean notNull = false;
  protected String min;
  protected String max;
  protected String defaultValue;
  protected String regexp;
  protected boolean readonly;
  protected Map<String, String> customFields;
  protected OCollate collate = new ODefaultCollate();
  protected OGlobalProperty globalRef;
  protected ODocument document;

  private volatile int hashCode;

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

  public String getFullNameQuoted() {
    acquireSchemaReadLock();
    try {
      return "`" + owner.getName() + "`.`" + globalRef.getName() + "`";
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

  public int compareTo(final OProperty o) {
    acquireSchemaReadLock();
    try {
      return globalRef.getName().compareTo(o.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param iType One of types supported.
   *     <ul>
   *       <li>UNIQUE: Doesn't allow duplicates
   *       <li>NOTUNIQUE: Allow duplicates
   *       <li>FULLTEXT: Indexes single word for full text search
   *     </ul>
   *
   * @return
   * @see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)} instead.
   */
  public OIndex createIndex(final OClass.INDEX_TYPE iType) {
    return createIndex(iType.toString());
  }

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param iType
   * @return
   * @see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)} instead.
   */
  public OIndex createIndex(final String iType) {
    acquireSchemaReadLock();
    try {
      return owner.createIndex(getFullName(), iType, globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OIndex createIndex(OClass.INDEX_TYPE iType, ODocument metadata) {
    return createIndex(iType.name(), metadata);
  }

  @Override
  public OIndex createIndex(String iType, ODocument metadata) {
    acquireSchemaReadLock();
    try {
      return owner.createIndex(
          getFullName(), iType, null, metadata, new String[] {globalRef.getName()});
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Remove the index on property
   *
   * @deprecated Use SQL command instead.
   */
  @Deprecated
  public OPropertyImpl dropIndexes() {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaReadLock();
    try {
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
  public OIndex getIndex() {
    acquireSchemaReadLock();
    try {
      Set<OIndex> indexes = owner.getInvolvedIndexes(globalRef.getName());
      if (indexes != null && !indexes.isEmpty()) return indexes.iterator().next();
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /** @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead. */
  @Deprecated
  public Set<OIndex> getIndexes() {
    acquireSchemaReadLock();
    try {
      return owner.getInvolvedIndexes(globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /** @deprecated Use {@link OClass#areIndexed(String...)} instead. */
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

  /**
   * Returns the linked class in lazy mode because while unmarshalling the class could be not loaded
   * yet.
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

  public static void checkSupportLinkedClass(OType type) {
    if (type != OType.LINK
        && type != OType.LINKSET
        && type != OType.LINKLIST
        && type != OType.LINKMAP
        && type != OType.EMBEDDED
        && type != OType.EMBEDDEDSET
        && type != OType.EMBEDDEDLIST
        && type != OType.EMBEDDEDMAP
        && type != OType.LINKBAG)
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

  public static void checkLinkTypeSupport(OType type) {
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

  public boolean isMandatory() {
    acquireSchemaReadLock();
    try {
      return mandatory;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isReadonly() {
    acquireSchemaReadLock();
    try {
      return readonly;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getMin() {
    acquireSchemaReadLock();
    try {
      return min;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getMax() {
    acquireSchemaReadLock();
    try {
      return max;
    } finally {
      releaseSchemaReadLock();
    }
  }

  protected static Object quoteString(String s) {
    if (s == null) {
      return "null";
    }
    String result = "\"" + (s.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"")) + "\"";
    return result;
  }

  public String getDefaultValue() {
    acquireSchemaReadLock();
    try {
      return defaultValue;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getRegexp() {
    acquireSchemaReadLock();
    try {
      return regexp;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getCustom(final String iName) {
    acquireSchemaReadLock();
    try {
      if (customFields == null) return null;

      return customFields.get(iName);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, String> getCustomInternal() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) return Collections.unmodifiableMap(customFields);
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void removeCustom(final String iName) {
    setCustom(iName, null);
  }

  public Set<String> getCustomKeys() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) return customFields.keySet();

      return new HashSet<String>();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Object get(final ATTRIBUTES attribute) {
    if (attribute == null) throw new IllegalArgumentException("attribute is null");

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
      case DESCRIPTION:
        return getDescription();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + attribute + "'");
  }

  public void set(final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null) throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    switch (attribute) {
      case LINKEDCLASS:
        setLinkedClass(getDatabase().getMetadata().getSchema().getClass(stringValue));
        break;
      case LINKEDTYPE:
        if (stringValue == null) setLinkedType(null);
        else setLinkedType(OType.valueOf(stringValue));
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
            throw new IllegalArgumentException(
                "Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
        } else {
          String customName = stringValue.substring(0, indx).trim();
          String customValue = stringValue.substring(indx + 1).trim();
          if (isQuoted(customValue)) {
            customValue = removeQuotes(customValue);
          }
          if (customValue.isEmpty()) removeCustom(customName);
          else setCustom(customName, customValue);
        }
        break;
      case DESCRIPTION:
        setDescription(stringValue);
        break;
    }
  }

  private String removeQuotes(String s) {
    s = s.trim();
    return s.substring(1, s.length() - 1);
  }

  private boolean isQuoted(String s) {
    s = s.trim();
    if (s.startsWith("\"") && s.endsWith("\"")) return true;
    if (s.startsWith("'") && s.endsWith("'")) return true;
    if (s.startsWith("`") && s.endsWith("`")) return true;

    return false;
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

  @Override
  public String getDescription() {
    acquireSchemaReadLock();
    try {
      return description;
    } finally {
      releaseSchemaReadLock();
    }
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
    if (sh != 0) return sh;

    acquireSchemaReadLock();
    try {
      sh = hashCode;
      if (sh != 0) return sh;

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
      if (this == obj) return true;
      if (obj == null || !OProperty.class.isAssignableFrom(obj.getClass())) return false;
      OProperty other = (OProperty) obj;
      if (owner == null) {
        if (other.getOwnerClass() != null) return false;
      } else if (!owner.equals(other.getOwnerClass())) return false;
      return this.getName().equals(other.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  @SuppressWarnings("unchecked")
  public void fromStream() {

    String name = document.field("name");
    OType type = null;
    if (document.field("type") != null)
      type = OType.getById(((Integer) document.field("type")).byteValue());
    Integer globalId = document.field("globalId");
    if (globalId != null) globalRef = owner.owner.getGlobalPropertyById(globalId);
    else {
      if (type == null) type = OType.ANY;
      globalRef = owner.owner.findOrCreateGlobalProperty(name, type);
    }

    mandatory = document.containsField("mandatory") ? (Boolean) document.field("mandatory") : false;
    readonly = document.containsField("readonly") ? (Boolean) document.field("readonly") : false;
    notNull = document.containsField("notNull") ? (Boolean) document.field("notNull") : false;
    defaultValue =
        (String) (document.containsField("defaultValue") ? document.field("defaultValue") : null);
    if (document.containsField("collate"))
      collate = OSQLEngine.getCollate((String) document.field("collate"));

    min = (String) (document.containsField("min") ? document.field("min") : null);
    max = (String) (document.containsField("max") ? document.field("max") : null);
    regexp = (String) (document.containsField("regexp") ? document.field("regexp") : null);
    linkedClassName =
        (String) (document.containsField("linkedClass") ? document.field("linkedClass") : null);
    linkedType =
        document.field("linkedType") != null
            ? OType.getById(((Integer) document.field("linkedType")).byteValue())
            : null;
    if (document.containsField("customFields")) {
      customFields = document.field("customFields", OType.EMBEDDEDMAP);
    } else {
      customFields = null;
    }
    description =
        (String) (document.containsField("description") ? document.field("description") : null);
  }

  public Collection<OIndex> getAllIndexes() {
    acquireSchemaReadLock();
    try {
      final Set<OIndex> indexes = owner.getIndexes();
      final List<OIndex> indexList = new LinkedList<OIndex>();
      for (final OIndex index : indexes) {
        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition.getFields().contains(globalRef.getName())) indexList.add(index);
      }

      return indexList;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public ODocument toStream() {
    document.field("name", getName());
    document.field("type", getType().id);
    document.field("globalId", globalRef.getId());
    document.field("mandatory", mandatory);
    document.field("readonly", readonly);
    document.field("notNull", notNull);
    document.field("defaultValue", defaultValue);

    document.field("min", min);
    document.field("max", max);
    if (regexp != null) {
      document.field("regexp", regexp);
    } else {
      document.removeField("regexp");
    }
    if (linkedType != null) document.field("linkedType", linkedType.id);
    if (linkedClass != null || linkedClassName != null)
      document.field("linkedClass", linkedClass != null ? linkedClass.getName() : linkedClassName);

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        OType.EMBEDDEDMAP);
    if (collate != null) {
      document.field("collate", collate.getName());
    }
    document.field("description", description);
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
    if (getDatabase().isRemote())
      throw new OSchemaException(
          "'Internal' schema modification methods can be used only inside of embedded database");
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  protected void checkForDateFormat(final String iDateAsString) {
    if (iDateAsString != null)
      if (globalRef.getType() == OType.DATE) {
        try {
          ODateHelper.getDateFormatInstance(getDatabase()).parse(iDateAsString);
        } catch (ParseException e) {
          throw OException.wrapException(
              new OSchemaException(
                  "Invalid date format while formatting date '" + iDateAsString + "'"),
              e);
        }
      } else if (globalRef.getType() == OType.DATETIME) {
        try {
          ODateHelper.getDateTimeFormatInstance(getDatabase()).parse(iDateAsString);
        } catch (ParseException e) {
          throw OException.wrapException(
              new OSchemaException(
                  "Invalid datetime format while formatting date '" + iDateAsString + "'"),
              e);
        }
      }
  }

  protected boolean isDistributedCommand() {
    return !((ODatabaseDocumentInternal) getDatabase()).isLocalEnv();
  }

  @Override
  public Integer getId() {
    return globalRef.getId();
  }

  public void fromStream(ODocument document) {
    this.document = document;
    fromStream();
  }

  public ODocument toNetworkStream() {
    ODocument document = new ODocument();
    document.setTrackingChanges(false);
    document.field("name", getName());
    document.field("type", getType().id);
    document.field("globalId", globalRef.getId());
    document.field("mandatory", mandatory);
    document.field("readonly", readonly);
    document.field("notNull", notNull);
    document.field("defaultValue", defaultValue);

    document.field("min", min);
    document.field("max", max);
    if (regexp != null) {
      document.field("regexp", regexp);
    } else {
      document.removeField("regexp");
    }
    if (linkedType != null) document.field("linkedType", linkedType.id);
    if (linkedClass != null || linkedClassName != null)
      document.field("linkedClass", linkedClass != null ? linkedClass.getName() : linkedClassName);

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        OType.EMBEDDEDMAP);
    if (collate != null) {
      document.field("collate", collate.getName());
    }
    document.field("description", description);

    return document;
  }
}
