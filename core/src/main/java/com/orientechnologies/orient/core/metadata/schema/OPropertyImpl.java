/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.metadata.schema;

import java.text.ParseException;
import java.util.*;

import com.orientechnologies.common.util.OCaseIncentiveComparator;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Contains the description of a persistent class property.
 * 
 * @author Luca Garulli
 * 
 */
public class OPropertyImpl extends ODocumentWrapperNoClass implements OProperty {
  private OClassImpl          owner;

  private String              name;
  private OType               type;

  private OType               linkedType;
  private OClass              linkedClass;
  transient private String    linkedClassName;

  private boolean             mandatory;
  private boolean             notNull = false;
  private String              min;
  private String              max;
  private String              regexp;
  private boolean             readonly;
  private Map<String, String> customFields;

  /**
   * Constructor used in unmarshalling.
   */
  public OPropertyImpl() {
  }

  public OPropertyImpl(final OClassImpl iOwner, final String iName, final OType iType) {
    this(iOwner);
    name = iName;
    type = iType;
  }

  public OPropertyImpl(final OClassImpl iOwner) {
    document = new ODocument();
    owner = iOwner;
  }

  public OPropertyImpl(final OClassImpl iOwner, final ODocument iDocument) {
    this(iOwner);
    document = iDocument;
  }

  public String getName() {
    return name;
  }

  public String getFullName() {
    return owner.getName() + "." + name;
  }

  public OType getType() {
    return type;
  }

  public int compareTo(final OProperty o) {
    return name.compareTo(o.getName());
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
    return owner.createIndex(getFullName(), iType, name);
  }

  /**
   * Remove the index on property
   * 
   * @deprecated Use {@link OIndexManager#dropIndex(String)} instead.
   */
  @Deprecated
  public OPropertyImpl dropIndexes() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

    final ArrayList<OIndex<?>> relatedIndexes = new ArrayList<OIndex<?>>();
    for (final OIndex<?> index : indexManager.getClassIndexes(owner.getName())) {
      final OIndexDefinition definition = index.getDefinition();

      if (OCollections.indexOf(definition.getFields(), name, new OCaseIncentiveComparator()) > -1) {
        if (definition instanceof OPropertyIndexDefinition) {
          relatedIndexes.add(index);
        } else {
          throw new IllegalArgumentException("This operation applicable only for property indexes. " + index.getName() + " is "
              + index.getDefinition());
        }
      }
    }

    for (final OIndex<?> index : relatedIndexes) {
      getDatabase().getMetadata().getIndexManager().dropIndex(index.getName());
    }

    return this;
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
    Set<OIndex<?>> indexes = owner.getInvolvedIndexes(name);
    if (indexes != null && !indexes.isEmpty())
      return indexes.iterator().next();
    return null;
  }

  /**
   * 
   * @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead.
   */
  @Deprecated
  public Set<OIndex<?>> getIndexes() {
    return owner.getInvolvedIndexes(name);
  }

  /**
   * 
   * @deprecated Use {@link OClass#areIndexed(String...)} instead.
   */
  @Deprecated
  public boolean isIndexed() {
    return owner.areIndexed(name);
  }

  public OClass getOwnerClass() {
    return owner;
  }

  public OProperty setName(final String iName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s name %s", getFullName(), iName);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.name = iName;
    return this;
  }

  public void setNameInternal(final String iName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.name = iName;
  }

  /**
   * Returns the linked class in lazy mode because while unmarshalling the class could be not loaded yet.
   * 
   * @return
   */
  public OClass getLinkedClass() {
    if (linkedClass == null && linkedClassName != null)
      linkedClass = owner.owner.getClass(linkedClassName);
    return linkedClass;
  }

  public OPropertyImpl setLinkedClass(final OClass iLinkedClass) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s linkedclass %s", getFullName(), iLinkedClass);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.linkedClass = iLinkedClass;
    return this;
  }

  public void setLinkedClassInternal(final OClass iLinkedClass) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.linkedClass = iLinkedClass;
  }

  public OType getLinkedType() {
    return linkedType;
  }

  public OPropertyImpl setLinkedType(final OType iLinkedType) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s linkedtype %s", getFullName(), iLinkedType);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.linkedType = iLinkedType;
    return this;
  }

  public void setLinkedTypeInternal(final OType iLinkedType) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.linkedType = iLinkedType;
  }

  public boolean isNotNull() {
    return notNull;
  }

  public OPropertyImpl setNotNull(final boolean iNotNull) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s notnull %s", getFullName(), iNotNull);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    notNull = iNotNull;
    return this;
  }

  public void setNotNullInternal(final boolean iNotNull) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    notNull = iNotNull;
  }

  public boolean isMandatory() {
    return mandatory;
  }

  public OPropertyImpl setMandatory(final boolean iMandatory) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s mandatory %s", getFullName(), iMandatory);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.mandatory = iMandatory;

    return this;
  }

  public void setMandatoryInternal(final boolean iMandatory) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.mandatory = iMandatory;
  }

  public boolean isReadonly() {
    return readonly;
  }

  public OPropertyImpl setReadonly(final boolean iReadonly) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s readonly %s", getFullName(), iReadonly);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.readonly = iReadonly;

    return this;
  }

  public void setReadonlyInternal(final boolean iReadonly) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.readonly = iReadonly;
  }

  public String getMin() {
    return min;
  }

  public OPropertyImpl setMin(final String iMin) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s min %s", getFullName(), iMin);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.min = iMin;
    checkForDateFormat(iMin);
    return this;
  }

  public void setMinInternal(final String iMin) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.min = iMin;
    checkForDateFormat(iMin);
  }

  public String getMax() {
    return max;
  }

  public OPropertyImpl setMax(final String iMax) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s max %s", getFullName(), iMax);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.max = iMax;
    checkForDateFormat(iMax);
    return this;
  }

  public void setMaxInternal(final String iMax) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.max = iMax;
    checkForDateFormat(iMax);
  }

  public String getRegexp() {
    return regexp;
  }

  public OPropertyImpl setRegexp(final String iRegexp) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s regexp %s", getFullName(), iRegexp);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    this.regexp = iRegexp;
    return this;
  }

  public void setRegexpInternal(final String iRegexp) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.regexp = iRegexp;
  }

  public OPropertyImpl setType(final OType iType) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s type %s", getFullName(), iType.toString());
    getDatabase().command(new OCommandSQL(cmd)).execute();
    type = iType;
    return this;
  }

  public String getCustom(final String iName) {
    if (customFields == null)
      return null;

    return customFields.get(iName);
  }

  public void setCustomInternal(final String iName, final String iValue) {
    if (customFields == null)
      customFields = new HashMap<String, String>();
    if (iValue == null || "null".equalsIgnoreCase(iValue))
      customFields.remove(iName);
    else
      customFields.put(iName, iValue);
  }

  public OPropertyImpl setCustom(final String iName, final String iValue) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s custom %s=%s", getFullName(), iName, iValue);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    setCustomInternal(iName, iValue);
    return this;
  }

  public Map<String, String> getCustomInternal() {
    if (customFields != null)
      return Collections.unmodifiableMap(customFields);
    return null;
  }

  public void removeCustom(final String iName) {
    setCustom(iName, null);
  }

  public void clearCustom() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter property %s custom clear", getFullName());
    getDatabase().command(new OCommandSQL(cmd)).execute();
    clearCustomInternal();
  }

  public void clearCustomInternal() {
    customFields = null;
  }

  public Set<String> getCustomKeys() {
    if (customFields != null)
      return customFields.keySet();
    return new HashSet<String>();
  }

  /**
   * Change the type. It checks for compatibility between the change of type.
   * 
   * @param iType
   */
  public void setTypeInternal(final OType iType) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    if (iType == type)
      // NO CHANGES
      return;

    boolean ok = false;
    switch (type) {
    case LINKLIST:
      ok = iType == OType.LINKSET;
      break;

    case LINKSET:
      ok = iType == OType.LINKLIST;
      break;
    }

    if (!ok)
      throw new IllegalArgumentException("Cannot change property type from " + type + " to " + iType);

    type = iType;
  }

  public Object get(final ATTRIBUTES iAttribute) {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    switch (iAttribute) {
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
    case NAME:
      return getName();
    case NOTNULL:
      return isNotNull();
    case REGEXP:
      return getRegexp();
    case TYPE:
      return getType();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
  }

  public void setInternalAndSave(final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;
    final boolean isNull = stringValue == null || stringValue.equalsIgnoreCase("NULL");

    switch (attribute) {
    case LINKEDCLASS:
      setLinkedClassInternal(isNull ? null : getDatabase().getMetadata().getSchema().getClass(stringValue));
      break;
    case LINKEDTYPE:
      setLinkedTypeInternal(isNull ? null : OType.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
      break;
    case MIN:
      setMinInternal(isNull ? null : stringValue);
      break;
    case MANDATORY:
      setMandatoryInternal(Boolean.parseBoolean(stringValue));
      break;
    case READONLY:
      setReadonlyInternal(Boolean.parseBoolean(stringValue));
      break;
    case MAX:
      setMaxInternal(isNull ? null : stringValue);
      break;
    case NAME:
      setNameInternal(stringValue);
      break;
    case NOTNULL:
      setNotNullInternal(Boolean.parseBoolean(stringValue));
      break;
    case REGEXP:
      setRegexpInternal(isNull ? null : stringValue);
      break;
    case TYPE:
      setTypeInternal(isNull ? null : OType.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
      break;
    case CUSTOM:
      if (iValue.toString().indexOf("=") == -1) {
        if (iValue.toString().equalsIgnoreCase("clear")) {
          clearCustomInternal();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        final List<String> words = OStringSerializerHelper.smartSplit(iValue.toString(), '=');
        setCustomInternal(words.get(0).trim(), words.get(1).trim());
      }
      break;
    }

    try {
      saveInternal();
    } catch (Exception e) {
    }

    owner.reload();
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
    case CUSTOM:
      if (iValue.toString().indexOf("=") == -1) {
        if (iValue.toString().equalsIgnoreCase("clear")) {
          clearCustom();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        final List<String> words = OStringSerializerHelper.smartSplit(iValue.toString(), '=');
        setCustom(words.get(0).trim(), words.get(1).trim());
      }
      break;
    }
  }

  @Override
  public String toString() {
    return name + " (type=" + type + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((owner == null) ? 0 : owner.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    OPropertyImpl other = (OPropertyImpl) obj;
    if (owner == null) {
      if (other.owner != null)
        return false;
    } else if (!owner.equals(other.owner))
      return false;
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromStream() {
    name = document.field("name");
    if (document.field("type") != null)
      type = OType.getById(((Integer) document.field("type")).byteValue());

    mandatory = document.containsField("mandatory") ? (Boolean) document.field("mandatory") : false;
    readonly = document.containsField("readonly") ? (Boolean) document.field("readonly") : false;
    notNull = document.containsField("notNull") ? (Boolean) document.field("notNull") : false;

    min = (String) (document.containsField("min") ? document.field("min") : null);
    max = (String) (document.containsField("max") ? document.field("max") : null);
    regexp = (String) (document.containsField("regexp") ? document.field("regexp") : null);
    linkedClassName = (String) (document.containsField("linkedClass") ? document.field("linkedClass") : null);
    linkedType = document.field("linkedType") != null ? OType.getById(((Integer) document.field("linkedType")).byteValue()) : null;
    customFields = (Map<String, String>) (document.containsField("customFields") ? document
        .field("customFields", OType.EMBEDDEDMAP) : null);
  }

  public Collection<OIndex<?>> getAllIndexes() {
    final Set<OIndex<?>> indexes = owner.getIndexes();
    final List<OIndex<?>> indexList = new LinkedList<OIndex<?>>();
    for (final OIndex<?> index : indexes) {
      final OIndexDefinition indexDefinition = index.getDefinition();
      if (indexDefinition.getFields().contains(name))
        indexList.add(index);
    }

    return indexList;
  }

  @Override
  @OBeforeSerialization
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

    try {
      document.field("name", name);
      document.field("type", type.id);
      document.field("mandatory", mandatory);
      document.field("readonly", readonly);
      document.field("notNull", notNull);

      document.field("min", min);
      document.field("max", max);
      document.field("regexp", regexp);

      if (linkedType != null)
        document.field("linkedType", linkedType.id);
      if (linkedClass != null || linkedClassName != null)
        document.field("linkedClass", linkedClass != null ? linkedClass.getName() : linkedClassName);

      document.field("customFields", customFields != null && customFields.size() > 0 ? customFields : null, OType.EMBEDDEDMAP);

    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
    return document;
  }

  public void saveInternal() {
    if (!(getDatabase().getStorage() instanceof OStorageProxy))
      ((OSchemaProxy) getDatabase().getMetadata().getSchema()).saveInternal();
  }

  private void checkForDateFormat(final String iDateAsString) {
    if (iDateAsString != null)
      if (type == OType.DATE) {
        try {
          owner.owner.getDocument().getDatabase().getStorage().getConfiguration().getDateFormatInstance().parse(iDateAsString);
        } catch (ParseException e) {
          throw new OSchemaException("Invalid date format while formatting date '" + iDateAsString + "'", e);
        }
      } else if (type == OType.DATETIME) {
        try {
          owner.owner.getDocument().getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance().parse(iDateAsString);
        } catch (ParseException e) {
          throw new OSchemaException("Invalid datetime format while formatting date '" + iDateAsString + "'", e);
        }
      }
  }

  protected ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
}
