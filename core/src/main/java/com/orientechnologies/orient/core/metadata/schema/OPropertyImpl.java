/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Contains the description of a persistent class property.
 * 
 * @author Luca Garulli
 * 
 */
public class OPropertyImpl extends ODocumentWrapperNoClass implements OProperty {
	private OClassImpl				owner;

	private String						name;
	private OType							type;

	private OType							linkedType;
	private OClass						linkedClass;
	transient private String	linkedClassName;

	private boolean						mandatory;
	private boolean						notNull	= false;
	private String						min;
	private String						max;
	private String						regexp;

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
		document = new ODocument(iOwner.getDocument().getDatabase());
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
	 * 
	 * @param iType
	 *          One of types supported.
	 *          <ul>
	 *          <li>UNIQUE: Doesn't allow duplicates</li>
	 *          <li>NOTUNIQUE: Allow duplicates</li>
	 *          <li>FULLTEXT: Indexes single word for full text search</li>
	 *          </ul>
	 * @return
	 * @deprecated Use {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)} instead.
	 */
	@Deprecated
	public OIndex<?> createIndex(final OClass.INDEX_TYPE iType) {
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
	public Set<OIndex<?>> getIndex() {
		Set<OIndex<?>> indexes = owner.getInvolvedIndexes(name);
		if (indexes != null)
			indexes.iterator().next();
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
			throw new IllegalArgumentException("Can't change property type from " + type + " to " + iType);

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

		throw new IllegalArgumentException("Can't find attribute '" + iAttribute + "'");
	}

	public void setInternalAndSave(final ATTRIBUTES attribute, final Object iValue) {
		if (attribute == null)
			throw new IllegalArgumentException("attribute is null");

		String stringValue = iValue != null ? iValue.toString() : null;

		switch (attribute) {
		case LINKEDCLASS:
			setLinkedClassInternal(getDatabase().getMetadata().getSchema().getClass(stringValue));
			break;
		case LINKEDTYPE:
			setLinkedTypeInternal(OType.valueOf(stringValue));
			break;
		case MIN:
			setMinInternal(stringValue);
			break;
		case MANDATORY:
			setMandatoryInternal(Boolean.parseBoolean(stringValue));
			break;
		case MAX:
			setMaxInternal(stringValue);
			break;
		case NAME:
			setNameInternal(stringValue);
			break;
		case NOTNULL:
			setNotNullInternal(Boolean.parseBoolean(stringValue));
			break;
		case REGEXP:
			setRegexpInternal(stringValue);
			break;
		case TYPE:
			setTypeInternal(OType.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
			break;
		}

		try {
			// owner.validateInstances();
			saveInternal();
		} catch (Exception e) {
			owner.reload();
		}
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

	@Override
	public void fromStream() {
		name = document.field("name");
		if (document.field("type") != null)
			type = OType.getById(((Integer) document.field("type")).byteValue());

		mandatory = (Boolean) document.field("mandatory");
		notNull = (Boolean) document.field("notNull");
		min = document.field("min");
		max = document.field("max");
		regexp = document.field("regexp");

		linkedClassName = (String) document.field("linkedClass");
		if (document.field("linkedType") != null)
			linkedType = OType.getById(((Integer) document.field("linkedType")).byteValue());
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
			document.field("notNull", notNull);
			document.field("min", min);
			document.field("max", max);
			document.field("regexp", regexp);

			document.field("linkedClass", linkedClass != null ? linkedClass.getName() : linkedClassName);
			document.field("linkedType", linkedType != null ? linkedType.id : null);
		} finally {
			document.setInternalStatus(ORecordElement.STATUS.LOADED);
		}
		return document;
	}

	public void saveInternal() {
		if (getDatabase().getStorage() instanceof OStorageEmbedded)
			((OSchemaProxy) getDatabase().getMetadata().getSchema()).saveInternal();
	}

	private void checkForDateFormat(final String iDateAsString) {
		if (iDateAsString != null)
			if (type == OType.DATE) {
				try {
					owner.owner.getDocument().getDatabase().getStorage().getConfiguration().getDateFormatInstance().parse(iDateAsString);
				} catch (ParseException e) {
					throw new OSchemaException("Invalid date format was set", e);
				}
			} else if (type == OType.DATETIME) {
				try {
					owner.owner.getDocument().getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance().parse(iDateAsString);
				} catch (ParseException e) {
					throw new OSchemaException("Invalid datetime format was set", e);
				}
			}
	}

	protected ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}
}
