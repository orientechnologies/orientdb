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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OPropertyIndex;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Contains the description of a persistent class property.
 * 
 * @author Luca Garulli
 * 
 */
public class OProperty extends ODocumentWrapperNoClass implements Comparable<OProperty> {
	public static enum INDEX_TYPE {
		UNIQUE, NOTUNIQUE, FULLTEXT, DICTIONARY
	};

	private OClass						owner;

	private int								id;
	private String						name;
	private OType							type;
	private int								offset;

	private OType							linkedType;
	private OClass						linkedClass;
	transient private String	linkedClassName;

	private OPropertyIndex		index;

	private boolean						mandatory;
	private boolean						notNull;
	private String						min;
	private String						max;
	private String						regexp;

	/**
	 * Constructor used in unmarshalling.
	 */
	public OProperty() {
	}

	public OProperty(OClass iOwner, String iName, OType iType, int iOffset) {
		this(iOwner);
		name = iName;
		type = iType;
		offset = iOffset;
	}

	public OProperty(OClass iOwner) {
		document = new ODocument(iOwner.getDocument().getDatabase());
		owner = iOwner;
		id = iOwner.properties.size();
	}

	public OProperty(final OClass iOwner, final ODocument iDocument) {
		this(iOwner);
		document = iDocument;
	}

	public String getName() {
		return name;
	}

	public OType getType() {
		return type;
	}

	public int offset() {
		return offset;
	}

	public int getId() {
		return id;
	}

	public int compareTo(final OProperty o) {
		return id - o.getId();
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

	public OProperty setLinkedClass(OClass linkedClass) {
		this.linkedClass = linkedClass;
		setDirty();
		return this;
	}

	@Override
	public void fromStream() {
		name = document.field("name");
		if (document.field("type") != null)
			type = OType.getById(((Integer) document.field("type")).byteValue());
		offset = ((Integer) document.field("offset")).intValue();

		mandatory = (Boolean) document.field("mandatory");
		notNull = (Boolean) document.field("notNull");
		min = document.field("min");
		max = document.field("max");
		regexp = document.field("regexp");

		linkedClassName = (String) document.field("linkedClass");
		if (document.field("linkedType") != null)
			linkedType = OType.getById(((Integer) document.field("linkedType")).byteValue());

		if (document.field("index") != null) {
			if (document.field("index-type") == null) {
				final OIndex underlyingIndex = document.getDatabase().getMetadata().getIndexManager()
						.getIndex((ORecordId) document.field("index", ORID.class));

				if (underlyingIndex != null)
					index = new OPropertyIndex(underlyingIndex, new String[] { name });
				else
					// REMOVE WRONG INDEX REF
					document.removeField("index");
			} else {
				// @COMPATIBILITY 0.9.24
				ODocument cfg = new ODocument(document.getDatabase());
				cfg.field(OIndex.CONFIG_TYPE, (String) document.field("index-type"));
				index = new OPropertyIndex(document.getDatabase(), owner, new String[] { name }, cfg);
			}
		}
	}

	@Override
	@OBeforeSerialization
	public ODocument toStream() {
		document.setStatus(STATUS.UNMARSHALLING);

		try {
			document.field("name", name);
			document.field("type", type.id);
			document.field("offset", offset);
			document.field("mandatory", mandatory);
			document.field("notNull", notNull);
			document.field("min", min);
			document.field("max", max);
			document.field("regexp", regexp);

			document.field("linkedClass", linkedClass != null ? linkedClass.getName() : null);
			document.field("linkedType", linkedType != null ? linkedType.id : null);

			// SAVE THE INDEX
			if (index != null) {
				index.getUnderlying().lazySave();
				document.field("index", index.getUnderlying().getIdentity());
			} else {
				document.field("index", ORecordId.EMPTY_RECORD_ID);
			}

		} finally {
			document.setStatus(STATUS.LOADED);
		}
		return document;
	}

	public OType getLinkedType() {
		return linkedType;
	}

	public OProperty setLinkedType(OType linkedType) {
		this.linkedType = linkedType;
		setDirty();
		return this;
	}

	public boolean isNotNull() {
		return notNull;
	}

	public OProperty setNotNull(boolean iNotNull) {
		notNull = iNotNull;
		setDirty();
		return this;
	}

	public boolean isMandatory() {
		return mandatory;
	}

	public OProperty setMandatory(boolean mandatory) {
		this.mandatory = mandatory;
		setDirty();
		return this;
	}

	public String getMin() {
		return min;
	}

	public OProperty setMin(String min) {
		this.min = min;
		checkForDateFormat(min);
		setDirty();
		return this;
	}

	public String getMax() {
		return max;
	}

	public OProperty setMax(String max) {
		this.max = max;
		checkForDateFormat(max);
		setDirty();
		return this;
	}

	/**
	 * Creates an index on this property. Indexes speed up queries but slow down insert and update operations. For massive inserts we
	 * suggest to remove the index, make the massive insert and recreate it.
	 * 
	 * @param iUnique
	 *          Don't allow duplicates. Now is supported only unique indexes, so pass always TRUE
	 * @deprecated Use {@link #createIndex(INDEX_TYPE)} instead
	 * @return
	 */
	@Deprecated
	public OPropertyIndex createIndex(final boolean iUnique) {
		return createIndex(iUnique ? INDEX_TYPE.UNIQUE : INDEX_TYPE.NOTUNIQUE);
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
	 */
	public OPropertyIndex createIndex(final INDEX_TYPE iType) {
		return createIndex(iType.toString(), null);
	}

	/**
	 * Creates an index on this property. Indexes speed up queries but slow down insert and update operations. For massive inserts we
	 * suggest to remove the index, make the massive insert and recreate it. This version accepts a progress listener interface to
	 * handle the progress status from the external.
	 * 
	 * @param iType
	 *          Index type name registered in OIndexFactory. Defaults are:
	 *          <ul>
	 *          <li>UNIQUE: Doesn't allow duplicates</li>
	 *          <li>NOTUNIQUE: Allow duplicates</li>
	 *          <li>FULLTEXT: Indexes single word for full text search</li>
	 *          </ul>
	 * @return
	 */
	public OPropertyIndex createIndex(final String iType, final OProgressListener iProgressListener) {
		index = new OPropertyIndex(document.getDatabase(), owner, new String[] { name }, iType, iProgressListener);
		return index;
	}

	public OPropertyIndex setIndex(final OIndex iIndex) {
		index = new OPropertyIndex(iIndex, new String[] { name });
		return index;
	}

	public OPropertyIndex createIndex(final ODocument iToLoad) {
		index = new OPropertyIndex(document.getDatabase(), owner, new String[] { name }, iToLoad);
		return index;
	}

	/**
	 * Remove the index on property
	 */
	public void removeIndex() {
		if (index != null) {
			document.getDatabase().getMetadata().getIndexManager().deleteIndex(index.getUnderlying().getName());
			index = null;
		}
	}

	public OPropertyIndex getIndex() {
		return index;
	}

	public boolean isIndexed() {
		return index != null;
	}

	public OClass getOwnerClass() {
		return owner;
	}

	public OProperty setDirty() {
		document.setDirty();
		if (owner != null)
			owner.setDirty();
		return this;
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
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		OProperty other = (OProperty) obj;
		if (owner == null) {
			if (other.owner != null)
				return false;
		} else if (!owner.equals(other.owner))
			return false;
		return true;
	}

	private void checkForDateFormat(String min) {
		if (type == OType.DATE) {
			try {
				owner.owner.getDocument().getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance().parse(min);
			} catch (ParseException e) {
				throw new OSchemaException("Invalid date format setted", e);
			}
		}
	}

	public String getRegexp() {
		return regexp;
	}

	public void setRegexp(String regexp) {
		this.regexp = regexp;
	}

	/**
	 * Change the type. It checks for compatibility between the change of type.
	 * 
	 * @param iType
	 */
	public void setType(final OType iType) {
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
		setDirty();
	}
}
