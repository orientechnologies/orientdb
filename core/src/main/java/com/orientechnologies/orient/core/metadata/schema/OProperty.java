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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OPropertyIndex;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Contains the description of a persistent class property.
 * 
 * @author Luca Garulli
 * 
 */
public class OProperty extends ODocumentWrapperNoClass {
	public static enum INDEX_TYPE {
		UNIQUE, NOTUNIQUE, FULLTEXT
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
			setIndex(INDEX_TYPE.valueOf((String) document.field("index-type")), ((ORecord<?>) document.field("index")).getIdentity());
			index.load();
		}
	}

	@Override
	@OBeforeSerialization
	public ODocument toStream() {
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
			index.lazySave();
			document.field("index", index.getIdentity());
			document.field("index-type", index.getType());
		} else {
			document.field("index", ORecordId.EMPTY_RECORD_ID);
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
	 * Creates a custom index on this property.
	 * 
	 * @param iIndexInstance
	 *          OPropertyIndex implementation.
	 * @return
	 */
	public OPropertyIndex createIndex(final OPropertyIndex iIndexInstance) {
		return createIndex(iIndexInstance, null);
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
		return createIndex(OIndexFactory.instance().newInstance(iType), iProgressListener);
	}

	/**
	 * Remove the index on property
	 */
	public void removeIndex() {
		if (index != null) {
			index.delete();
			index = null;
			setDirty();
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

	protected OPropertyIndex setIndex(final INDEX_TYPE iType, final ORID iRID) {
		return setIndex(iType.toString(), iRID);
	}

	/**
	 * Load the index
	 * 
	 * @param iType
	 *          Index type (see OIndexFactory)
	 * @param iRID
	 *          RecordID of the persistent map
	 * @return The Index instance
	 * @see OIndexFactory
	 */
	protected OPropertyIndex setIndex(final String iType, final ORID iRID) {
		// LOAD THE INDEX
		if (iRID != null && iRID.isValid())
			index = OIndexFactory.instance().newInstance(iType).configure(document.getDatabase(), this, iRID);

		return index;
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

	protected OPropertyIndex createIndex(final OPropertyIndex iIndexInstance, final OProgressListener iProgressListener) {
		if (index != null)
			throw new IllegalStateException("Index already created");

		try {
			index = iIndexInstance;
			index.create(document.getDatabase(), this, OStorage.CLUSTER_INDEX_NAME, iProgressListener);

			setDirty();

			if (document.getDatabase() != null) {
				// / SAVE ONLY IF THE PROPERTY IS ALREADY PERSISTENT
				index.lazySave();
				document.getDatabase().getMetadata().getSchema().save();
			}

		} catch (OIndexException e) {
			if (index != null) {
				index.clear();
				document.getDatabase().getMetadata().getSchema().save();
			}
			throw e;

		} catch (Exception e) {
			if (index != null) {
				index.clear();
				document.getDatabase().getMetadata().getSchema().save();
			}

			OLogManager.instance().exception("Unable to create index of type '%s' for property: %s", e, ODatabaseException.class,
					iIndexInstance.toString(), toString());
		}

		return index;
	}

	public String getRegexp() {
		return regexp;
	}

	public void setRegexp(String regexp) {
		this.regexp = regexp;
	}
}
