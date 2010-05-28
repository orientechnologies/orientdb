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

import com.orientechnologies.common.collection.OTreeMap;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Contains the description of a persistent class property.
 * 
 * @author Luca Garulli
 * 
 */
public class OProperty extends OSchemaRecord {
	private OClass					owner;

	private int							id;
	private String					name;
	private OType						type;
	private int							offset;
	private int							size;

	private OType						linkedType;
	private OClass					linkedClass;

	private OTreeMap<?, ?>	index;

	private boolean					mandatory;
	private boolean					notNull;
	private String					min;
	private String					max;

	/**
	 * Constructor used in unmarshalling.
	 */
	public OProperty() {
	}

	public OProperty(OClass iOwner) {
		owner = iOwner;
		id = iOwner.properties.size();
	}

	public OProperty(OClass iOwner, String iName, OType iType, int iOffset) {
		this(iOwner);
		name = iName;
		type = iType;
		offset = iOffset;
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

	public OClass getLinkedClass() {
		return linkedClass;
	}

	public OProperty setLinkedClass(OClass linkedClass) {
		this.linkedClass = linkedClass;
		return this;
	}

	public OProperty fromDocument(final ODocument iSource) {
		name = iSource.field("name");
		if (iSource.field("type") != null)
			type = OType.getById((Integer) iSource.field("type"));
		offset = iSource.field("offset");

		mandatory = iSource.field("mandatory");
		notNull = iSource.field("notNull");
		min = iSource.field("min");
		max = iSource.field("max");

		linkedClass = owner.owner.getClass((String) iSource.field("linkedClass"));
		if (field("linkedType") != null)
			linkedType = OType.getById((Integer) iSource.field("linkedType"));

		return this;
	}

	@Override
	public byte[] toStream() {
		field("name", name);
		field("type", type.id);
		field("offset", offset);
		field("mandatory", mandatory);
		field("notNull", notNull);
		field("min", min);
		field("max", max);

		field("linkedClass", linkedClass != null ? linkedClass.getName() : null);
		field("linkedType", linkedType != null ? linkedType.id : null);

		return super.toStream();
	}

	public OType getLinkedType() {
		return linkedType;
	}

	public OProperty setLinkedType(OType linkedType) {
		this.linkedType = linkedType;
		return this;
	}

	public boolean isNotNull() {
		return notNull;
	}

	public OProperty setNotNull(boolean iNotNull) {
		notNull = iNotNull;
		return this;
	}

	public boolean isMandatory() {
		return mandatory;
	}

	public OProperty setMandatory(boolean mandatory) {
		this.mandatory = mandatory;
		return this;
	}

	public String getMin() {
		return min;
	}

	public OProperty setMin(String min) {
		this.min = min;
		checkForDateFormat(min);
		return this;
	}

	public String getMax() {
		return max;
	}

	public OProperty setMax(String max) {
		this.max = max;
		checkForDateFormat(max);
		return this;
	}

	public OTreeMap<?, ?> getIndex() {
		return index;
	}

	@Override
	public String toString() {
		return name + " (id=" + id + ", " + type + ")";
	}

	private void checkForDateFormat(String min) {
		if (type == OType.DATE) {
			try {
				owner.owner.getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance().parse(min);
			} catch (ParseException e) {
				throw new OSchemaException("Invalid date format setted", e);
			}
		}
	}
}
