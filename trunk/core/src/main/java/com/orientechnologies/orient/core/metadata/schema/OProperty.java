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

import com.orientechnologies.common.collection.OTreeMap;
import com.orientechnologies.orient.core.record.ORecordPositional;
import com.orientechnologies.orient.core.serialization.OSerializableRecordPositional;

public class OProperty implements OSerializableRecordPositional {
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

	public void fromStream(ORecordPositional<String> iRecord) {
		name = iRecord.next();
		type = OType.getById(Integer.parseInt(iRecord.next()));
		offset = Integer.parseInt(iRecord.next());

		mandatory = Boolean.parseBoolean(iRecord.next());
		notNull = Boolean.parseBoolean(iRecord.next());
		min = iRecord.next();
		if (min.length() == 0)
			min = null;
		max = iRecord.next();
		if (max.length() == 0)
			max = null;

		if (type == OType.EMBEDDED || type == OType.LINK || type == OType.LINKSET)
			linkedClass = owner.owner.getClass(iRecord.next());
		else if (type == OType.EMBEDDEDSET || type == OType.EMBEDDEDLIST) {
			String value = iRecord.next();
			if (value != null && value.length() > 0)
				linkedClass = owner.owner.getClass(value);

			value = iRecord.next();
			if (value != null && value.length() > 0)
				linkedType = OType.getById(Integer.parseInt(value));
		}
	}

	public void toStream(ORecordPositional<String> iRecord) {
		iRecord.add(name);
		iRecord.add(String.valueOf(type.id));
		iRecord.add(String.valueOf(offset));
		iRecord.add(mandatory ? "1" : "0");
		iRecord.add(notNull ? "1" : "0");
		iRecord.add(min);
		iRecord.add(max);

		if (type == OType.EMBEDDED || type == OType.LINK || type == OType.LINKSET)
			iRecord.add(linkedClass.getName());
		else if (type == OType.EMBEDDEDSET || type == OType.EMBEDDEDLIST) {
			iRecord.add(linkedClass != null ? linkedClass.getName() : "");
			iRecord.add(linkedType != null ? String.valueOf(linkedType.id) : "");
		}
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
		return this;
	}

	public String getMax() {
		return max;
	}

	public OProperty setMax(String max) {
		this.max = max;
		return this;
	}

	public OTreeMap<?, ?> getIndex() {
		return index;
	}

	@Override
	public String toString() {
		return name + " (id=" + id + ", " + type + ")";
	}
}
