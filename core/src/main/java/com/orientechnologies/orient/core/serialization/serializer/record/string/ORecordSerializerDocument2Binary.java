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
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public class ORecordSerializerDocument2Binary implements ORecordSerializer {
	public static final String	NAME	= "ORecordDocument2binary";

	protected ORecordSchemaAware<?> newObject(ODatabaseRecord iDatabase, String iClassName) throws InstantiationException,
			IllegalAccessException {
		return new ODocument(iDatabase);
	}

	public ORecordInternal<?> fromStream(ODatabaseRecord iDatabase, byte[] iSource) {
		// TODO: HANDLE FACTORIES
		return fromStream(iSource, null);
	}

	public ORecordInternal<?> fromStream(byte[] iSource, ORecordInternal<?> iRecord) {
		ODocument record = (ODocument) iRecord;
		if (iRecord == null)
			record = new ODocument();

		ByteArrayInputStream stream = null;
		DataInputStream in = null;

		try {
			stream = new ByteArrayInputStream(iSource);
			in = new DataInputStream(stream);

			// UNMARSHALL ALL THE PROPERTIES
			Object value;
			int length;
			byte[] buffer;
			for (OProperty p : record.getSchemaClass().properties()) {
				value = null;

				switch (p.getType()) {
				case BINARY:
					length = in.readInt();
					if (length >= 0) {
						// != NULL
						buffer = new byte[length];
						in.readFully(buffer);
						value = buffer;
					}
					break;
				case BOOLEAN:
					value = in.readBoolean();
					break;
				case DATE:
				case DATETIME:
					long date = in.readLong();
					if (date > -1)
						value = new Date(date);
					break;
				case DOUBLE:
					value = in.readDouble();
					break;
				case EMBEDDED:
					length = in.readInt();
					if (length >= 0) {
						// != NULL
						buffer = new byte[length];
						in.readFully(buffer);
						value = new ODocument(p.getLinkedClass().getName()).fromStream(buffer);
					}
					break;
				case EMBEDDEDLIST:
					break;
				case EMBEDDEDSET:
					break;
				case FLOAT:
					value = in.readFloat();
					break;
				case INTEGER:
					value = in.readInt();
					break;
				case LINK:
					value = new ORecordId(in.readInt(), in.readInt());
					break;
				case LINKLIST:
					break;
				case LINKSET:
					break;
				case LONG:
					value = in.readLong();
					break;
				case SHORT:
					value = in.readShort();
					break;
				case STRING:
					value = in.readUTF();
					break;
				}
				record.field(p.getName(), value);
			}
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on unmarshalling object in binary format: " + record.getIdentity(), e);

		} finally {
			try {
				if (stream != null)
					stream.close();

				if (in != null)
					in.close();
			} catch (IOException e) {
			}
		}

		return iRecord;
	}

	public byte[] toStream(final ORecordInternal<?> iRecord, boolean iOnlyDelta) {
		ODocument record = (ODocument) iRecord;

		ByteArrayOutputStream stream = null;
		DataOutputStream out = null;

		try {
			stream = new ByteArrayOutputStream();
			out = new DataOutputStream(stream);

			// MARSHALL ALL THE PROPERTIES
			Object value;
			byte[] buffer;
			for (OProperty p : record.getSchemaClass().properties()) {
				value = record.field(p.getName());

				switch (p.getType()) {
				case BINARY:
					if (value == null)
						// NULL: WRITE -1 AS LENGTH
						out.writeInt(-1);
					else {
						buffer = (byte[]) value;
						out.writeInt(buffer.length);
						out.write(buffer);
					}
					break;
				case BOOLEAN:
					out.writeBoolean(value != null);
					if (value != null)
						out.writeBoolean((Boolean) value);
					break;
				case DATE:
				case DATETIME:
					out.writeLong(value != null ? ((Date) value).getTime() : -1);
					break;
				case DOUBLE:
					out.writeBoolean(value != null);
					if (value != null)
						out.writeDouble((Double) value);
					break;
				case EMBEDDED:
					if (value == null)
						// NULL: WRITE -1 AS LENGTH
						out.writeInt(-1);
					else {
						buffer = ((ORecordInternal<?>) value).toStream();
						out.writeInt(buffer.length);
						out.write(buffer);
					}
					break;
				case EMBEDDEDLIST:
					break;
				case EMBEDDEDSET:
					break;
				case FLOAT:
					out.writeBoolean(value != null);
					if (value != null)
						out.writeFloat((Float) value);
					break;
				case INTEGER:
					out.writeBoolean(value != null);
					if (value != null)
						out.writeInt((Integer) value);
					break;
				case LINK:
					out.writeBoolean(value != null);
					if (value != null) {
						if (!(value instanceof ORecord<?>))
							throw new ODatabaseException("Invalid property value in '" + p.getName() + "': found " + value.getClass()
									+ " while it was expected a ORecord");

						ORID rid = ((ORecord<?>) value).getIdentity();
						out.writeInt(rid.getClusterId());
						out.writeLong(rid.getClusterPosition());
					}
					break;
				case LINKLIST:
					break;
				case LINKSET:
					break;
				case LONG:
					out.writeBoolean(value != null);
					if (value != null)
						out.writeLong((Long) value);
					break;
				case SHORT:
					out.writeBoolean(value != null);
					if (value != null)
						out.writeShort((Short) value);
					break;
				case STRING:
					out.writeBoolean(value != null);
					if (value != null)
						out.writeUTF((String) value);
					break;
				}
			}
			return stream.toByteArray();
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on marshalling object in binary format: " + iRecord.getIdentity(), e);
		} finally {
			try {
				if (stream != null)
					stream.close();

				if (out != null)
					out.close();
			} catch (IOException e) {
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return NAME;
	}
}
