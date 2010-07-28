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
package com.orientechnologies.orient.core.serialization.serializer;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

@SuppressWarnings("unchecked")
public class OJSONWriter {
	private Writer	out;
	private boolean	prettyPrint			= true;
	private boolean	firstAttribute	= true;

	public OJSONWriter(final Writer out) {
		this.out = out;
	}

	public OJSONWriter beginObject() throws IOException {
		beginObject(0, false, null);
		return this;
	}

	public OJSONWriter beginObject(final int iIdentLevel) throws IOException {
		beginObject(iIdentLevel, false, null);
		return this;
	}

	public OJSONWriter beginObject(final Object iName) throws IOException {
		beginObject(0, false, iName);
		return this;
	}

	public OJSONWriter beginObject(final int iIdentLevel, final boolean iNewLine, final Object iName) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		if (iName != null)
			out.append("\"" + iName.toString() + "\":");

		out.append('{');

		firstAttribute = true;
		return this;
	}

	public OJSONWriter endObject() throws IOException {
		format(0, true);
		out.append('}');
		return this;
	}

	public OJSONWriter endObject(final int iIdentLevel) throws IOException {
		return endObject(iIdentLevel, true);
	}

	public OJSONWriter endObject(final int iIdentLevel, final boolean iNewLine) throws IOException {
		format(iIdentLevel, iNewLine);
		out.append('}');
		firstAttribute = false;
		return this;
	}

	public OJSONWriter beginCollection(final int iIdentLevel, final boolean iNewLine, final String iName) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		out.append(writeValue(iName));
		out.append(": [");

		firstAttribute = true;
		return this;
	}

	public OJSONWriter endCollection(final int iIdentLevel, final boolean iNewLine) throws IOException {
		format(iIdentLevel, iNewLine);
		firstAttribute = false;
		out.append(']');
		return this;
	}

	public void writeObjects(int iIdentLevel, boolean iNewLine, String iName, Object[]... iPairs) throws IOException {
		for (int i = 0; i < iPairs.length; ++i) {
			beginObject(iIdentLevel, true, iName);
			for (int k = 0; k < iPairs[i].length;) {
				writeAttribute(iIdentLevel + 1, false, (String) iPairs[i][k++], iPairs[i][k++]);
			}
			endObject(iIdentLevel, false);
		}
	}

	public OJSONWriter writeAttribute(final int iIdentLevel, final boolean iNewLine, final String iName, final Object iValue)
			throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		out.append(writeValue(iName));
		out.append(": ");
		out.append(writeValue(iValue));

		firstAttribute = false;
		return this;
	}

	public OJSONWriter writeValue(final int iIdentLevel, final boolean iNewLine, final Object iValue) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		out.append(writeValue(iValue));

		firstAttribute = false;
		return this;
	}

	public static String writeValue(final Object iValue) throws IOException {
		StringBuilder buffer = new StringBuilder();

		if (iValue == null)
			buffer.append("\"null\"");

		else if (iValue instanceof ORecordId) {
			ORecordId rid = (ORecordId) iValue;
			buffer.append("\"");
			buffer.append(rid.toString());
			buffer.append('\"');

		} else if (iValue instanceof ORecord<?>) {
			ORecord<?> linked = (ORecord<?>) iValue;
			if (linked.getIdentity().isValid()) {
				buffer.append("\"#");
				buffer.append(linked.getIdentity().toString());
				buffer.append('\"');
			} else {
				buffer.append(linked.toJSON("id,ver,class,ident:6"));

			}

		} else if (iValue.getClass().isArray()) {

			if (iValue instanceof byte[]) {
				buffer.append('\"');
				byte[] source = (byte[]) iValue;

				String encoded = OStringSerializerHelper.encode(OBase64Utils.encodeBytes(source));
				buffer.append(encoded);

				System.out.println(source.length + "--> " + encoded.length());

				buffer.append('\"');
			} else {
				buffer.append('[');
				for (int i = 0; i < Array.getLength(iValue); ++i) {
					if (i > 0)
						buffer.append(", ");
					buffer.append(writeValue(Array.get(iValue, i)));
				}
				buffer.append(']');
			}

		} else if (iValue instanceof Collection<?>) {
			Collection<Object> coll = (Collection<Object>) iValue;
			buffer.append('[');
			int i = 0;
			for (Iterator<Object> it = coll.iterator(); it.hasNext(); ++i) {
				if (i > 0)
					buffer.append(", ");
				buffer.append(writeValue(it.next()));
			}
			buffer.append(']');

		} else if (iValue instanceof Map<?, ?>) {
			Map<Object, Object> map = (Map<Object, Object>) iValue;
			buffer.append('{');
			int i = 0;
			Entry<Object, Object> entry;
			for (Iterator<Entry<Object, Object>> it = map.entrySet().iterator(); it.hasNext(); ++i) {
				entry = it.next();
				if (i > 0)
					buffer.append(", ");
				buffer.append(writeValue(entry.getKey()));
				buffer.append(": ");
				buffer.append(writeValue(entry.getValue()));
			}
			buffer.append('}');

		} else if (iValue instanceof String || iValue instanceof Date) {
			String v = iValue.toString();
			if (v.startsWith("\""))
				buffer.append(v);
			else {
				buffer.append('"');
				buffer.append(v);
				buffer.append('"');
			}
		} else
			buffer.append(iValue.toString());

		return buffer.toString();
	}

	public OJSONWriter flush() throws IOException {
		out.flush();
		return this;
	}

	public OJSONWriter close() throws IOException {
		out.close();
		return this;
	}

	private OJSONWriter format(final int iIdentLevel, final boolean iNewLine) throws IOException {
		if (iNewLine) {
			out.append('\n');

			if (prettyPrint)
				for (int i = 0; i < iIdentLevel; ++i)
					out.append("  ");
		}
		return this;
	}

	public OJSONWriter append(final String iText) throws IOException {
		out.append(iText);
		return this;
	}

	public boolean isPrettyPrint() {
		return prettyPrint;
	}

	public OJSONWriter setPrettyPrint(boolean prettyPrint) {
		this.prettyPrint = prettyPrint;
		return this;
	}

	public void write(final String iText) throws IOException {
		out.append(iText);
	}
}
