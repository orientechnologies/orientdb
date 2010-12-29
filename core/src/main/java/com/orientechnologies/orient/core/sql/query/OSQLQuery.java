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
package com.orientechnologies.orient.core.sql.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * SQL query implementation.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 *          Record type to return.
 */
public abstract class OSQLQuery<T extends Object> extends OQueryAbstract<T> implements OCommandRequestText {
	protected String	text;

	public OSQLQuery() {
	}

	public OSQLQuery(final String iText) {
		text = iText.trim();
	}

	/**
	 * Delegates to the OQueryExecutor the query execution.
	 */
	@SuppressWarnings("unchecked")
	public List<T> run(final Object... iArgs) {
		setParameters(iArgs);
		return (List<T>) database.getStorage().command(this);
	}

	/**
	 * Returns only the first record if any.
	 */
	public T runFirst(final Object... iArgs) {
		setLimit(1);
		final List<T> result = execute(iArgs);
		return result != null && result.size() > 0 ? result.get(0) : null;
	}

	public String getText() {
		return text;
	}

	@Override
	public String toString() {
		return "OSQLQuery [text=" + text + "]";
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		final OMemoryInputStream buffer = new OMemoryInputStream(iStream);
		try {
			text = buffer.getAsString();
			limit = buffer.getAsInteger();
			beginRange = new ORecordId().fromStream(buffer.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			endRange = new ORecordId().fromStream(buffer.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			fetchPlan = buffer.getAsString();
			if (fetchPlan.length() == 0)
				fetchPlan = null;

			byte[] paramBuffer = buffer.getAsByteArray();

			if (paramBuffer.length == 0)
				parameters = null;
			else {
				final ODocument param = new ODocument();
				param.fromStream(paramBuffer);

				Map<String, Object> params = param.field("params");

				parameters = new HashMap<Object, Object>();
				for (Entry<String, Object> p : params.entrySet()) {
					if (Character.isDigit(p.getKey().charAt(0)))
						parameters.put(Integer.parseInt(p.getKey()), p.getValue());
					else
						parameters.put(p.getKey(), p.getValue());
				}
			}
		} catch (IOException e) {
			throw new OSerializationException("Error while unmarshalling OSQLQuery", e);
		}
		return this;
	}

	public byte[] toStream() throws OSerializationException {
		final OMemoryOutputStream buffer = new OMemoryOutputStream();
		try {
			buffer.add(text);
			buffer.add(limit);
			buffer.addAsFixed(beginRange.toStream());
			buffer.addAsFixed(endRange.toStream());
			buffer.add(fetchPlan != null ? fetchPlan : "");

			if (parameters == null || parameters.size() == 0)
				buffer.add(new byte[0]);
			else {
				final ODocument param = new ODocument();
				param.field("params", parameters);
				buffer.add(param.toStream());
			}

		} catch (IOException e) {
			throw new OSerializationException("Error while marshalling OSQLQuery", e);
		}

		return buffer.toByteArray();
	}
}
