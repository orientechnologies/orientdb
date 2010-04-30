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
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;

/**
 * SQL query implementation.
 * 
 * @author luca
 * 
 * @param <T>
 *          Record type to return.
 */
public abstract class OSQLQuery<T extends ORecordSchemaAware<?>> extends OQueryAbstract<T> implements OSerializableStream {
	protected String				text;

	protected OSQLFilter		compiledFilter;
	protected List<String>	projections	= new ArrayList<String>();

	public OSQLQuery() {
	}

	public OSQLQuery(final String iText) {
		text = iText;
	}

	/**
	 * Delegates to the OQueryExecutor the query execution.
	 */
	public List<T> execute(final int iLimit) {
		limit = iLimit;
		return database.getStorage().getCommandExecutor().execute(this, iLimit);
	}

	/**
	 * Delegates to the OQueryExecutor the query execution.
	 */
	public T executeFirst() {
		return database.getStorage().getCommandExecutor().executeFirst(this);
	}

	public List<String> getProjections() {
		return projections;
	}

	/**
	 * Compile the filter conditions only the first time.
	 */
	public void parse() {
		if (compiledFilter == null) {
			int pos = extractProjections();
			if (pos == -1)
				return;

			compiledFilter = new OSQLFilter(text.substring(pos));
		}
	}

	protected int extractProjections() {
		final String textUpperCase = text.toUpperCase();

		int currentPos = 0;

		StringBuilder word = new StringBuilder();

		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
		if (!word.toString().equals(OSQLHelper.KEYWORD_SELECT))
			return -1;

		int fromPosition = textUpperCase.indexOf(OSQLHelper.KEYWORD_FROM, currentPos);
		if (fromPosition == -1)
			throw new OQueryParsingException("Missed " + OSQLHelper.KEYWORD_FROM, text, currentPos);

		String[] items = textUpperCase.substring(currentPos, fromPosition).split(",");
		if (items == null || items.length == 0)
			throw new OQueryParsingException("No projections found between " + OSQLHelper.KEYWORD_SELECT + " and "
					+ OSQLHelper.KEYWORD_FROM, text, currentPos);

		for (String i : items)
			projections.add(i.trim());

		currentPos = fromPosition + OSQLHelper.KEYWORD_FROM.length() + 1;

		return currentPos;
	}

	public String getText() {
		return text;
	}

	@Override
	public String toString() {
		return "OSQLQuery [text=" + text + "]";
	}

	public OSerializableStream fromStream(final byte[] iStream) throws IOException {
		text = OBinaryProtocol.bytes2string(iStream);
		return this;
	}

	public byte[] toStream() throws IOException {
		return OBinaryProtocol.string2bytes(text);
	}
}
