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
package com.orientechnologies.orient.core.record.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerRaw;

/**
 * The rawest representation of a record. It's schema less. Use this if you need to store Strings or byte[] without matter about the
 * content. Useful also to store multimedia contents and binary files. The object can be reused across calls to the database by
 * using the reset() at every re-use.
 */
@SuppressWarnings({ "unchecked", "serial" })
public class ORecordBytes extends ORecordAbstract<byte[]> {

	public static final byte	RECORD_TYPE	= 'b';

	public ORecordBytes() {
		setup();
	}

	public ORecordBytes(final ODatabaseRecord iDatabase) {
		setup();
		ODatabaseRecordThreadLocal.INSTANCE.set(iDatabase);
	}

	public ORecordBytes(final ODatabaseRecord iDatabase, final byte[] iSource) {
		this(iSource);
		ODatabaseRecordThreadLocal.INSTANCE.set(iDatabase);
	}

	public ORecordBytes(final byte[] iSource) {
		super(iSource);
		_dirty = true;
		setup();
	}

	public ORecordBytes(final ORID iRecordId) {
		_recordId = (ORecordId) iRecordId;
		setup();
	}

	public ORecordBytes reset(final byte[] iSource) {
		reset();
		_source = iSource;
		return this;
	}

	public ORecordBytes copy() {
		return (ORecordBytes) copyTo(new ORecordBytes());
	}

	@Override
	public ORecordBytes fromStream(final byte[] iRecordBuffer) {
		_source = iRecordBuffer;
		_status = ORecordElement.STATUS.LOADED;
		return this;
	}

	@Override
	public byte[] toStream() {
		return _source;
	}

	public byte getRecordType() {
		return RECORD_TYPE;
	}

	@Override
	protected void setup() {
		super.setup();
		_recordFormat = ORecordSerializerFactory.instance().getFormat(ORecordSerializerRaw.NAME);
	}

	/**
	 * Reads the input stream in memory. This is less efficient than {@link #fromInputStream(InputStream, int)} because allocation is
	 * made multiple times. If you already know the input size use {@link #fromInputStream(InputStream, int)}.
	 * 
	 * @param in
	 *          Input Stream, use buffered input stream wrapper to speed up reading
	 * @return Buffer read from the stream. It's also the internal buffer size in bytes
	 * @throws IOException
	 */
	public int fromInputStream(final InputStream in) throws IOException {
		final OMemoryStream out = new OMemoryStream();
		try {
			int b;
			while ((b = in.read()) > -1)
				out.write(b);
			out.flush();
			_source = out.toByteArray();
		} finally {
			out.close();
		}
		_size = _source.length;
		return _size;
	}

	/**
	 * Reads the input stream in memory specifying the maximum bytes to read. This is more efficient than
	 * {@link #fromInputStream(InputStream)} because allocation is made only once. If input stream contains less bytes than total size
	 * parameter, the rest of content will be empty (filled to 0)
	 * 
	 * @param in
	 *          Input Stream, use buffered input stream wrapper to speed up reading
	 * @param iMaxSize
	 *          Maximin size to read
	 * @return Buffer read from the stream. It's also the internal buffer size in bytes
	 * @throws IOException
	 */
	public int fromInputStream(final InputStream in, final int iMaxSize) throws IOException {
		final int bufferSize = Math.min(in.available(), iMaxSize);
		_source = new byte[bufferSize];
		in.read(_source);
		_size = bufferSize;
		return _size;
	}

	public void toOutputStream(final OutputStream out) throws IOException {
		checkForLoading();

		if (_source.length > 0)
			out.write(_source);
	}
}
