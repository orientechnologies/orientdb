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

import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * Extension of ORecordBytes that handle lazy serialization and converts temporary links (record id in transactions) to finals.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "unchecked", "serial" })
public class ORecordBytesLazy extends ORecordBytes {
	private OSerializableStream	serializableContent;

	public ORecordBytesLazy(final OSerializableStream iSerializable) {
		this.serializableContent = iSerializable;
	}

	@Override
	public byte[] toStream() {
		if (_source == null)
			_source = serializableContent.toStream();
		return _source;
	}

	@Override
	public ORecordBytesLazy copy() {
		return (ORecordBytesLazy) copyTo(new ORecordBytesLazy(serializableContent));
	}

	public OSerializableStream getSerializableContent() {
		return serializableContent;
	}

	public void recycle(final OSerializableStream iSerializableContent) {
		this.serializableContent = iSerializableContent;
	}
}
