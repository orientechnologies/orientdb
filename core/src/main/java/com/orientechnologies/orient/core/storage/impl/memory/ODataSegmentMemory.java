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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.util.ArrayList;
import java.util.List;

public class ODataSegmentMemory {
	private final List<byte[]>	entries	= new ArrayList<byte[]>();

	public ODataSegmentMemory() {
	}

	public void close() {
		entries.clear();
	}

	public int count() {
		return entries.size();
	}

	public long getSize() {
		long size = 0;
		for (byte[] e : entries)
			size += e.length;

		return size;
	}

	public long createRecord(byte[] iContent) {
		entries.add(iContent);
		return entries.size() - 1;
	}

	public void deleteRecord(final long iRecordPosition) {
		entries.set((int) iRecordPosition, null);
	}

	public byte[] readRecord(final long iRecordPosition) {
		return entries.get((int) iRecordPosition);
	}

	public void updateRecord(final long iRecordPosition, final byte[] iContent) {
		entries.set((int) iRecordPosition, iContent);
	}
}
