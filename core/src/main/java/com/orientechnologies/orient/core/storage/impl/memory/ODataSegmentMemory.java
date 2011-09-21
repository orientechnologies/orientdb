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

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;

public class ODataSegmentMemory extends OSharedResourceAbstract {
	private final List<byte[]>	entries	= new ArrayList<byte[]>();

	public ODataSegmentMemory() {
	}

	public void close() {
		acquireExclusiveLock();
		try {

			entries.clear();

		} finally {
			releaseExclusiveLock();
		}
	}

	public int count() {
		acquireSharedLock();
		try {

			return entries.size();

		} finally {
			releaseSharedLock();
		}
	}

	public long getSize() {
		acquireSharedLock();
		try {

			long size = 0;
			for (byte[] e : entries)
				if (e != null)
					size += e.length;

			return size;

		} finally {
			releaseSharedLock();
		}
	}

	public long createRecord(byte[] iContent) {
		acquireExclusiveLock();
		try {

			entries.add(iContent);
			return entries.size() - 1;

		} finally {
			releaseExclusiveLock();
		}
	}

	public void deleteRecord(final long iRecordPosition) {
		acquireExclusiveLock();
		try {

			entries.set((int) iRecordPosition, null);

		} finally {
			releaseExclusiveLock();
		}
	}

	public byte[] readRecord(final long iRecordPosition) {
		acquireSharedLock();
		try {

			return entries.get((int) iRecordPosition);

		} finally {
			releaseSharedLock();
		}
	}

	public void updateRecord(final long iRecordPosition, final byte[] iContent) {
		acquireExclusiveLock();
		try {

			entries.set((int) iRecordPosition, iContent);

		} finally {
			releaseExclusiveLock();
		}
	}
}
