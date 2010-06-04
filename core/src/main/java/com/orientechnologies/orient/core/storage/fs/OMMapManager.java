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
package com.orientechnologies.orient.core.storage.fs;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.exception.OIOException;

public class OMMapManager {
	public static final int									DEF_BLOCK_SIZE	= 1500000;
	private static final int								MAX_MEMORY			= 100000000;

	private static int											totalMemory;

	private static ArrayDeque<OMMapBufferEntry>	buffersLRU			= new ArrayDeque<OMMapBufferEntry>();

	public static OMMapBufferEntry request(final OFileMMap iFile, final int iBeginOffset, final int iSize) {
		return request(iFile, iBeginOffset, iSize, false);
	}

	public synchronized static OMMapBufferEntry request(final OFileMMap iFile, final int iBeginOffset, final int iSize,
			final boolean iForce) {
		try {
			// SEARCH THE REQUESTED RANGE IN CACHED BUFFERS
			for (OMMapBufferEntry e : buffersLRU) {
				if (iFile.equals(e.file) && iBeginOffset >= e.beginOffset && iBeginOffset + iSize <= e.beginOffset + e.size) {
					OProfiler.getInstance().updateStatistic("OMMapManager.usePage", 1);
					// FOUND: USE IT
					return e;
				}
			}

			int bufferSize = iForce ? iSize : iSize <= DEF_BLOCK_SIZE ? DEF_BLOCK_SIZE : iSize;
			if (iBeginOffset + bufferSize > iFile.getFileSize())
				bufferSize = iFile.getFileSize() - iBeginOffset;

			if (bufferSize <= 0)
				throw new IllegalArgumentException("Invalid range requested for file " + iFile + ". Requested " + iSize
						+ " bytes from the address: " + iBeginOffset);

			totalMemory += bufferSize;

			// FREE LESS-USED BUFFERS UNTIL THE FREE-MEMORY IS DOWN THE CONFIGURED MAX LIMIT
			OMMapBufferEntry entry;
			if (totalMemory > MAX_MEMORY) {
				int pagesUnloaded = 0;

				// REMOVE THE LAST ENTRY AND UPDATE THE TOTAL MEMORY
				for (Iterator<OMMapBufferEntry> it = buffersLRU.descendingIterator(); it.hasNext();) {
					entry = it.next();
					if (!entry.pin) {
						it.remove();
						pagesUnloaded++;

						entry.buffer.force();
						entry.buffer = null;

						totalMemory -= entry.size;

						if (totalMemory < MAX_MEMORY)
							break;
					}
				}

				OProfiler.getInstance().updateStatistic("OMMapManager.pagesUnloaded", pagesUnloaded);
			}

			// LOAD THE PAGE
			entry = mapBuffer(iFile, iBeginOffset, bufferSize);
			buffersLRU.push(entry);

			return entry;

		} catch (IOException e) {
			throw new OIOException("You can't access to the file portion " + iBeginOffset + "-" + iBeginOffset + iSize + " bytes", e);
		}
	}

	static OMMapBufferEntry mapBuffer(final OFileMMap iFile, final int iBeginOffset, final int iSize) throws IOException {
		OProfiler.getInstance().updateStatistic("OMMapManager.loadPage", 1);
		long timer = OProfiler.getInstance().startChrono();
		try {
			return new OMMapBufferEntry(iFile, iFile.map(iBeginOffset, iSize), iBeginOffset, iSize);
		} finally {
			OProfiler.getInstance().stopChrono("OMMapManager.loadPage", timer);
		}
	}

	public static void close() {
	}
}
