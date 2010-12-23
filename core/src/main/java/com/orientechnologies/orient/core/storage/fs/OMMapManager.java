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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OMMapManager {
	public static final int											DEF_BLOCK_SIZE;
	private static final int										MAX_MEMORY;
	private static final int										FORCE_DELAY;
	private static final int										FORCE_RETRY;

	private static int													totalMemory;

	private static LinkedList<OMMapBufferEntry>	buffersLRU	= new LinkedList<OMMapBufferEntry>();

	static {
		DEF_BLOCK_SIZE = OGlobalConfiguration.FILE_MMAP_BLOCK_SIZE.getValueAsInteger();
		MAX_MEMORY = OGlobalConfiguration.FILE_MMAP_MAX_MEMORY.getValueAsInteger();
		FORCE_DELAY = OGlobalConfiguration.FILE_MMAP_FORCE_DELAY.getValueAsInteger();
		FORCE_RETRY = OGlobalConfiguration.FILE_MMAP_FORCE_RETRY.getValueAsInteger();
	}

	public static OMMapBufferEntry request(final OFileMMap iFile, final int iBeginOffset, final int iSize) {
		return request(iFile, iBeginOffset, iSize, false);
	}

	public synchronized static OMMapBufferEntry request(final OFileMMap iFile, final int iBeginOffset, final int iSize,
			final boolean iForce) {
		try {
			// SEARCH THE REQUESTED RANGE IN CACHED BUFFERS
			for (OMMapBufferEntry e : buffersLRU) {
				if (iFile.equals(e.file) && iBeginOffset >= e.beginOffset && iBeginOffset + iSize <= e.beginOffset + e.size) {
					OProfiler.getInstance().updateCounter("OMMapManager.usePage", 1);
					// FOUND: USE IT
					return e;
				}
			}

			int bufferSize = iForce ? iSize : iSize <= DEF_BLOCK_SIZE ? DEF_BLOCK_SIZE : iSize;
			if (iBeginOffset + bufferSize > iFile.getFileSize())
				// REQUESTED BUFFER IS TOO LARGE: GET AS MAXIMUM AS POSSIBLE
				bufferSize = iFile.getFileSize() - iBeginOffset;

			if (bufferSize <= 0)
				throw new IllegalArgumentException("Invalid range requested for file " + iFile + ". Requested " + iSize
						+ " bytes from the address " + iBeginOffset + " while the total file size is " + iFile.getFileSize());

			totalMemory += bufferSize;

			// FREE LESS-USED BUFFERS UNTIL THE FREE-MEMORY IS DOWN THE CONFIGURED MAX LIMIT
			OMMapBufferEntry entry;
			if (totalMemory > MAX_MEMORY) {
				int pagesUnloaded = 0;

				// REMOVE THE LAST ENTRY AND UPDATE THE TOTAL MEMORY
				for (Iterator<OMMapBufferEntry> it = buffersLRU.descendingIterator(); it.hasNext();) {
					entry = it.next();
					if (!entry.pin) {
						// FORCE THE WRITE OF THE BUFFER
						for (int i = 0; i < FORCE_RETRY; ++i) {
							try {
								entry.buffer.force();
							} catch (Exception e) {
								OLogManager.instance().debug(entry.buffer,
										"Can't write memory buffer to disk. Retrying (" + (i + 1) + "/" + FORCE_RETRY + ")...");
								try {
									System.gc();
									Thread.sleep(FORCE_DELAY);
								} catch (InterruptedException e1) {
								}
							}
						}
						entry.buffer.force();

						it.remove();
						pagesUnloaded++;

						entry.buffer = null;

						totalMemory -= entry.size;

						if (totalMemory < MAX_MEMORY)
							break;
					}
				}

				OProfiler.getInstance().updateCounter("OMMapManager.pagesUnloaded", pagesUnloaded);
			}

			// LOAD THE PAGE
			entry = mapBuffer(iFile, iBeginOffset, bufferSize);
			buffersLRU.addFirst(entry);

			return entry;

		} catch (IOException e) {
			throw new OIOException("You can't access to the file portion " + iBeginOffset + "-" + iBeginOffset + iSize + " bytes", e);
		}
	}

	/**
	 * Flush away all the buffers of file closed. This frees the memory.
	 */
	public synchronized static void flush() {
		OMMapBufferEntry entry;
		for (Iterator<OMMapBufferEntry> it = buffersLRU.iterator(); it.hasNext();) {
			entry = it.next();
			if (entry.file != null && entry.file.isClosed()) {
				totalMemory -= entry.size;
				entry.close();
				it.remove();
			}
		}
	}

	public synchronized static void shutdown() {
		for (OMMapBufferEntry entry : new ArrayList<OMMapBufferEntry>(buffersLRU)) {
			entry.close();
		}
		buffersLRU.clear();
		buffersLRU = null;
		totalMemory = 0;
	}

	private static OMMapBufferEntry mapBuffer(final OFileMMap iFile, final int iBeginOffset, final int iSize) throws IOException {
		OProfiler.getInstance().updateCounter("OMMapManager.loadPage", 1);
		long timer = OProfiler.getInstance().startChrono();
		try {
			return new OMMapBufferEntry(iFile, iFile.map(iBeginOffset, iSize), iBeginOffset, iSize);
		} finally {
			OProfiler.getInstance().stopChrono("OMMapManager.loadPage", timer);
		}
	}
}
