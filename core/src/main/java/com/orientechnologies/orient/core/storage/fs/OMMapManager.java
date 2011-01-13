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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OMMapManager {
	public enum OPERATION_TYPE {
		READ, WRITE
	}

	private static final long															MIN_MEMORY				= 50000000;
	public static final int																DEF_BLOCK_SIZE;
	private static final int															FORCE_DELAY;
	private static final int															FORCE_RETRY;

	private static long																		maxMemory;
	private static long																		totalMemory;

	private static List<OMMapBufferEntry>									bufferPoolLRU			= new ArrayList<OMMapBufferEntry>();
	private static Map<OFileMMap, List<OMMapBufferEntry>>	bufferPoolPerFile	= new HashMap<OFileMMap, List<OMMapBufferEntry>>();

	static {
		DEF_BLOCK_SIZE = OGlobalConfiguration.FILE_MMAP_BLOCK_SIZE.getValueAsInteger();
		FORCE_DELAY = OGlobalConfiguration.FILE_MMAP_FORCE_DELAY.getValueAsInteger();
		FORCE_RETRY = OGlobalConfiguration.FILE_MMAP_FORCE_RETRY.getValueAsInteger();

		maxMemory = OGlobalConfiguration.FILE_MMAP_MAX_MEMORY.getValueAsLong();
	}

	public static OMMapBufferEntry request(final OFileMMap iFile, final int iBeginOffset, final int iSize, final OPERATION_TYPE iOperationType) {
		return request(iFile, iBeginOffset, iSize, false, iOperationType);
	}

	/**
	 * Requests a mmap buffer to use.
	 * 
	 * @param iFile
	 *          MMap file
	 * @param iBeginOffset
	 *          Begin offset
	 * @param iSize
	 *          Portion size requested
	 * @param iForce
	 *          Tells if the size is mandatory or can be rounded to the next segment
	 * @param iOperationType
	 *          READ or WRITE
	 * @return The mmap buffer entry if found, or null if the operation is READ and the buffer pool is full.
	 */
	public synchronized static OMMapBufferEntry request(final OFileMMap iFile, final int iBeginOffset, final int iSize, final boolean iForce,
			final OPERATION_TYPE iOperationType) {

		if (bufferPoolLRU.size() > 0) {
			// SEARCH IF IT'S BETWEEN THE LAST 10 BLOCK USED: THIS IS THE COMMON CASE ON MASSIVE INSERTION
			OMMapBufferEntry e;
			final int min = Math.max(bufferPoolLRU.size() - 10, -1);
			for (int i = bufferPoolLRU.size() - 1; i > min; --i) {
				e = bufferPoolLRU.get(i);

				if (e.file == iFile && iBeginOffset >= e.beginOffset && iBeginOffset + iSize <= e.beginOffset + e.size) {
					// FOUND: USE IT
					OProfiler.getInstance().updateCounter("OMMapManager.usePage", 1);
					e.counter++;
					return e;
				}
			}
		}

		// SEARCH THE REQUESTED RANGE IN THE CACHED BUFFERS
		List<OMMapBufferEntry> fileEntries = bufferPoolPerFile.get(iFile);
		if (fileEntries == null) {
			fileEntries = new ArrayList<OMMapBufferEntry>();
			bufferPoolPerFile.put(iFile, fileEntries);
		}

		int position = searchEntry(fileEntries, iBeginOffset, iSize);
		if (position > -1)
			// FOUND
			return fileEntries.get(position);

		int bufferSize = iForce ? iSize : iSize <= DEF_BLOCK_SIZE ? DEF_BLOCK_SIZE : iSize;
		if (iBeginOffset + bufferSize > iFile.getFileSize())
			// REQUESTED BUFFER IS TOO LARGE: GET AS MAXIMUM AS POSSIBLE
			bufferSize = iFile.getFileSize() - iBeginOffset;

		if (bufferSize <= 0)
			throw new IllegalArgumentException("Invalid range requested for file " + iFile + ". Requested " + iSize + " bytes from the address "
					+ iBeginOffset + " while the total file size is " + iFile.getFileSize());

		totalMemory += bufferSize;

		// FREE LESS-USED BUFFERS UNTIL THE FREE-MEMORY IS DOWN THE CONFIGURED MAX LIMIT
		OMMapBufferEntry entry = null;
		boolean forceSucceed;
		do {
			if (totalMemory > maxMemory) {
				int pagesUnloaded = 0;

				// SORT AS LRU, FIRT = MOST USED
				Collections.sort(bufferPoolLRU, new Comparator<OMMapBufferEntry>() {
					public int compare(final OMMapBufferEntry o1, final OMMapBufferEntry o2) {
						return (int) (o1.counter - o2.counter);
					}
				});

				// REMOVE THE LESS USED ENTRY AND UPDATE THE TOTAL MEMORY
				for (Iterator<OMMapBufferEntry> it = bufferPoolLRU.iterator(); it.hasNext();) {
					entry = it.next();
					if (!entry.pin) {
						// FORCE THE WRITE OF THE BUFFER
						forceSucceed = false;
						for (int i = 0; i < FORCE_RETRY; ++i) {
							try {
								entry.buffer.force();
								forceSucceed = true;
								break;
							} catch (Exception e) {
								OLogManager.instance().debug(entry.buffer, "Can't write memory buffer to disk. Retrying (" + (i + 1) + "/" + FORCE_RETRY + ")...");
								try {
									System.gc();
									Thread.sleep(FORCE_DELAY);
								} catch (InterruptedException e1) {
								}
							}
						}

						if (!forceSucceed)
							entry.buffer.force();

						// REMOVE FROM COLLECTIONS
						it.remove();
						bufferPoolPerFile.get(entry.file).remove(entry);

						pagesUnloaded++;

						entry.buffer = null;

						totalMemory -= entry.size;

						if (totalMemory < maxMemory)
							break;
					}
				}

				OProfiler.getInstance().updateCounter("OMMapManager.pagesUnloaded", pagesUnloaded);

				// RECOMPUTE THE POSITION AFTER REMOVING
				position = searchEntry(fileEntries, iBeginOffset, iSize);
			}

			// LOAD THE PAGE
			try {
				entry = mapBuffer(iFile, iBeginOffset, bufferSize);
			} catch (Exception e) {
				// REDUCE MAX MEMORY TO FORCE EMPTY BUFFERS
				maxMemory = maxMemory * 90 / 100;
				OLogManager.instance().warn(OMMapManager.class, "Memory mapping error, try to reduce max memory to %d and retry...", maxMemory);
			}
		} while (entry == null && maxMemory > MIN_MEMORY);

		if (entry == null)
			throw new OIOException("You can't access to the file portion " + iBeginOffset + "-" + iBeginOffset + iSize + " bytes");

		bufferPoolLRU.add(entry);
		fileEntries.add((position + 1) * -1, entry);

		return entry;
	}

	/**
	 * Flush away all the buffers of file closed. This frees the memory.
	 */
	public synchronized static void flush() {
		OMMapBufferEntry entry;
		for (Iterator<OMMapBufferEntry> it = bufferPoolLRU.iterator(); it.hasNext();) {
			entry = it.next();
			if (entry.file != null && entry.file.isClosed()) {
				totalMemory -= entry.size;

				bufferPoolPerFile.get(entry.file).remove(entry);
				it.remove();

				entry.close();
			}
		}
	}

	public synchronized static void shutdown() {
		for (OMMapBufferEntry entry : new ArrayList<OMMapBufferEntry>(bufferPoolLRU)) {
			entry.close();
		}
		bufferPoolLRU.clear();
		bufferPoolLRU = null;
		bufferPoolPerFile.clear();
		bufferPoolPerFile = null;
		totalMemory = 0;
	}

	public static long getMaxMemory() {
		return maxMemory;
	}

	public static void setMaxMemory(final long iMaxMemory) {
		maxMemory = iMaxMemory;
	}

	public static long getTotalMemory() {
		return totalMemory;
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

	/**
	 * Search for a buffer in the ordered list.
	 * 
	 * @param fileEntries
	 * @param iBeginOffset
	 * @param iSize
	 * @return negative number means not found. The position to insert is the (return value +1)*-1. Zero or positive number is the
	 *         found position.
	 */
	private static int searchEntry(final List<OMMapBufferEntry> fileEntries, final int iBeginOffset, final int iSize) {
		OMMapBufferEntry e;

		// BINARY SEARCH
		int low = 0;
		int high = fileEntries.size() - 1;
		int mid = -1;

		while (low <= high) {
			mid = (low + high) >>> 1;
			e = fileEntries.get(mid);

			if (iBeginOffset >= e.beginOffset && iBeginOffset + iSize <= e.beginOffset + e.size) {
				// FOUND: USE IT
				OProfiler.getInstance().updateCounter("OMMapManager.usePage", 1);
				e.counter++;
				return mid;
			}

			if (low == high)
				return (low + 2) * -1;

			if (iBeginOffset >= e.beginOffset)
				low = mid + 1;
			else
				high = mid;
		}

		return mid;
	}
}
