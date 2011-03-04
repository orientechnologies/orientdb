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
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OMMapManager {
	public enum OPERATION_TYPE {
		READ, WRITE
	}

	public enum STRATEGY {
		MMAP_ALWAYS, MMAP_WRITE_ALWAYS_READ_IF_AVAIL_POOL, MMAP_WRITE_ALWAYS_READ_IF_IN_MEM, MMAP_ONLY_AVAIL_POOL, MMAP_NEVER
	}

	private static final long															MIN_MEMORY				= 50000000;
	private static final int															FORCE_DELAY;
	private static final int															FORCE_RETRY;

	private static STRATEGY																lastStrategy;
	private static int																		blockSize;
	private static long																		maxMemory;
	private static long																		totalMemory;

	private static List<OMMapBufferEntry>									bufferPoolLRU			= new ArrayList<OMMapBufferEntry>();
	private static Map<OFileMMap, List<OMMapBufferEntry>>	bufferPoolPerFile	= new HashMap<OFileMMap, List<OMMapBufferEntry>>();

	static {
		blockSize = OGlobalConfiguration.FILE_MMAP_BLOCK_SIZE.getValueAsInteger();
		FORCE_DELAY = OGlobalConfiguration.FILE_MMAP_FORCE_DELAY.getValueAsInteger();
		FORCE_RETRY = OGlobalConfiguration.FILE_MMAP_FORCE_RETRY.getValueAsInteger();
		maxMemory = OGlobalConfiguration.FILE_MMAP_MAX_MEMORY.getValueAsLong();

		OProfiler.getInstance().registerHookValue("mmap.totalMemory", new OProfilerHookValue() {
			public Object getValue() {
				return totalMemory;
			}
		});

		OProfiler.getInstance().registerHookValue("mmap.maxMemory", new OProfilerHookValue() {
			public Object getValue() {
				return maxMemory;
			}
		});

		OProfiler.getInstance().registerHookValue("mmap.blockSize", new OProfilerHookValue() {
			public Object getValue() {
				return blockSize;
			}
		});

		OProfiler.getInstance().registerHookValue("mmap.blocks", new OProfilerHookValue() {
			public Object getValue() {
				return bufferPoolLRU.size();
			}
		});

		OProfiler.getInstance().registerHookValue("mmap.strategy", new OProfilerHookValue() {
			public Object getValue() {
				return lastStrategy;
			}
		});
	}

	public static OMMapBufferEntry request(final OFileMMap iFile, final long iBeginOffset, final int iSize,
			final OPERATION_TYPE iOperationType, final STRATEGY iStrategy) {
		return request(iFile, iBeginOffset, iSize, false, iOperationType, iStrategy);
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
	 * @param iStrategy
	 * @return The mmap buffer entry if found, or null if the operation is READ and the buffer pool is full.
	 */
	public synchronized static OMMapBufferEntry request(final OFileMMap iFile, final long iBeginOffset, final int iSize,
			final boolean iForce, final OPERATION_TYPE iOperationType, final STRATEGY iStrategy) {

		if (iStrategy == STRATEGY.MMAP_NEVER)
			return null;

		lastStrategy = iStrategy;

		if (bufferPoolLRU.size() > 0) {
			// SEARCH IF IT'S BETWEEN THE LAST 5 BLOCK USED: THIS IS THE COMMON CASE ON MASSIVE INSERTION
			OMMapBufferEntry e;
			final int min = Math.max(bufferPoolLRU.size() - 5, -1);
			for (int i = bufferPoolLRU.size() - 1; i > min; --i) {
				e = bufferPoolLRU.get(i);

				if (e.file == iFile && iBeginOffset >= e.beginOffset && iBeginOffset + iSize <= e.beginOffset + e.size) {
					// FOUND: USE IT
					OProfiler.getInstance().updateCounter("OMMapManager.reusedPageBetweenLast", 1);
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

		if (iOperationType == OPERATION_TYPE.READ && iStrategy == STRATEGY.MMAP_WRITE_ALWAYS_READ_IF_IN_MEM) {
			// READ NOT IN BUFFER POOL: RETURN NULL TO LET TO THE CALLER TO EXECUTE A DIRECT READ WITHOUT MMAP
			final int p = (position + 2) * -1;
			if (p > -1 && p <= fileEntries.size()) {
				// CHECK IF THERE IS A BUFFER TO COMMIT TO DISK
				final OMMapBufferEntry entry = fileEntries.get(p);
				if (entry.beginOffset <= iBeginOffset && entry.beginOffset + entry.size >= iBeginOffset) {
					OProfiler.getInstance().updateCounter("OMMapManager.overlappedPageUsingChannel", 1);
					commitBuffer(entry);
				}
			}
			OProfiler.getInstance().updateCounter("OMMapManager.usedChannel", 1);
			return null;
		}

		int bufferSize = iForce ? iSize : iSize <= blockSize ? blockSize : iSize;
		if (iBeginOffset + bufferSize > iFile.getFileSize())
			// REQUESTED BUFFER IS TOO LARGE: GET AS MAXIMUM AS POSSIBLE
			bufferSize = (int) (iFile.getFileSize() - iBeginOffset);

		if (bufferSize <= 0)
			throw new IllegalArgumentException("Invalid range requested for file " + iFile + ". Requested " + iSize
					+ " bytes from the address " + iBeginOffset + " while the total file size is " + iFile.getFileSize());

		if (totalMemory + bufferSize > maxMemory
				&& (iStrategy == STRATEGY.MMAP_ONLY_AVAIL_POOL || iOperationType == OPERATION_TYPE.READ
						&& iStrategy == STRATEGY.MMAP_WRITE_ALWAYS_READ_IF_AVAIL_POOL)) {
			OProfiler.getInstance().updateCounter("OMMapManager.usedChannel", 1);
			return null;
		}

		// FREE LESS-USED BUFFERS UNTIL THE FREE-MEMORY IS DOWN THE CONFIGURED MAX LIMIT
		OMMapBufferEntry entry = null;

		do {
			if (totalMemory + bufferSize > maxMemory) {
				final long memoryThreshold = (long) (maxMemory * 0.75);

				OLogManager.instance().debug(null, "Free mmmap blocks, at least %d MB...", (totalMemory - memoryThreshold) / 1000000);

				// SORT AS LRU, FIRST = MOST USED
				Collections.sort(bufferPoolLRU, new Comparator<OMMapBufferEntry>() {
					public int compare(final OMMapBufferEntry o1, final OMMapBufferEntry o2) {
						return (int) (o1.counter - o2.counter);
					}
				});

				// REMOVE THE LESS USED ENTRY AND UPDATE THE TOTAL MEMORY
				for (Iterator<OMMapBufferEntry> it = bufferPoolLRU.iterator(); it.hasNext();) {
					entry = it.next();
					if (!entry.pin) {
						if (!commitBuffer(entry))
							// ERROR ON COMMITTING BUFFER: TRY WITH THE NEXT ONE
							continue;

						// REMOVE FROM COLLECTIONS
						it.remove();
						bufferPoolPerFile.get(entry.file).remove(entry);

						entry.buffer = null;

						totalMemory -= entry.size;

						if (totalMemory < memoryThreshold)
							break;
					}
				}
			}

			// RECOMPUTE THE POSITION AFTER REMOVING
			position = searchEntry(fileEntries, iBeginOffset, iSize);
			if (position > -1)
				// FOUND: THIS IS PRETT STRANGE SINCE IT WASN'T FOUND!
				return fileEntries.get(position);

			// LOAD THE PAGE
			try {
				entry = mapBuffer(iFile, iBeginOffset, bufferSize);
			} catch (Exception e) {
				// REDUCE MAX MEMORY TO FORCE EMPTY BUFFERS
				maxMemory = maxMemory * 90 / 100;
				OLogManager.instance().warn(OMMapManager.class, "Memory mapping error, try to reduce max memory to %d and retry...",
						maxMemory);
			}
		} while (entry == null && maxMemory > MIN_MEMORY);

		if (entry == null)
			throw new OIOException("You can't access to the file portion " + iBeginOffset + "-" + iBeginOffset + iSize + " bytes");

		totalMemory += bufferSize;
		bufferPoolLRU.add(entry);
		fileEntries.add((position + 1) * -1, entry);

		return entry;
	}

	/**
	 * Flush away all the buffers of closed files. This frees the memory.
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
		bufferPoolPerFile.clear();
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

	public static int getBlockSize() {
		return blockSize;
	}

	public static void setBlockSize(final int blockSize) {
		OMMapManager.blockSize = blockSize;
	}

	private static OMMapBufferEntry mapBuffer(final OFileMMap iFile, final long iBeginOffset, final int iSize) throws IOException {
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
	private static int searchEntry(final List<OMMapBufferEntry> fileEntries, final long iBeginOffset, final int iSize) {
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
				OProfiler.getInstance().updateCounter("OMMapManager.reusedPage", 1);
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

	protected static boolean commitBuffer(final OMMapBufferEntry iEntry) {
		final long timer = OProfiler.getInstance().startChrono();

		// FORCE THE WRITE OF THE BUFFER
		boolean forceSucceed = false;
		for (int i = 0; i < FORCE_RETRY; ++i) {
			try {
				iEntry.buffer.force();
				forceSucceed = true;
				break;
			} catch (Exception e) {
				OLogManager.instance()
						.debug(iEntry, "Can't write memory buffer to disk. Retrying (" + (i + 1) + "/" + FORCE_RETRY + ")...");
				try {
					System.gc();
					Thread.sleep(FORCE_DELAY);
				} catch (InterruptedException e1) {
				}
			}
		}

		if (!forceSucceed)
			OLogManager.instance().debug(iEntry, "Can't commit memory buffer to disk after %d retries", FORCE_RETRY);
		else
			OProfiler.getInstance().updateCounter("OMMapManager.pagesCommitted", 1);

		OProfiler.getInstance().stopChrono("OMMapManager.commitPages", timer);

		return forceSucceed;
	}
}
