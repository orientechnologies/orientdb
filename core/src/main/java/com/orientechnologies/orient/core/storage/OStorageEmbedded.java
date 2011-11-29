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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Interface for embedded storage.
 * 
 * @see OStorageLocal, OStorageMemory
 * @author Luca Garulli
 * 
 */
public abstract class OStorageEmbedded extends OStorageAbstract {
	protected final ORecordLockManager	lockManager;

	public OStorageEmbedded(final String iName, final String iFilePath, final String iMode) {
		super(iName, iFilePath, iMode);

		lockManager = new ORecordLockManager(OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.getValueAsInteger());
	}

	protected abstract ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, boolean iAtomicLock);

	public abstract OCluster getClusterByName(final String iClusterName);

	/**
	 * Execute the command request and return the result back.
	 */
	public Object command(final OCommandRequestText iCommand) {
		final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);
		executor.setProgressListener(iCommand.getProgressListener());
		executor.parse(iCommand);
		try {
			return executor.execute(iCommand.getParameters());
		} catch (OException e) {
			// PASS THROUGHT
			throw e;
		} catch (Exception e) {
			throw new OCommandExecutionException("Error on execution of command: " + iCommand, e);
		}
	}

	/**
	 * Browse N clusters. The entire operation use a shared lock on the storage and lock the cluster from the external avoiding atomic
	 * lock at every record read.
	 * 
	 * @param iClusterId
	 *          Array of cluster ids
	 * @param iListener
	 *          The listener to call for each record found
	 * @param ioRecord
	 */
	public void browse(final int[] iClusterId, final ORID iBeginRange, final ORID iEndRange, final ORecordBrowsingListener iListener,
			ORecordInternal<?> ioRecord, final boolean iLockEntireCluster) {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		try {

			for (int clusterId : iClusterId) {
				if (iBeginRange != null)
					if (clusterId < iBeginRange.getClusterId())
						// JUMP THIS
						continue;

				if (iEndRange != null)
					if (clusterId > iEndRange.getClusterId())
						// STOP
						break;

				final OCluster cluster = getClusterById(clusterId);

				final long beginClusterPosition = iBeginRange != null && iBeginRange.getClusterId() == clusterId ? iBeginRange
						.getClusterPosition() : 0;
				final long endClusterPosition = iEndRange != null && iEndRange.getClusterId() == clusterId ? iEndRange.getClusterPosition()
						: -1;

				ioRecord = browseCluster(iListener, ioRecord, cluster, beginClusterPosition, endClusterPosition, iLockEntireCluster);
			}
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on browsing elements of cluster: " + iClusterId, e);

		} finally {
			OProfiler.getInstance().stopChrono("OStorageLocal.foreach", timer);
		}
	}

	public ORecordInternal<?> browseCluster(final ORecordBrowsingListener iListener, ORecordInternal<?> ioRecord,
			final OCluster cluster, final long iBeginRange, final long iEndRange, final boolean iLockEntireCluster) throws IOException {
		ORecordInternal<?> record;

		if (iLockEntireCluster)
			// LOCK THE ENTIRE CLUSTER AVOIDING TO LOCK EVERY SINGLE RECORD
			cluster.lock();

		try {
			final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
			ORecordIteratorCluster<ORecordInternal<?>> iterator = new ORecordIteratorCluster<ORecordInternal<?>>(db,
					(ODatabaseRecordAbstract) db, cluster.getId(), iBeginRange, iEndRange);

			// BROWSE ALL THE RECORDS
			while (iterator.hasNext()) {
				record = iterator.next();

				if (record != null && record.getRecordType() != ODocument.RECORD_TYPE)
					// WRONG RECORD TYPE: JUMP IT
					continue;

				ORecordInternal<?> recordToCheck = null;
				try {

					// GET THE CACHED RECORD
					recordToCheck = record;

					if (!iListener.foreach(recordToCheck))
						// LISTENER HAS INTERRUPTED THE EXECUTION
						break;
				} catch (OCommandExecutionException e) {
					// PASS THROUGH
					throw e;
				} catch (Exception e) {
					OLogManager.instance().exception("Error on loading record %s. Cause: %s", e, OStorageException.class,
							recordToCheck != null ? recordToCheck.getIdentity() : null, e);
				}
			}
		} finally {

			if (iLockEntireCluster)
				// UNLOCK THE ENTIRE CLUSTER
				cluster.unlock();
		}

		return ioRecord;
	}

	/**
	 * Check if the storage is open. If it's closed an exception is raised.
	 */
	protected void checkOpeness() {
		if (status != STATUS.OPEN)
			throw new OStorageException("Storage " + name + " is not opened.");
	}
}
