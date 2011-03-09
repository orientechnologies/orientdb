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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordFactory;
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

	public OStorageEmbedded(final String iName, final String iFilePath, final String iMode) {
		super(iName, iFilePath, iMode);
	}

	protected abstract ORawBuffer readRecord(final int iRequesterId, final OCluster iClusterSegment, final long iPosition,
			boolean iAtomicLock);

	/**
	 * Execute the command request and return the result back.
	 */
	public Object command(final OCommandRequestText iCommand) {
		final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);
		executor.setProgressListener(iCommand.getProgressListener());
		executor.parse(iCommand);
		try {
			return executor.execute(iCommand.getParameters());
		} catch (OCommandExecutionException e) {
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
	 * @param iRequesterId
	 *          The requester of the operation. Needed to know who locks
	 * @param iClusterId
	 *          Array of cluster ids
	 * @param iListener
	 *          The listener to call for each record found
	 * @param ioRecord
	 */
	public void browse(final int iRequesterId, final int[] iClusterId, final ORecordId iBeginRange, final ORecordId iEndRange,
			final ORecordBrowsingListener iListener, ORecordInternal<?> ioRecord, final boolean iLockEntireCluster) {
		checkOpeness();

		final long timer = OProfiler.getInstance().startChrono();

		final boolean locked = lock.acquireSharedLock();

		try {
			OCluster cluster;

			long beginClusterPosition;
			long endClusterPosition;

			for (int clusterId : iClusterId) {
				if (iBeginRange != null)
					if (clusterId < iBeginRange.getClusterId())
						// JUMP THIS
						continue;

				if (iEndRange != null)
					if (clusterId > iEndRange.getClusterId())
						// STOP
						break;

				cluster = getClusterById(clusterId);

				beginClusterPosition = iBeginRange != null && iBeginRange.getClusterId() == clusterId ? iBeginRange.getClusterPosition()
						: 0;
				endClusterPosition = iEndRange != null && iEndRange.getClusterId() == clusterId ? iEndRange.getClusterPosition() : -1;

				ioRecord = browseCluster(iRequesterId, iListener, ioRecord, cluster, beginClusterPosition, endClusterPosition,
						iLockEntireCluster);
			}
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on browsing elements of cluster: " + iClusterId, e);

		} finally {
			lock.releaseSharedLock(locked);

			OProfiler.getInstance().stopChrono("OStorageLocal.foreach", timer);
		}
	}

	public ORecordInternal<?> browseCluster(final int iRequesterId, final ORecordBrowsingListener iListener,
			ORecordInternal<?> ioRecord, final OCluster cluster, final long iBeginRange, final long iEndRange,
			final boolean iLockEntireCluster) throws IOException {
		ORecordInternal<?> record;
		ORawBuffer recordBuffer;
		long positionInPhyCluster;

		if (iLockEntireCluster)
			// LOCK THE ENTIRE CLUSTER AVOIDING TO LOCK EVERY SINGLE RECORD
			cluster.lock();

		try {
			final OClusterPositionIterator iterator = cluster.absoluteIterator(iBeginRange, iEndRange);

			final ORecordId rid = new ORecordId(cluster.getId());
			final ODatabaseRecord database = ioRecord.getDatabase();
			ORecordInternal<?> recordToCheck = null;

			// BROWSE ALL THE RECORDS
			while (iterator.hasNext()) {
				positionInPhyCluster = iterator.next();

				if (positionInPhyCluster == -1)
					// NOT VALID POSITION (IT HAS BEEN DELETED)
					continue;

				rid.clusterPosition = positionInPhyCluster;

				// TRY IN TX
				record = database.getTransaction().getEntry(rid);
				if (record == null)
					// TRY IN CACHE
					record = database.getCache().findRecord(rid);

				if (record != null && record.getRecordType() != ODocument.RECORD_TYPE)
					// WRONG RECORD TYPE: JUMP IT
					continue;

				try {
					if (record == null) {
						// READ THE RAW RECORD. IF iLockEntireCluster THEN THE READ WILL
						// BE NOT-LOCKING, OTHERWISE YES
						recordBuffer = readRecord(iRequesterId, cluster, positionInPhyCluster, !iLockEntireCluster);
						if (recordBuffer == null)
							continue;

						if (recordBuffer.recordType != ODocument.RECORD_TYPE)
							// WRONG RECORD TYPE: JUMP IT
							continue;

						if (ioRecord.getRecordType() != recordBuffer.recordType) {
							// RECORD NULL OR DIFFERENT IN TYPE: CREATE A NEW ONE
							final ORecordInternal<?> newRecord = ORecordFactory.newInstance(recordBuffer.recordType);
							newRecord.setDatabase(ioRecord.getDatabase());
							ioRecord = newRecord;
						} else
							// RESET CURRENT RECORD
							ioRecord.reset();

						ioRecord.setVersion(recordBuffer.version);
						ioRecord.setIdentity(cluster.getId(), positionInPhyCluster);
						ioRecord.fromStream(recordBuffer.buffer);
						recordToCheck = ioRecord;
					} else
						// GET THE RECORD CACHED
						recordToCheck = record;

					if (!iListener.foreach(recordToCheck))
						// LISTENER HAS INTERRUPTED THE EXECUTION
						break;
				} catch (Exception e) {
					OLogManager.instance().error(this, "Error on loading record %s. Cause: %s", recordToCheck.getIdentity(), e);
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
		if (!open)
			throw new OStorageException("Storage " + name + " is not opened.");
	}
}
