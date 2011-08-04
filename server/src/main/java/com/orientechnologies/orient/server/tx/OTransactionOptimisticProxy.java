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
package com.orientechnologies.orient.server.tx;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OTransactionOptimisticProxy extends OTransactionOptimistic {
	private final Map<ORecordId, ORecord<?>>	createdRecords			= new HashMap<ORecordId, ORecord<?>>();
	private final Map<ORecordId, ORecord<?>>	updatedRecords			= new HashMap<ORecordId, ORecord<?>>();
	private final int													clientTxId;
	private ODocument													remoteIndexEntries	= null;
	private final OChannelBinary							channel;

	public OTransactionOptimisticProxy(final ODatabaseRecordTx iDatabase, final OChannelBinary iChannel) throws IOException {
		super(iDatabase);
		channel = iChannel;
		clientTxId = iChannel.readInt();
	}

	@Override
	public void begin() {
		super.begin();

		try {
			setUsingLog(channel.readByte() == 1);

			while (channel.readByte() == 1) {
				final byte recordStatus = channel.readByte();

				final ORecordId rid = channel.readRID();

				final OTransactionEntryProxy entry = new OTransactionEntryProxy(channel.readByte());
				entry.status = recordStatus;

				switch (entry.status) {
				case OTransactionRecordEntry.CREATED:
					entry.clusterName = channel.readString();
					entry.getRecord().fill(database, rid, 0, channel.readBytes(), true);

					// SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW RID TO SEND BACK TO THE REQUESTER
					createdRecords.put(rid.copy(), entry.getRecord());
					break;

				case OTransactionRecordEntry.UPDATED:
					entry.getRecord().fill(database, rid, channel.readInt(), channel.readBytes(), true);

					// SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
					updatedRecords.put(rid, entry.getRecord());
					break;

				case OTransactionRecordEntry.DELETED:
					entry.getRecord().fill(database, rid, channel.readInt(), null, false);
					break;

				default:
					throw new OTransactionException("Unrecognized tx command: " + entry.status);
				}

				// PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
				recordEntries.put((ORecordId) entry.getRecord().getIdentity(), entry);

			}
			remoteIndexEntries = new ODocument(channel.readBytes());

			// UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
			for (ORecord<?> record : createdRecords.values())
				unmarshallRecord(record);
			for (ORecord<?> record : updatedRecords.values())
				unmarshallRecord(record);

		} catch (IOException e) {
			rollback();
			throw new OSerializationException("Can't read transaction record from the network. Transaction aborted", e);
		}
	}

	@Override
	public ORecordInternal<?> getRecord(final ORID rid) {
		ORecordInternal<?> record = super.getRecord(rid);
		if (record == null && rid.isNew())
			// SEARCH BETWEEN CREATED RECORDS
			record = (ORecordInternal<?>) createdRecords.get(rid);

		return record;
	}

	@Override
	public ODocument getIndexChanges() {
		return remoteIndexEntries.merge(super.getIndexChanges(), true, true);
	}

	public Map<ORecordId, ORecord<?>> getCreatedRecords() {
		return createdRecords;
	}

	public Map<ORecordId, ORecord<?>> getUpdatedRecords() {
		return updatedRecords;
	}

	/**
	 * Unmarshalls collections. This prevent temporary RIDs remains stored as are.
	 */
	private void unmarshallRecord(final ORecord<?> iRecord) {
		if (iRecord instanceof ODocument) {
			((ODocument) iRecord).deserializeFields();

			for (Entry<String, Object> field : ((ODocument) iRecord)) {
				if (field.getValue() instanceof ORecordLazyList)
					((ORecordLazyList) field.getValue()).lazyLoad(true);
				else if (field.getValue() instanceof ORecordLazySet)
					((ORecordLazySet) field.getValue()).lazyLoad(true);
			}
		}
	}
}
