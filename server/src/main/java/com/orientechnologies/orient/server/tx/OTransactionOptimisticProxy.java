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

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OTransactionOptimisticProxy extends OTransactionOptimistic {
	private final Map<ORecordId, ORecord<?>>	createdRecords			= new HashMap<ORecordId, ORecord<?>>();
	private final Map<ORecordId, ORecord<?>>	updatedRecords			= new HashMap<ORecordId, ORecord<?>>();
	private int																clientTxId;
	private ODocument													remoteIndexEntries	= null;

	public OTransactionOptimisticProxy(final ODatabaseRecordTx iDatabase, final OChannelBinary iChannel) throws IOException {
		super(iDatabase);
		clientTxId = iChannel.readInt();
		setUsingLog(iChannel.readByte() == 1);

		while (iChannel.readByte() == 1) {
			try {
				final byte recordStatus = iChannel.readByte();

				final ORecordId rid = iChannel.readRID();

				final OTransactionEntryProxy entry = new OTransactionEntryProxy(iChannel.readByte());
				entry.status = recordStatus;

				switch (entry.status) {
				case OTransactionRecordEntry.CREATED:
					entry.clusterName = iChannel.readString();
					entry.getRecord().fill(iDatabase, rid, 0, iChannel.readBytes(), true);

					if (entry.getRecord() instanceof ODocument)
						// ASSURE FIELDS ARE UNMARSHALLED: THIS PREVENT TO STORE TEMPORARY RID
						((ODocument) entry.getRecord()).deserializeFields();

					// SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW RID TO SEND BACK TO THE REQUESTER
					createdRecords.put(rid.copy(), entry.getRecord());
					break;

				case OTransactionRecordEntry.UPDATED:
					entry.getRecord().fill(iDatabase, rid, iChannel.readInt(), iChannel.readBytes(), true);

					if (entry.getRecord() instanceof ODocument)
						// ASSURE FIELDS ARE UNMARSHALLED: THIS PREVENT TO STORE TEMPORARY RID
						((ODocument) entry.getRecord()).deserializeFields();

					// SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
					updatedRecords.put(rid, entry.getRecord());
					break;

				case OTransactionRecordEntry.DELETED:
					entry.getRecord().fill(iDatabase, rid, iChannel.readInt(), null, false);
					break;

				default:
					throw new OTransactionException("Unrecognized tx command: " + entry.status);
				}

				// PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
				recordEntries.put((ORecordId) entry.getRecord().getIdentity(), entry);

			} catch (IOException e) {
				throw new OSerializationException("Can't read transaction record from the network", e);
			}
		}

		remoteIndexEntries = new ODocument(iChannel.readBytes());
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
}
