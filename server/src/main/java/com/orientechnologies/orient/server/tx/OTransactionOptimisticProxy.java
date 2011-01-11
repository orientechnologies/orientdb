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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OTransactionOptimisticProxy extends OTransactionAbstract {
	private int														size;
	private OChannelBinary								channel;
	private List<OTransactionEntryProxy>	entries					= new ArrayList<OTransactionEntryProxy>();
	private boolean												exhausted				= false;
	private Map<ORecordId, ORecord<?>>		updatedRecords	= new HashMap<ORecordId, ORecord<?>>();

	public OTransactionOptimisticProxy(final ODatabaseRecordTx iDatabase, final OChannelBinary iChannel) throws IOException {
		super(iDatabase, -1);

		channel = iChannel;
		id = iChannel.readInt();
		size = iChannel.readInt();
	}

	@Override
	public Iterable<? extends OTransactionEntry> getEntries() {

		return new Iterable<OTransactionEntry>() {

			@Override
			public Iterator<OTransactionEntry> iterator() {
				return new Iterator<OTransactionEntry>() {
					private int	current	= 1;

					@Override
					public boolean hasNext() {
						if (exhausted)
							return false;

						exhausted = current > size;
						return !exhausted;
					}

					@Override
					public OTransactionEntryProxy next() {
						try {
							OTransactionEntryProxy entry = new OTransactionEntryProxy();

							if (entries.size() < size) {
								final ORecordId rid = (ORecordId) entry.getRecord().getIdentity();

								entry.status = channel.readByte();
								rid.clusterId = channel.readShort();
								((OTransactionRecordProxy) entry.getRecord()).setRecordType(channel.readByte());

								switch (entry.status) {
								case OTransactionEntry.CREATED:
									rid.clusterPosition = -1;
									entry.clusterName = channel.readString();
									entry.getRecord().fromStream(channel.readBytes());
									break;

								case OTransactionEntry.UPDATED:
									rid.clusterPosition = channel.readLong();
									entry.getRecord().setVersion(channel.readInt());
									entry.getRecord().fromStream(channel.readBytes());

									// SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
									updatedRecords.put(rid, entry.getRecord());
									break;

								case OTransactionEntry.DELETED:
									rid.clusterPosition = channel.readLong();
									entry.getRecord().setVersion(channel.readInt());
									break;

								default:
									throw new OTransactionException("Unrecognized tx command: " + entry.status);
								}

								// PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
								entries.add(entry);
							} else
								entry = entries.get(current - 1);

							current++;

							return entry;
						} catch (IOException e) {
							throw new OSerializationException("Can't read transaction record from the network", e);
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException("remove");
					}
				};
			}
		};

	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void begin() {
		throw new UnsupportedOperationException("begin");
	}

	@Override
	public void commit() {
		throw new UnsupportedOperationException("commit");
	}

	@Override
	public void rollback() {
		throw new UnsupportedOperationException("rollback");
	}

	public long createRecord(final ORecordInternal<?> iContent) {
		throw new UnsupportedOperationException("createRecord");
	}

	@Override
	public void delete(final ORecordInternal<?> iRecord) {
		throw new UnsupportedOperationException("delete");
	}

	@Override
	public ORecordInternal<?> load(final int iClusterId, final long iPosition, final ORecordInternal<?> iRecord,
			final String iFetchPlan) {
		throw new UnsupportedOperationException("load");
	}

	@Override
	public void save(final ORecordInternal<?> iContent, final String iClusterName) {
		throw new UnsupportedOperationException("save");
	}

	@Override
	public void clearEntries() {
		entries.clear();
	}

	@Override
	public List<OTransactionEntry> getEntriesByClass(String iClassName) {
		throw new UnsupportedOperationException("getRecordsByClass");
	}

	@Override
	public List<OTransactionEntry> getEntriesByClusterIds(int[] iIds) {
		throw new UnsupportedOperationException("getRecordsByClusterIds");
	}

	public Map<ORecordId, ORecord<?>> getUpdatedRecords() {
		return updatedRecords;
	}
}
