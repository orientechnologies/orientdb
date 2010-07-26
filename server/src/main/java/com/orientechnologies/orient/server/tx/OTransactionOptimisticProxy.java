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
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OTransactionOptimisticProxy extends OTransactionAbstract<OTransactionRecordProxy> {
	private OTransactionEntryProxy				entry		= new OTransactionEntryProxy();
	private int														size;
	private OChannelBinary								channel;
	private List<OTransactionEntryProxy>	entries	= new ArrayList<OTransactionEntryProxy>();

	public OTransactionOptimisticProxy(final ODatabaseRecordTx<OTransactionRecordProxy> iDatabase, final OChannelBinary iChannel)
			throws IOException {
		super(iDatabase, -1);

		channel = iChannel;
		id = iChannel.readInt();
		size = iChannel.readInt();
	}

	public Iterable<? extends OTransactionEntry<OTransactionRecordProxy>> getEntries() {

		return new Iterable<OTransactionEntryProxy>() {

			public Iterator<OTransactionEntryProxy> iterator() {
				return new Iterator<OTransactionEntryProxy>() {
					private int	current	= 1;

					public boolean hasNext() {
						return current <= size;
					}

					public OTransactionEntryProxy next() {
						try {
							if (entries.size() < size) {
								final ORecordId rid = (ORecordId) entry.record.getIdentity();

								entry.status = channel.readByte();
								rid.clusterId = channel.readShort();
								entry.record.recordType = channel.readByte();

								switch (entry.status) {
								case OTransactionEntry.CREATED:
									entry.clusterName = channel.readString();
									entry.record.stream = channel.readBytes();
									break;

								case OTransactionEntry.UPDATED:
									rid.clusterPosition = channel.readLong();
									entry.record.version = channel.readInt();
									entry.record.stream = channel.readBytes();
									break;

								case OTransactionEntry.DELETED:
									rid.clusterPosition = channel.readLong();
									entry.record.version = channel.readInt();
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

					public void remove() {
						throw new UnsupportedOperationException("remove");
					}
				};
			}
		};

	}

	public int size() {
		return size;
	}

	public void begin() {
		throw new UnsupportedOperationException("begin");
	}

	public void commit() {
		throw new UnsupportedOperationException("commit");
	}

	public void rollback() {
		throw new UnsupportedOperationException("rollback");
	}

	public long createRecord(final OTransactionRecordProxy iContent) {
		throw new UnsupportedOperationException("createRecord");
	}

	public void delete(final OTransactionRecordProxy iRecord) {
		throw new UnsupportedOperationException("delete");
	}

	public OTransactionRecordProxy load(final int iClusterId, final long iPosition, final OTransactionRecordProxy iRecord,
			final String iFetchPlan) {
		throw new UnsupportedOperationException("load");
	}

	public void save(final OTransactionRecordProxy iContent, final String iClusterName) {
		throw new UnsupportedOperationException("save");
	}
}
