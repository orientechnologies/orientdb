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
package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

/**
 * Proxied abstract index.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public abstract class OIndexRemote<T> implements OIndex<T> {
	private final String					wrappedType;
	private final ORID						rid;

	protected String							name;

	protected final static String	QUERY_GET											= "select from index:%s where key = ?";
	protected final static String	QUERY_ENTRIES									= "select key, rid from index:%s";

	private final static String		QUERY_GET_MAJOR								= "select from index:%s where key > ?";
	private final static String		QUERY_GET_MAJOR_EQUALS				= "select from index:%s where key >= ?";
	private final static String		QUERY_GET_VALUE_MAJOR					= "select @rid from index:%s where key > ?";
	private final static String		QUERY_GET_VALUE_MAJOR_EQUALS	= "select @rid from index:%s where key >= ?";
	private final static String		QUERY_GET_MINOR								= "select from index:%s where key < ?";
	private final static String		QUERY_GET_MINOR_EQUALS				= "select from index:%s where key <= ?";
	private final static String		QUERY_GET_VALUE_MINOR					= "select @rid from index:%s where key < ?";
	private final static String		QUERY_GET_VALUE_MINOR_EQUALS	= "select @rid from index:%s where key <= ?";
	private final static String		QUERY_GET_RANGE								= "select from index:%s where key between ? and ?";
	private final static String     QUERY_GET_VALUES                = "select @rid from index:%s where key in [%s]";
    private final static String     QUERY_GET_ENTRIES               = "select from index:%s where key in [%s]";
	private final static String		QUERY_GET_VALUE_RANGE					= "select @rid from index:%s where key between ? and ?";
	private final static String		QUERY_PUT											= "insert into index:%s (key,rid) values (%s,%s)";
	private final static String		QUERY_REMOVE									= "delete from index:%s where key = %s";
	private final static String		QUERY_REMOVE2									= "delete from index:%s where key = %s and rid = %s";
	private final static String		QUERY_REMOVE3									= "delete from index:%s where rid = ?";
	private final static String		QUERY_CONTAINS								= "select count(*) as size from	index:%s where key = ?";
	private final static String		QUERY_SIZE										= "select count(*) as size from index:%s";
	private final static String		QUERY_KEYS										= "select key from index:%s";
	private final static String		QUERY_REBUILD									= "rebuild index %s";
	private final static String		QUERY_CLEAR										= "delete from index:%s";

	public OIndexRemote(final String iName, final String iWrappedType, final ORID iRid) {
		this.name = iName;
		this.wrappedType = iWrappedType;
		this.rid = iRid;
	}

	public OIndexRemote<T> create(final String iName, final OType iKeyType, final ODatabaseRecord iDatabase,
			final String iClusterIndexName, final int[] iClusterIdsToIndex, final OProgressListener iProgressListener,
			final boolean iAutomatic) {
		name = iName;
		// final OCommandRequest cmd = formatCommand(QUERY_CREATE, name, wrappedType);
		// database.command(cmd).execute();
		return this;
	}

	public OIndexRemote<T> delete() {
		// final OCommandRequest cmd = formatCommand(QUERY_DROP, name);
		// database.command(cmd).execute();
		return this;
	}

	public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo) {
		final OCommandRequest cmd = formatCommand(QUERY_GET_VALUE_RANGE, name);
		return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
	}

	public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
		final OCommandRequest cmd = formatCommand(QUERY_GET_VALUE_RANGE, name);
		return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
	}

	public Set<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
		final OCommandRequest cmd = formatCommand(QUERY_GET_RANGE, name);
		return (Set<ODocument>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
	}

	public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
		final OCommandRequest cmd = formatCommand(QUERY_GET_RANGE, name);
		return (Collection<ODocument>) getDatabase().command(cmd).execute(iRangeFrom, iRangeTo);
	}

	public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
		final OCommandRequest cmd;
		if (isInclusive)
			cmd = formatCommand(QUERY_GET_VALUE_MAJOR_EQUALS, name);
		else
			cmd = formatCommand(QUERY_GET_VALUE_MAJOR, name);
		return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(fromKey);
	}

	public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
		final OCommandRequest cmd;
		if (isInclusive)
			cmd = formatCommand(QUERY_GET_MAJOR_EQUALS, name);
		else
			cmd = formatCommand(QUERY_GET_MAJOR, name);
		return (Collection<ODocument>) getDatabase().command(cmd).execute(fromKey);
	}

	public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
		final OCommandRequest cmd;
		if (isInclusive)
			cmd = formatCommand(QUERY_GET_VALUE_MINOR_EQUALS, name);
		else
			cmd = formatCommand(QUERY_GET_VALUE_MINOR, name);
		return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(toKey);
	}

	public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
		final OCommandRequest cmd;
		if (isInclusive)
			cmd = formatCommand(QUERY_GET_MINOR_EQUALS, name);
		else
			cmd = formatCommand(QUERY_GET_MINOR, name);
		return (Collection<ODocument>) getDatabase().command(cmd).execute(toKey);
	}

	public boolean contains(final Object iKey) {
		final OCommandRequest cmd = formatCommand(QUERY_CONTAINS, name);
		final List<ODocument> result = getDatabase().command(cmd).execute();
		return (Long) result.get(0).field("size") > 0;
	}

	public OIndexRemote<T> put(Object iKey, final OIdentifiable iValue) {
		if (iKey instanceof String)
			iKey = "'" + iKey + "'";

		if (iValue instanceof ORecord<?> && !iValue.getIdentity().isValid())
			// SAVE IT BEFORE TO PUT
			((ORecord<?>) iValue).save();

		final OCommandRequest cmd = formatCommand(QUERY_PUT, name, iKey, iValue.getIdentity());
		getDatabase().command(cmd).execute();
		return this;
	}

	public boolean remove(Object iKey) {
		if (iKey instanceof String)
			iKey = "'" + iKey + "'";

		final OCommandRequest cmd = formatCommand(QUERY_REMOVE, name, iKey);
		return Boolean.parseBoolean((String) getDatabase().command(cmd).execute(iKey));
	}

	public boolean remove(Object iKey, final OIdentifiable iRID) {
		if (iKey instanceof String)
			iKey = "'" + iKey + "'";

		final OCommandRequest cmd = formatCommand(QUERY_REMOVE2, name, iKey, iRID.getIdentity());
		return Boolean.parseBoolean((String) getDatabase().command(cmd).execute(iKey, iRID));
	}

	public int remove(final OIdentifiable iRecord) {
		final OCommandRequest cmd = formatCommand(QUERY_REMOVE3, name, iRecord.getIdentity());
		return (Integer) getDatabase().command(cmd).execute(iRecord);
	}

	public long rebuild() {
		final OCommandRequest cmd = formatCommand(QUERY_REBUILD, name);
		return (Long) getDatabase().command(cmd).execute();
	}

	public OIndexRemote<T> clear() {
		final OCommandRequest cmd = formatCommand(QUERY_CLEAR, name);
		getDatabase().command(cmd).execute();
		return this;
	}

	public Iterable<Object> keys() {
		final OCommandRequest cmd = formatCommand(QUERY_KEYS, name);
		return (Iterable<Object>) getDatabase().command(cmd).execute();
	}

	public long getSize() {
		final OCommandRequest cmd = formatCommand(QUERY_SIZE, name);
		final List<ODocument> result = getDatabase().command(cmd).execute();
		return (Long) result.get(0).field("size");
	}

	public void unload() {
	}

	public boolean isAutomatic() {
		return false;
	}

	public String getName() {
		return name;
	}

	/**
	 * Do nothing.
	 */
	public OIndexRemote<T> lazySave() {
		return this;
	}

	public String getType() {
		return wrappedType;
	}

	public void setCallback(OIndexCallback iCallback) {
	}

	public ODocument getConfiguration() {
		return null;
	}

	public ORID getIdentity() {
		return rid;
	}

	protected OCommandRequest formatCommand(final String iTemplate, final Object... iArgs) {
		final String text = String.format(iTemplate, iArgs);
		return new OCommandSQL(text);
	}

	public void commit(final ODocument iDocument) {
	}

	public OIndexInternal<T> getInternal() {
		return null;
	}

	protected ODatabaseComplex<ORecordInternal<?>> getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	public long rebuild(final OProgressListener iProgressListener) {
		return rebuild();
	}

	public OType getKeyType() {
		return null;
	}

    public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
        final StringBuilder params = new StringBuilder();
        if (!iKeys.isEmpty()) {
            params.append("?");
            for (int i = 1; i < iKeys.size(); i++) {
                params.append(", ?");
            }
        }

        final OCommandRequest cmd = formatCommand(QUERY_GET_VALUES, name, params.toString());
        return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(iKeys.toArray());
    }

    public Collection<ODocument> getEntries(final Collection<?> iKeys) {
        final StringBuilder params = new StringBuilder();
        if (!iKeys.isEmpty()) {
            params.append("?");
            for (int i = 1; i < iKeys.size(); i++) {
                params.append(", ?");
            }
        }

        final OCommandRequest cmd = formatCommand(QUERY_GET_ENTRIES, name, params.toString());
        return (Collection<ODocument>) getDatabase().command(cmd).execute(iKeys.toArray());
    }
}
