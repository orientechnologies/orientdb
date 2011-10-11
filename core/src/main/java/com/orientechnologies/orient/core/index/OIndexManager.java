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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface OIndexManager {
	public OIndexManager load();

	public void create();

	public Collection<? extends OIndex<?>> getIndexes();

	public OIndex<?> getIndex(final String iName);

	public OIndex<?> getIndex(final ORID iRID);

	public OIndex<?> createIndex(final String iName, final String iType, OIndexDefinition iIndexDefinition, final int[] iClusterIdsToIndex,
                                 final OProgressListener iProgressListener);

	public OIndexManager dropIndex(final String iIndexName);

    public String getDefaultClusterName();

	public void setDefaultClusterName(String defaultClusterName);

	public ODictionary<ORecordInternal<?>> getDictionary();

	public void flush();

	public ODocument getConfiguration();

      /**
     * Returns list of indexes that contain passed in fields names as their first keys.
     * Order of fields does not matter.
     * <p/>
     * All indexes sorted by their count of parameters in ascending order.
     * If there are indexes for the given set of fields in super class they
     * will be taken into account.
     *
     *
       *
       * @param className name of class which is indexed.
       * @param fields    Field names.
       * @return list of indexes that contain passed in fields names as their first keys.
     */
    public Set<OIndex<?>> getClassInvolvedIndexes( String className, Collection<String> fields );

    /**
     * Returns list of indexes that contain passed in fields names as their first keys.
     * Order of fields does not matter.
     * <p/>
     * All indexes sorted by their count of parameters in ascending order.
     * If there are indexes for the given set of fields in super class they
     * will be taken into account.
     *
     *
     *
     * @param className name of class which is indexed.
     * @param fields    Field names.
     * @return list of indexes that contain passed in fields names as their first keys.
     */
    public Set<OIndex<?>> getClassInvolvedIndexes( String className, String... fields );


    /**
     * Indicates whether given fields are contained as first key fields in class indexes.
     * Order of fields does not matter. If there are indexes for the given set of fields in super class they
     * will be taken into account.
     *
     * @param className name of class which contain {@code fields}.
     * @param fields Field names.
     * @return <code>true</code> if given fields are contained as first key fields in class indexes.
     */
    public boolean areIndexed(String className, Collection<String> fields);

    /**
     * @param className name of class which contain {@code fields}.
     * @param fields Field names.
     * @return <code>true</code> if given fields are contained as first key fields in class indexes.
     * @see #areIndexed(String, java.util.Collection)
     */
    public boolean areIndexed(String className, String... fields);

    public Set<OIndex<?>> getClassIndexes(String className);

    public OIndex<?> getClassIndex(String className, String indexName);
}
