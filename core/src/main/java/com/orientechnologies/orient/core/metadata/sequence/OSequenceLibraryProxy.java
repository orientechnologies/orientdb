/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Set;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class OSequenceLibraryProxy extends OProxedResource<OSequenceLibrary> implements OSequenceLibrary {
    public OSequenceLibraryProxy(final OSequenceLibrary iDelegate, final ODatabaseDocumentInternal iDatabase) {
        super(iDelegate, iDatabase);
    }

    @Override
    public Set<String> getSequenceNames() {
        return delegate.getSequenceNames();
    }

    @Override
    public int getSequenceCount() {
        return delegate.getSequenceCount();
    }

    @Override
    public OSequence getSequence(String iName) {
        return delegate.getSequence(iName);
    }

    @Override
    public OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params) {
        return delegate.createSequence(iName, sequenceType, params);
    }

    @Override
    public void dropSequence(String iName) {
        delegate.dropSequence(iName);
    }

    @Override
    public OSequence onSequenceCreated(ODocument iDocument) {
        return delegate.onSequenceCreated(iDocument);
    }

    @Override
    public OSequence onSequenceUpdated(ODocument iDocument) {
        return delegate.onSequenceUpdated(iDocument);
    }

    @Override
    public void onSequenceDropped(ODocument iDocument) {
        delegate.onSequenceDropped(iDocument);
    }

    @Override
    public void create() {
        delegate.create();
    }

    @Override
    public void load() {
        delegate.load();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
