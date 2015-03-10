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
