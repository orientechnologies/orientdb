package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;

import java.util.Set;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class OSequenceLibraryProxy extends OProxedResource<OSequenceLibraryImpl> implements OSequenceLibrary {
    public OSequenceLibraryProxy(final OSequenceLibraryImpl iDelegate, final ODatabaseDocumentInternal iDatabase) {
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
        return delegate.getSequence(database,iName);
    }

    @Override
    public OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params) {
        return delegate.createSequence(database,iName, sequenceType, params);
    }

    @Override
    public void dropSequence(String iName) {
      delegate.dropSequence(database,iName);
    }

    @Override
    public void create() {
        delegate.create(database);
    }

    @Override
    public void load() {
      delegate.load(database);
    }

    @Override
    public void close() {
        delegate.close();
    }
    
    public OSequenceLibraryImpl getDelegate(){
      return delegate;
    }
    
}
