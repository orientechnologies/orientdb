/*
 * Copyright 2010-2014 OrientDB LTD (info--at--orientdb.com)
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
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
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
    return delegate.getSequence(database, iName);
  }

  @Override
  public OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params) {
    return delegate.createSequence(database, iName, sequenceType, params);
  }

  @Override
  public OSequence getSequence(ODatabaseDocumentInternal database, String iName) {
    return delegate.getSequence(database, iName);
  }

  @Override
  public OSequence createSequence(ODatabaseDocumentInternal database, String iName, SEQUENCE_TYPE sequenceType,
      OSequence.CreateParams params) {
    return delegate.createSequence(database, iName, sequenceType, params);
  }

  @Override
  public void dropSequence(ODatabaseDocumentInternal database, String iName) {
    delegate.dropSequence(database, iName);
  }

  @Override
  public void dropSequence(String iName) {
    delegate.dropSequence(database, iName);
  }

  @Override
  public void create() {
    delegate.create(database);
  }

  @Override
  public void load(ODatabaseDocumentInternal db) {
    delegate.load(db);
  }

  @Override
  public void load() {
    delegate.load(database);
  }

  @Override
  public void close() {
    delegate.close();
  }

  public OSequenceLibraryImpl getDelegate() {
    return delegate;
  }

}
