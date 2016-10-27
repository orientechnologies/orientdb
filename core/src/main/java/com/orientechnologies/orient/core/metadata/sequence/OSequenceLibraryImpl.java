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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class OSequenceLibraryImpl implements OSequenceLibrary {
  private final Map<String, OSequence> sequences = new ConcurrentHashMap<String, OSequence>();

  @Override
  public void create() {

  }

  public void create(ODatabaseDocumentInternal database) {
    init(database);
  }

  @Override
  public void load() {
    throw new UnsupportedOperationException("use api with database for internal");
  }

  @Override
  public void load(final ODatabaseDocumentInternal db) {
    sequences.clear();

    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass(OSequence.CLASS_NAME)) {
      final List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + OSequence.CLASS_NAME));
      for (ODocument document : result) {
        document.reload();

        final OSequence sequence = OSequenceHelper.createSequence(document);
        sequences.put(sequence.getName().toUpperCase(), sequence);
      }
    }
  }

  @Override
  public void close() {
    sequences.clear();
  }

  @Override
  public Set<String> getSequenceNames() {
    return sequences.keySet();
  }

  @Override
  public int getSequenceCount() {
    return sequences.size();
  }

  @Override
  public OSequence getSequence(final ODatabaseDocumentInternal database, final String iName) {
    OSequence seq = sequences.get(iName.toUpperCase());
    if (seq == null) {
      load(database);
      seq = sequences.get(iName.toUpperCase());
    }

    if (seq != null)
      seq.bindOnLocalThread();

    return seq;
  }

  @Override
  public OSequence getSequence(final String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSequence createSequence(final String iName, final SEQUENCE_TYPE sequenceType, final OSequence.CreateParams params) {
    throw new UnsupportedOperationException("use api with database for internal");
  }

  @Override
  public OSequence createSequence(final ODatabaseDocumentInternal database, final String iName, final SEQUENCE_TYPE sequenceType,
      final OSequence.CreateParams params) {
    init(database);

    final String key = iName.toUpperCase();
    validateSequenceNoExists(key);

    final OSequence sequence = OSequenceHelper.createSequence(sequenceType, params, null).setName(iName);
    sequence.save();
    sequences.put(key, sequence);

    return sequence;
  }

  @Override
  public void dropSequence(final String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropSequence(final ODatabaseDocumentInternal database, final String iName) {
    final String seqName = iName.toUpperCase();
    final OSequence seq = getSequence(database, seqName);

    if (seq != null) {
      seq.getDocument().delete();
      sequences.remove(seqName);
    }
  }

  public OSequence onSequenceCreated(final ODatabaseDocumentInternal database, final ODocument iDocument) {
    init(database);

    final OSequence sequence = OSequenceHelper.createSequence(iDocument);

    final String name = sequence.getName().toUpperCase();
    validateSequenceNoExists(name);

    sequences.put(name, sequence);

    return sequence;
  }

  public OSequence onSequenceUpdated(final ODatabaseDocumentInternal database, final ODocument iDocument) {
    final String name = OSequence.getSequenceName(iDocument);
    if (name == null) {
      return null;
    }
    final OSequence sequence = getSequence(name);
    if (sequence == null) {
      return null;
    }

    sequence.onUpdate(iDocument);

    return sequence;
  }

  public void onSequenceDropped(final ODatabaseDocumentInternal database, final ODocument iDocument) {
    final String name = OSequence.getSequenceName(iDocument);
    validateSequenceExists(name);

    sequences.remove(name);
  }

  private void init(final ODatabaseDocumentInternal database) {
    if (database.getMetadata().getSchema().existsClass(OSequence.CLASS_NAME)) {
      return;
    }

    final OClassImpl sequenceClass = (OClassImpl) database.getMetadata().getSchema().createClass(OSequence.CLASS_NAME);
    OSequence.initClass(sequenceClass);
  }

  private void validateSequenceNoExists(final String iName) {
    if (sequences.containsKey(iName)) {
      throw new OSequenceException("Sequence '" + iName + "' already exists");
    }
  }

  private void validateSequenceExists(final String iName) {
    if (!sequences.containsKey(iName)) {
      throw new OSequenceException("Sequence '" + iName + "' does not exists");
    }
  }

}