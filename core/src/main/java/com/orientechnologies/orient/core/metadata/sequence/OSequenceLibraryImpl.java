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
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class OSequenceLibraryImpl {
  private final Map<String, OSequence> sequences = new ConcurrentHashMap<String, OSequence>();
  private final AtomicBoolean reloadNeeded = new AtomicBoolean(false);

  public void create(ODatabaseDocumentInternal database) {
    init(database);
  }

  public synchronized void load(final ODatabaseDocumentInternal db) {
    sequences.clear();

    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass(OSequence.CLASS_NAME)) {
      try (final OResultSet result = db.query("SELECT FROM " + OSequence.CLASS_NAME)) {
        while (result.hasNext()) {
          OResult res = result.next();

          final OSequence sequence =
              OSequenceHelper.createSequence((ODocument) res.getElement().get());
          sequences.put(sequence.getName().toUpperCase(Locale.ENGLISH), sequence);
        }
      }
    }
  }

  public void close() {
    sequences.clear();
  }

  public synchronized Set<String> getSequenceNames(ODatabaseDocumentInternal database) {
    reloadIfNeeded(database);
    return sequences.keySet();
  }

  public synchronized int getSequenceCount(ODatabaseDocumentInternal database) {
    reloadIfNeeded(database);
    return sequences.size();
  }

  public OSequence getSequence(final ODatabaseDocumentInternal database, final String iName) {
    final String name = iName.toUpperCase(Locale.ENGLISH);
    reloadIfNeeded(database);
    OSequence seq;
    synchronized (this) {
      seq = sequences.get(name);
      if (seq != null && seq.getDocument() != null) {
        ORecord tmp = database.getRecord(seq.getDocument());
        int a = 0;
        ++a;
      }
      if (seq == null) {
        load(database);
        seq = sequences.get(name);
      }
    }

    if (seq != null) {
      seq.bindOnLocalThread();
    }

    return seq;
  }

  public synchronized OSequence createSequence(
      final ODatabaseDocumentInternal database,
      final String iName,
      final SEQUENCE_TYPE sequenceType,
      final OSequence.CreateParams params) {
    init(database);
    reloadIfNeeded(database);

    final String key = iName.toUpperCase(Locale.ENGLISH);
    validateSequenceNoExists(key);

    final OSequence sequence =
        OSequenceHelper.createSequence(sequenceType, params, null).setName(iName);
    sequence.save(database);
    sequences.put(key, sequence);
    if (database.getTransaction().isActive()) {
      this.reloadNeeded.set(true);
    }

    return sequence;
  }

  public synchronized void dropSequence(
      final ODatabaseDocumentInternal database, final String iName) {
    final OSequence seq = getSequence(database, iName);

    if (seq != null) {
      database.delete(seq.getDocument().getIdentity());
      sequences.remove(iName.toUpperCase(Locale.ENGLISH));
    }
  }

  public OSequence onSequenceCreated(
      final ODatabaseDocumentInternal database, final ODocument iDocument) {
    init(database);

    String name = OSequence.getSequenceName(iDocument);
    if (name == null) return null;

    name = name.toUpperCase(Locale.ENGLISH);

    final OSequence seq = getSequence(database, name);

    if (seq != null) return seq;

    final OSequence sequence = OSequenceHelper.createSequence(iDocument);

    sequences.put(name, sequence);
    onSequenceLibraryUpdate(database);
    return sequence;
  }

  public OSequence onSequenceUpdated(
      final ODatabaseDocumentInternal database, final ODocument iDocument) {
    String name = OSequence.getSequenceName(iDocument);
    if (name == null) return null;

    name = name.toUpperCase(Locale.ENGLISH);

    final OSequence sequence = sequences.get(name);
    if (sequence == null) return null;

    sequence.onUpdate(iDocument);
    return sequence;
  }

  public void onSequenceDropped(
      final ODatabaseDocumentInternal database, final ODocument iDocument) {
    String name = OSequence.getSequenceName(iDocument);
    if (name == null) return;

    name = name.toUpperCase(Locale.ENGLISH);

    sequences.remove(name);
    onSequenceLibraryUpdate(database);
  }

  private void init(final ODatabaseDocumentInternal database) {
    if (database.getMetadata().getSchema().existsClass(OSequence.CLASS_NAME)) {
      return;
    }

    final OClassImpl sequenceClass =
        (OClassImpl) database.getMetadata().getSchema().createClass(OSequence.CLASS_NAME);
    OSequence.initClass(sequenceClass);
  }

  private void validateSequenceNoExists(final String iName) {
    if (sequences.containsKey(iName)) {
      throw new OSequenceException("Sequence '" + iName + "' already exists");
    }
  }

  private void onSequenceLibraryUpdate(ODatabaseDocumentInternal database) {
    for (OMetadataUpdateListener one : database.getSharedContext().browseListeners()) {
      one.onSequenceLibraryUpdate(database.getName());
    }
  }

  private void reloadIfNeeded(ODatabaseDocumentInternal database) {
    if (reloadNeeded.get()) {
      load(database);
      reloadNeeded.set(false);
    }
  }

  public void update() {
    reloadNeeded.set(true);
  }
}
