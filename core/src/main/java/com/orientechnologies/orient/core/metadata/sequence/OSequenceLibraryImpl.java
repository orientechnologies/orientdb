package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class OSequenceLibraryImpl implements OSequenceLibrary {
  private final Map<String, OSequence> sequences = new ConcurrentHashMap<String, OSequence>();


  @Override
  public void create() {
    init();
  }

  @Override
  public void load() {
    sequences.clear();

    //
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().existsClass(OSequence.CLASS_NAME)) {
      List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + OSequence.CLASS_NAME));
      for (ODocument document : result) {
        document.reload();

        OSequence sequence = OSequenceHelper.createSequence(document);
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
    return null;
  }

  @Override
  public int getSequenceCount() {
    return sequences.size();
  }

  @Override
  public OSequence getSequence(String iName) {
    return sequences.get(iName.toUpperCase());
  }

  @Override
  public OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params) {
    init();

    final String key = iName.toUpperCase();
    validateSequenceNoExists(key);

    final OSequence sequence = OSequenceHelper.createSequence(sequenceType, params, null).setName(iName);
    sequence.save();
    sequences.put(key, sequence);

    return sequence;
  }

  @Override
  public void dropSequence(String iName) {
    OSequence seq = getSequence(iName);

    seq.getDocument().delete();
    sequences.remove(iName);
  }

  @Override
  public OSequence onSequenceCreated(ODocument iDocument) {
    init();

    OSequence sequence = OSequenceHelper.createSequence(iDocument);

    final String name = sequence.getName().toUpperCase();
    validateSequenceNoExists(name);

    sequences.put(name, sequence);

    return sequence;
  }

  @Override
  public OSequence onSequenceUpdated(ODocument iDocument) {
    String name = OSequence.getSequenceName(iDocument);
    if (name == null) {
      return null;
    }
    OSequence sequence = getSequence(name);
    if (sequence == null) {
      return null;
    }

    sequence.onUpdate(iDocument);

    return sequence;
  }

  @Override
  public void onSequenceDropped(ODocument iDocument) {
    String name = OSequence.getSequenceName(iDocument);
    validateSequenceExists(name);

    sequences.remove(name);
  }

  private void init() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db.getMetadata().getSchema().existsClass(OSequence.CLASS_NAME)) {
      return;
    }

    final OClass sequenceClass = db.getMetadata().getSchema().createClass(OSequence.CLASS_NAME);
    OSequence.initClass(sequenceClass);
  }

  private void validateSequenceNoExists(final String iName) {
    if (sequences.containsKey(iName)) {
      throw new OSequenceException("Sequence " + iName + " already exists!");
    }
  }

  private void validateSequenceExists(final String iName) {
    if (!sequences.containsKey(iName)) {
      throw new OSequenceException("Sequence " + iName + " does not exists!");
    }
  }
}