package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

  }

  public void create(ODatabaseDocumentInternal database) {
    init(database);
  }

  @Override
  public void load() {
     throw new UnsupportedOperationException("use api with database for internal");
  }

  public void load(ODatabaseDocumentInternal database) {
    sequences.clear();

    //
    if (((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().existsClass(OSequence.CLASS_NAME)) {
      List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + OSequence.CLASS_NAME));
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
    return sequences.keySet();
  }

  @Override
  public int getSequenceCount() {
    return sequences.size();
  }


  public OSequence getSequence(ODatabaseDocumentInternal database, String iName) {
    final OSequence seq = sequences.get(iName.toUpperCase());
    if (seq == null)
      load(database);

    return sequences.get(iName.toUpperCase());
  }

  @Override
  public OSequence getSequence(String iName) {
      throw new UnsupportedOperationException();
  }

  @Override
  public OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params) {
    throw new UnsupportedOperationException("use api with database for internal");
  }

  public OSequence createSequence(ODatabaseDocumentInternal database, String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params) {
    init(database);

    final String key = iName.toUpperCase();
    validateSequenceNoExists(key);

    final OSequence sequence = OSequenceHelper.createSequence(sequenceType, params, null).setName(iName);
    sequence.save();
    sequences.put(key, sequence);

    return sequence;
  }

  @Override
  public void dropSequence(String iName) {
    throw new UnsupportedOperationException();
  }

  public void dropSequence(ODatabaseDocumentInternal database, String iName) {
    String seqName = iName.toUpperCase();
    OSequence seq = getSequence(database, seqName);

    if (seq != null) {
      seq.getDocument().delete();
      sequences.remove(seqName);
    }
  }

  public OSequence onSequenceCreated(ODatabaseDocumentInternal database, ODocument iDocument) {
    init(database);

    OSequence sequence = OSequenceHelper.createSequence(iDocument);

    final String name = sequence.getName().toUpperCase();
    validateSequenceNoExists(name);

    sequences.put(name, sequence);

    return sequence;
  }

  public OSequence onSequenceUpdated(ODatabaseDocumentInternal database,ODocument iDocument) {
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

  public void onSequenceDropped(ODatabaseDocumentInternal database,ODocument iDocument) {
    String name = OSequence.getSequenceName(iDocument);
    validateSequenceExists(name);

    sequences.remove(name);
  }

  private void init(ODatabaseDocumentInternal database) {
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