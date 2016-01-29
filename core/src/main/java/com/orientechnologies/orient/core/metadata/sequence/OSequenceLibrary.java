package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Set;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public interface OSequenceLibrary {
  Set<String> getSequenceNames();

  int getSequenceCount();

  OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params);

  OSequence getSequence(String iName);

  void dropSequence(String iName);

  OSequence onSequenceCreated(ODocument iDocument);

  OSequence onSequenceUpdated(ODocument iDocument);

  void onSequenceDropped(ODocument iDocument);

  void create();

  void load();

  void close();
}
