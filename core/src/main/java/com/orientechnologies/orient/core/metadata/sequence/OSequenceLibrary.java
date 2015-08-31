package com.orientechnologies.orient.core.metadata.sequence;

import java.util.Set;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public interface OSequenceLibrary {
  public Set<String> getSequenceNames();

  public int getSequenceCount();

  public OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params);

  public OSequence getSequence(String iName);

  public void dropSequence(String iName);

  public OSequence onSequenceCreated(ODocument iDocument);

  public OSequence onSequenceUpdated(ODocument iDocument);

  public void onSequenceDropped(ODocument iDocument);

  public void create();

  public void load();

  public void close();
}