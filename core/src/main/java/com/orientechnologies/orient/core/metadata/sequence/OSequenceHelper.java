package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/1/2015
 */
public class OSequenceHelper {
  public static OSequence createSequence(SEQUENCE_TYPE sequenceType, OSequence.CreateParams params, ODocument document) {
    switch (sequenceType) {
      case ORDERED:
        return new OSequenceOrdered(document, params);
      case CACHED:
        return new OSequenceCached(document, params);
      default:
        throw new IllegalArgumentException("sequenceType");
    }
  }

  public static SEQUENCE_TYPE getSequenceTyeFromString(String typeAsString) {
    return SEQUENCE_TYPE.valueOf(typeAsString);
  }

  public static OSequence createSequence(ODocument document) {
    SEQUENCE_TYPE sequenceType = OSequence.getSequenceType(document);
    return createSequence(sequenceType, null, document);
  }
}