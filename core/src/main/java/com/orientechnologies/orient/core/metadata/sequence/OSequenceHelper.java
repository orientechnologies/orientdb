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

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/1/2015
 */
public class OSequenceHelper {
  public static final SEQUENCE_TYPE DEFAULT_SEQUENCE_TYPE = SEQUENCE_TYPE.CACHED;

  public static OSequence createSequence(
      SEQUENCE_TYPE sequenceType, OSequence.CreateParams params, ODocument document) {
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

  public static boolean updateParamsOnLocal(OSequence.CreateParams params, OSequence seq)
      throws ODatabaseException {
    return seq.updateParams(params, false);
  }

  public static long resetSequenceOnLocal(OSequence seq) throws ODatabaseException {
    return seq.reset(false);
  }

  public static long sequenceNextOnLocal(OSequence seq) throws ODatabaseException {
    return seq.next(false);
  }

  public static long sequenceCurrentOnLocal(OSequence seq) throws ODatabaseException {
    return seq.current(false);
  }

  public static void dropLocalSequence(OSequenceLibrary sequenceLibary, String name)
      throws ODatabaseException {
    if (sequenceLibary instanceof OSequenceLibraryAbstract) {
      ((OSequenceLibraryAbstract) sequenceLibary).dropSequence(name, false);
    } else {
      throw new ODatabaseException(
          "Sequence library invalid class: "
              + sequenceLibary.getClass().getName()
              + ". Sequnce library should implement be derived form OSequenceLibraryAbstract");
    }
  }

  public static OSequence createSequenceOnLocal(
      OSequenceLibrary sequenceLibary,
      String sequenceName,
      SEQUENCE_TYPE sequenceType,
      OSequence.CreateParams params) {
    if (sequenceLibary instanceof OSequenceLibraryAbstract) {
      return ((OSequenceLibraryAbstract) sequenceLibary)
          .createSequence(sequenceName, sequenceType, params, false);
    } else {
      throw new ODatabaseException(
          "Sequence library invalid class: "
              + sequenceLibary.getClass().getName()
              + ". Sequnce library should implement be derived form OSequenceLibraryAbstract");
    }
  }

  public static long sequenceNextWithNewCurrentValueOnLocal(OSequenceCached seq, long currentValue)
      throws ODatabaseException {
    return seq.nextWithNewCurrentValue(currentValue, false);
  }
}
