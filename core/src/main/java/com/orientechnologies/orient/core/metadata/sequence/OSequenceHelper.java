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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/1/2015
 */
public class OSequenceHelper {
  public static final SEQUENCE_TYPE DEFAULT_SEQUENCE_TYPE = SEQUENCE_TYPE.CACHED;

  private OSequenceHelper(){
  }
  
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