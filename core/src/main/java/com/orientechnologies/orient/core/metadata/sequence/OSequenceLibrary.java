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
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;

import java.util.Set;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public interface OSequenceLibrary {
  Set<String> getSequenceNames();

  int getSequenceCount();

  OSequence createSequence(String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params);

  OSequence getSequence(ODatabaseDocumentInternal database, String iName);

  OSequence createSequence(ODatabaseDocumentInternal database, String iName, SEQUENCE_TYPE sequenceType,
      OSequence.CreateParams params);

  void dropSequence(ODatabaseDocumentInternal database, String iName);

  void create();

  void load(ODatabaseDocumentInternal db);

  void close();

  @Deprecated
  void load();

  @Deprecated
  void dropSequence(String iName);

  @Deprecated
  OSequence getSequence(String iName);
}
