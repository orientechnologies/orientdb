/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import java.util.Map;

/**
 *
 * @author mdjurovi
 */
public abstract class ChangeSerializationHelper {
  
  public static Change createChangeInstance(byte type, int value) {
    switch (type) {
    case AbsoluteChange.TYPE:
      return new AbsoluteChange(value);
    case DiffChange.TYPE:
      return new DiffChange(value);
    default:
      throw new IllegalArgumentException("Change type is incorrect");
    }
  }
  
  public abstract Map<OIdentifiable, Change> deserializeChanges(BytesContainer container);
  public abstract <K extends OIdentifiable> int serializeChanges(Map<K, Change> changes, OBinarySerializer<K> keySerializer, BytesContainer bytes);
  protected abstract int getChangesSerializedSize(int changesCount);
}
