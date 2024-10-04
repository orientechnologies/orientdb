/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.IndexEngineValidator;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * Dictionary index similar to unique index but does not check for updates, just executes changes.
 * Last put always wins and override the previous value.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexDictionary extends OIndexOneValue {

  public OIndexDictionary(OIndexMetadata im, final OStorage storage) {
    super(im, storage);
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.Dictionary);
  }

  @Override
  public IndexEngineValidator<Object, ORID> getUniqueValidator() {
    return null;
  }
}
