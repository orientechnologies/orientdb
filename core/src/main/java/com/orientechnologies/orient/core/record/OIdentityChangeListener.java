/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.id.ORID;

/**
 * Listener, which is called when record identity is changed. Identity is changed if new record is saved or if transaction is
 * committed and new record created inside of transaction.
 */
public interface OIdentityChangeListener {
  /**
   * This method is called if record identity is changed.
   * 
   * @param prevRid
   *          Previous identity of given record.
   * @param record
   *          Instance of record identity of which is changed.
   */
  public void onIdentityChanged(ORID prevRid, ORecord<?> record);
}
