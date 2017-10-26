/*
 * Copyright 2010-2013 OrientDB LTD (info--at--orientdb.com)
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
package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.tx.OTransactionInternal;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12.06.13
 */
public class OStorageTransaction {
  private final OTransactionInternal clientTx;
  private final OMicroTransaction    microTransaction;

  public OStorageTransaction(OTransactionInternal clientTx) {
    this.clientTx = clientTx;
    this.microTransaction = null;
  }

  /**
   * Instantiates a new storage transaction for the given micro-transaction.
   *
   * @param microTransaction the micro-transaction.
   */
  public OStorageTransaction(OMicroTransaction microTransaction) {
    this.microTransaction = microTransaction;
    this.clientTx = null;
  }

  public OTransactionInternal getClientTx() {
    return clientTx;
  }

  /**
   * @return the micro-transaction associated with this storage transaction or {@code null} if there are no such transaction.
   */
  public OMicroTransaction getMicroTransaction() {
    return microTransaction;
  }
}
