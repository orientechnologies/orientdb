/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous command result manager. As soon as a record is returned by the command is sent over the wire.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OAsyncCommandResultListener extends OAbstractCommandResultListener {

  private final ONetworkProtocolBinary protocol;
  private final AtomicBoolean          empty       = new AtomicBoolean(true);
  private final int                    txId;
  private final Set<ORID>              alreadySent = new HashSet<ORID>();

  public OAsyncCommandResultListener(final ONetworkProtocolBinary iNetworkProtocolBinary, final int txId,
      final OCommandResultListener wrappedResultListener) {
    super(wrappedResultListener);
    this.protocol = iNetworkProtocolBinary;
    this.txId = txId;
  }

  @Override
  public boolean result(final Object iRecord) {
    if (empty.compareAndSet(true, false))
      try {
        protocol.sendOk(txId);
      } catch (IOException ignored) {
      }

    try {
      fetchRecord(iRecord, new ORemoteFetchListener() {
        @Override
        protected void sendRecord(ORecord iLinked) {
          if (!alreadySent.contains(iLinked.getIdentity())) {
            alreadySent.add(iLinked.getIdentity());
            try {
              protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
              protocol.writeIdentifiable(iLinked);
            } catch (IOException e) {
              OLogManager.instance().error(this, "Cannot write against channel", e);
            }
          }
        }
      });
      alreadySent.add(((OIdentifiable) iRecord).getIdentity());
      protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
      protocol.writeIdentifiable(((OIdentifiable) iRecord).getRecord());
      protocol.channel.flush();// TODO review this flush... it's for non blocking...

      if (wrappedResultListener != null)
        // NOTIFY THE WRAPPED LISTENER
        wrappedResultListener.result(iRecord);

    } catch (IOException e) {
      return false;
    }

    return true;
  }

  public boolean isEmpty() {
    return empty.get();
  }

}
