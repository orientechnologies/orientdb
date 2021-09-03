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

package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OClientConnection;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous command result manager. As soon as a record is returned by the command is sent over
 * the wire.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OAsyncCommandResultListener extends OAbstractCommandResultListener {

  private final ONetworkProtocolBinary protocol;
  private final AtomicBoolean empty = new AtomicBoolean(true);
  private final int txId;
  private final Set<ORID> alreadySent = new HashSet<ORID>();
  private final OClientConnection connection;

  public OAsyncCommandResultListener(
      OClientConnection connection, final OCommandResultListener wrappedResultListener) {
    super(wrappedResultListener);
    this.protocol = (ONetworkProtocolBinary) connection.getProtocol();
    this.txId = connection.getId();
    this.connection = connection;
  }

  @Override
  public boolean result(final Object iRecord) {
    empty.compareAndSet(true, false);

    try {
      fetchRecord(
          iRecord,
          new ORemoteFetchListener() {
            @Override
            protected void sendRecord(ORecord iLinked) {
              if (!alreadySent.contains(iLinked.getIdentity())) {
                alreadySent.add(iLinked.getIdentity());
                try {
                  protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
                  protocol.writeIdentifiable(protocol.channel, connection, iLinked);
                } catch (IOException e) {
                  OLogManager.instance().error(this, "Cannot write against channel", e);
                }
              }
            }
          });
      alreadySent.add(((OIdentifiable) iRecord).getIdentity());
      protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
      protocol.writeIdentifiable(
          protocol.channel, connection, ((OIdentifiable) iRecord).getRecord());
      protocol.channel.flush(); // TODO review this flush... it's for non blocking...

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

  @Override
  public void linkdedBySimpleValue(ODocument doc) {
    ORemoteFetchListener listener =
        new ORemoteFetchListener() {
          @Override
          protected void sendRecord(ORecord iLinked) {
            if (!alreadySent.contains(iLinked.getIdentity())) {
              alreadySent.add(iLinked.getIdentity());
              try {
                protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
                protocol.writeIdentifiable(protocol.channel, connection, iLinked);
              } catch (IOException e) {
                OLogManager.instance().error(this, "Cannot write against channel", e);
              }
            }
          }

          @Override
          public void parseLinked(
              ODocument iRootRecord,
              OIdentifiable iLinked,
              Object iUserObject,
              String iFieldName,
              OFetchContext iContext)
              throws OFetchException {
            if (iLinked instanceof ORecord) sendRecord((ORecord) iLinked);
          }

          @Override
          public void parseLinkedCollectionValue(
              ODocument iRootRecord,
              OIdentifiable iLinked,
              Object iUserObject,
              String iFieldName,
              OFetchContext iContext)
              throws OFetchException {
            if (iLinked instanceof ORecord) sendRecord((ORecord) iLinked);
          }
        };
    final OFetchContext context = new ORemoteFetchContext();
    OFetchHelper.fetch(doc, doc, OFetchHelper.buildFetchPlan(""), listener, context, "");
  }
}
