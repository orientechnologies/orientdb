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

import java.io.IOException;
 import java.util.concurrent.atomic.AtomicBoolean;

 import com.orientechnologies.common.log.OLogManager;
 import com.orientechnologies.orient.core.command.OCommandResultListener;
 import com.orientechnologies.orient.core.db.record.OIdentifiable;
 import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
 import com.orientechnologies.orient.core.record.ORecord;

/**
  * Asynchronous command result manager. As soon as a record is returned by the command is sent over the wire.
  *
  * @author Luca Garulli (l.garulli--at--orientechnologies.com)
  *
  */
 public class OAsyncCommandResultListener extends OAbstractCommandResultListener {

   private final ONetworkProtocolBinary protocol;
   private final AtomicBoolean          empty = new AtomicBoolean(true);
   private final int                    txId;
   private final OCommandResultListener resultListener;

   public OAsyncCommandResultListener(final ONetworkProtocolBinary iNetworkProtocolBinary, final int txId,
       OCommandResultListener resultListener) {
     this.protocol = iNetworkProtocolBinary;
     this.txId = txId;
     this.resultListener = resultListener;
   }

   @Override
   public boolean result(final Object iRecord) {
     if (empty.compareAndSet(true, false)) {
         try {
             protocol.sendOk(txId);
         } catch (IOException ignored) {
         }
     }

     try {
       fetchRecord(iRecord, new ORemoteFetchListener() {
         @Override
         protected void sendRecord(ORecord iLinked) {
           try {
             if (protocol.connection.data.protocolVersion >= 17) {
               protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
               protocol.writeIdentifiable(iLinked);
             }
           } catch (IOException e) {
             OLogManager.instance().error(this, "Cannot write against channel", e);
           }
         }
       });

       protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
       protocol.writeIdentifiable(((OIdentifiable) iRecord).getRecord());

     } catch (IOException e) {
       return false;
     }

     return true;
   }

   @Override
   public void end() {
     super.end();
     if (resultListener != null) {
         resultListener.end();
     }
   }

   public boolean isEmpty() {
     return empty.get();
   }
 }
