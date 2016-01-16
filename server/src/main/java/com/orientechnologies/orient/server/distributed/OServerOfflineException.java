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
package com.orientechnologies.orient.server.distributed;

/**
  * Exception thrown during distributed operation and the server is not ready to execute an operation because it's not online.
  *
  * @author Luca Garulli (l.garulli--at--orientechnologies.com)
  *
  */
 public class OServerOfflineException extends ODistributedException {
   private static final long serialVersionUID = 1L;
   private String            nodeId;
   private String            nodeStatus;

   public OServerOfflineException() {
   }

   public OServerOfflineException(final String iNodeId, final String iNodeStatus) {
     nodeId = iNodeId;
     nodeStatus = iNodeStatus;
   }

   public OServerOfflineException(final String iNodeId, final String iNodeStatus, final String iMessage) {
     super(iMessage);
     nodeId = iNodeId;
     nodeStatus = iNodeStatus;
   }

   public String getNodeId() {
     return nodeId;
   }

   public String getNodeStatus() {
     return nodeStatus;
   }
 }
