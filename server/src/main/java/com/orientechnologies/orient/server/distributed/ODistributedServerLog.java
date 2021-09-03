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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import java.util.logging.Level;

/**
 * Distributed logger.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODistributedServerLog {
  public enum DIRECTION {
    NONE,
    IN,
    OUT,
    BOTH
  }

  public static boolean isDebugEnabled() {
    return OLogManager.instance().isDebugEnabled();
  }

  public static void debug(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.FINE,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            null,
            true,
            null,
            iAdditionalArgs);
  }

  public static void debug(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.FINE,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            iException,
            true,
            null,
            iAdditionalArgs);
  }

  public static void info(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.INFO,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            null,
            true,
            null,
            iAdditionalArgs);
  }

  public static void info(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.INFO,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            iException,
            true,
            null,
            iAdditionalArgs);
  }

  public static void warn(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.WARNING,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            null,
            true,
            null,
            iAdditionalArgs);
  }

  public static void warn(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.WARNING,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            iException,
            true,
            null,
            iAdditionalArgs);
  }

  public static void error(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.SEVERE,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            null,
            true,
            null,
            iAdditionalArgs);
  }

  public static void error(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    OLogManager.instance()
        .log(
            iRequester,
            Level.SEVERE,
            formatMessage(iRequester, iLocalNode, iRemoteNode, iDirection, iMessage),
            iException,
            true,
            null,
            iAdditionalArgs);
  }

  protected static String formatMessage(
      final Object iRequester,
      final String iLocalNode,
      final String iRemoteNode,
      final DIRECTION iDirection,
      final String iMessage) {
    final StringBuilder message = new StringBuilder(256);

    if (iLocalNode != null) {
      message.append('[');
      message.append(iLocalNode);
      message.append(']');
    }

    if (iRemoteNode != null && !iRemoteNode.equals(iLocalNode)) {
      switch (iDirection) {
        case IN:
          message.append("<-");
          break;
        case OUT:
          message.append("->");
          break;
        case BOTH:
          message.append("<>");
          break;
        case NONE:
          message.append("--");
          break;
      }

      message.append('[');
      message.append(iRemoteNode);
      message.append(']');
    }

    message.append(' ');
    message.append(iMessage);

    return message.toString();
  }
}
