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

package com.orientechnologies.orient.core.util;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;


/**
 * Contains information about current host.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OHostInfo {
  /**
   * Gets mac address of current host.
   * 
   * @return mac address
   */
  public static byte[] getMac() {
    try {
      final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        final byte[] mac = networkInterface.getHardwareAddress();
        if (mac != null && mac.length == 6)
          return mac;
      }
    } catch (SocketException e) {
      throw new IllegalStateException("Error during MAC address retrieval.", e);
    }

    throw new IllegalStateException("Node id is possible to generate only on machine which have at least"
        + " one network interface with mac address.");
  }
}
