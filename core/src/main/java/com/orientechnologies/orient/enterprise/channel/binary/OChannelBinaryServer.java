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

package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class OChannelBinaryServer extends OChannelBinary {

  public OChannelBinaryServer(final Socket iSocket, final OContextConfiguration iConfig)
      throws IOException {
    super(iSocket, iConfig);

    if (socketBufferSize > 0) {
      inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
      outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);
    } else {
      inStream = new BufferedInputStream(socket.getInputStream());
      outStream = new BufferedOutputStream(socket.getOutputStream());
    }

    out = new DataOutputStream(outStream);
    in = new DataInputStream(inStream);
    connected();
  }
}
