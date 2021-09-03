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
package com.orientechnologies.orient.enterprise.channel.text;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import java.io.IOException;
import java.net.Socket;

public class OChannelText extends OChannel {

  public OChannelText(final Socket iSocket, final OContextConfiguration iConfig)
      throws IOException {
    super(iSocket, iConfig);
  }

  /**
   * @param iBuffer byte[] to fill
   * @param iStartingPosition Offset to start to fill the buffer
   * @param iContentLength Length of expected content to read
   * @return total of bytes read
   * @throws IOException
   */
  public int read(final byte[] iBuffer, final int iStartingPosition, final int iContentLength)
      throws IOException {
    int pos;
    int read = 0;
    pos = iStartingPosition;

    for (int required = iContentLength; required > 0; required -= read) {
      read = inStream.read(iBuffer, pos, required);
      pos += read;
    }

    updateMetricReceivedBytes(read);
    return pos - iStartingPosition;
  }

  public byte read() throws IOException {
    updateMetricReceivedBytes(1);
    return (byte) inStream.read();
  }

  public byte[] readBytes(final int iTotal) throws IOException {
    final byte[] buffer = new byte[iTotal];
    updateMetricReceivedBytes(iTotal);
    inStream.read(buffer);
    return buffer;
  }

  public OChannelText writeBytes(final byte[] iContent) throws IOException {
    outStream.write(iContent);
    updateMetricTransmittedBytes(iContent.length);
    return this;
  }
}
