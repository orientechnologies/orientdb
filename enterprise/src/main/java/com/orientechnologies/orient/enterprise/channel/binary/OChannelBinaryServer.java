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


package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.orientechnologies.orient.core.config.OContextConfiguration;

public class OChannelBinaryServer extends OChannelBinary {

	public OChannelBinaryServer(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		super(iSocket, iConfig);

		inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
		outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);

		out = new DataOutputStream(outStream);
		in = new DataInputStream(inStream);
		connected();
	}
}
