/*
 *
 * Copyright 2011 Luca Molino (luca.molino--AT--assetdata.it)
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
package com.orientechnologies.orient.server.network.protocol.http.multipart;

import java.io.IOException;

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

/**
 * @author luca.molino
 * 
 */
public class OHttpMultipartHelper {

	protected static boolean isMultipartPartHeader(StringBuilder header) {
		final String linePart = header.toString().trim();
		return ((linePart.equals(OHttpUtils.MULTIPART_CONTENT_CHARSET)) || (linePart.equals(OHttpUtils.MULTIPART_CONTENT_FILENAME))
				|| (linePart.equals(OHttpUtils.MULTIPART_CONTENT_NAME)) || (linePart.equals(OHttpUtils.MULTIPART_CONTENT_TYPE))
				|| (linePart.equals(OHttpUtils.MULTIPART_CONTENT_DISPOSITION)) || (linePart
				.equals(OHttpUtils.MULTIPART_CONTENT_TRANSFER_ENCODING)));
	}

	public static boolean isEndRequest(final OHttpRequest iRequest) throws IOException {
		int in = iRequest.multipartStream.read();
		if (((char) in) == '-') {
			in = iRequest.multipartStream.read();
			if (((char) in) == '-') {
				in = iRequest.multipartStream.read();
				if (((char) in) == '\r') {
					in = iRequest.multipartStream.read();
					if (((char) in) == '\n') {
						return true;
					} else {
						iRequest.multipartStream.setSkipInput(in);
					}
				} else {
					iRequest.multipartStream.setSkipInput(in);
				}
			} else {
				iRequest.multipartStream.setSkipInput(in);
			}
		} else {
			iRequest.multipartStream.setSkipInput(in);
		}
		return false;
	}

}
