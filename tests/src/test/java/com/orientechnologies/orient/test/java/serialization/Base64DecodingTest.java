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
package com.orientechnologies.orient.test.java.serialization;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

@Test(enabled = false)
public class Base64DecodingTest extends SpeedTestMonoThread {

	private static final String	TEXT	= "Ciao, questa, e, una, prova. Che ne pensi?";
	private byte[]							textAsBytes;
	private String							encoded;

	public Base64DecodingTest() throws InstantiationException, IllegalAccessException {
		super(1000000);
	}

	@Override
	public void cycle() throws IOException {
		byte[] decoded = OBase64Utils.decode(encoded);

		Assert.assertEquals(new String(decoded), TEXT);
	}

	@Override
	public void init() throws NoSuchAlgorithmException {
		textAsBytes = TEXT.getBytes();
		encoded = OBase64Utils.encodeBytes(textAsBytes);
	}
}
