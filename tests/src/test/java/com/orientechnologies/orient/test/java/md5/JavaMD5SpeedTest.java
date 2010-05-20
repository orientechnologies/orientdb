/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.test.java.md5;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;

public class JavaMD5SpeedTest extends SpeedTestMonoThread {
	private MessageDigest				md;
	private final static byte[]	RESULT	= { 33, 35, 47, 41, 122, 87, -91, -89, 67, -119, 74, 14, 74, -128, 31, -61 };

	public JavaMD5SpeedTest() {
		super(1000000);
	}

	@Override
	@Test(enabled = false)
	public void cycle() {
		md.reset();
		md.update("admin".getBytes());
		byte[] result = md.digest();

		for (int i = 0; i < RESULT.length; ++i)
			Assert.assertTrue(result[i] == RESULT[i]);
	}

	@Override
	public void init() throws NoSuchAlgorithmException {
		md = MessageDigest.getInstance("MD5");
	}
}
