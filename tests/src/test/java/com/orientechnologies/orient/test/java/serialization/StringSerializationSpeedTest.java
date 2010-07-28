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
package com.orientechnologies.orient.test.java.serialization;

import java.io.IOException;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;

public class StringSerializationSpeedTest extends SpeedTestMonoThread {

	public StringSerializationSpeedTest() {
		super(1000000);
	}

	@Override
	@Test(enabled = false)
	public void cycle() throws IOException {
		StringBuilder buffer = new StringBuilder();
		buffer.append(new Integer(300).toString());
		buffer.append(new Boolean(true).toString());
		buffer.append("Questa e una prova di scrittura di una stringa");
		buffer.append(new Float(3.0f).toString());
		buffer.append(new Long(30000000L).toString());

		buffer.toString().getBytes();
	}
}
