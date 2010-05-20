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
package com.orientechnologies.orient.test.java.lang;

import java.io.IOException;
import java.lang.reflect.Field;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.test.domain.business.Account;

public class ReflectionSpeedTest extends SpeedTestMonoThread {
	private Field		field;
	private Account	account	= new Account();

	public ReflectionSpeedTest() throws SecurityException, NoSuchFieldException {
		super(10000000);
		field = Account.class.getDeclaredField("name");
		if (!field.isAccessible())
			field.setAccessible(true);
	}

	@Override
	@Test(enabled = false)
	public void cycle() throws IOException, IllegalArgumentException, IllegalAccessException {
		field.set(account, "Set test");
	}
}
