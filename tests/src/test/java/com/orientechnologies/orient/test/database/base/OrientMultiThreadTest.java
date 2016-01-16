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
package com.orientechnologies.orient.test.database.base;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMultiThreads;
import com.orientechnologies.common.test.SpeedTestThread;

@Test(enabled = false)
public abstract class OrientMultiThreadTest extends SpeedTestMultiThreads {
	public OrientMultiThreadTest(int iCycles, int iThreads, Class<? extends SpeedTestThread> iThreadClass) {
		super(iCycles, iThreads, iThreadClass);
	}
}
