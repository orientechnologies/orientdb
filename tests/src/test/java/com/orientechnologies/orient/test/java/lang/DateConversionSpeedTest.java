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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateConversionSpeedTest {
	private static final long	MAX	= 100000000;

	public static final void main(String[] args) throws ParseException {

		long begin = System.currentTimeMillis();

		String timer = String.valueOf(begin);
		String timer2 = String.valueOf(timer + 1000000);

		for (int i = 0; i < MAX; ++i) {
			int result = timer.compareTo(timer2);
		}
		System.out.println("String: " + (System.currentTimeMillis() - begin));
		// ------------------------------------------------------------------------

		begin = System.currentTimeMillis();
		final Date d = new Date(begin);
		for (int i = 0; i < MAX; ++i) {
			final long timerLong = Long.parseLong(timer);
			int result = d.compareTo(new Date(timerLong));
		}
		System.out.println("Date: " + (System.currentTimeMillis() - begin));
		// ------------------------------------------------------------------------

		begin = System.currentTimeMillis();
		final String today = "20110526165400";
		final SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		for (int i = 0; i < MAX; ++i) {
			int result = d.compareTo(df.parse(today));
		}
		System.out.println("String: " + (System.currentTimeMillis() - begin));
		// ------------------------------------------------------------------------

		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(begin);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

		final long roundedTime = c.getTimeInMillis();

		System.out.println("original time.: " + begin);
		System.out.println("rounded time..: " + roundedTime);
		
	}
}
