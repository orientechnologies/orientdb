/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.record.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;

import junit.framework.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author bogdan
 */
public class ORecordBytesTest {
	private static final int SMALL_ARRAY = 3;
	private static final int BIG_ARRAY = 7;
	private static final int FULL_ARRAY = 5;
	private InputStream inputStream;
	private InputStream emptyStream;
	private ORecordBytes testedInstance;

	@BeforeMethod
	public void setUp() throws Exception {
		inputStream = new ByteArrayInputStream(new byte[]{1, 2, 3, 4, 5});
		emptyStream = new ByteArrayInputStream(new byte[]{});
		testedInstance = new ORecordBytes();
	}

	@Test
	public void testFromInputStream_ReadEmpty() throws Exception {
		final int result = testedInstance.fromInputStream(emptyStream, SMALL_ARRAY);
		Assert.assertEquals(0, result);
		final byte[] source = (byte[]) getFieldValue(testedInstance, "_source");
		Assert.assertEquals(0, source.length);
	}

	@Test
	public void testFromInputStream_ReadSmall() throws Exception {
		final int result = testedInstance.fromInputStream(inputStream, SMALL_ARRAY);
		Assert.assertEquals(SMALL_ARRAY, result);
		final byte[] source = (byte[]) getFieldValue(testedInstance, "_source");
		Assert.assertEquals(SMALL_ARRAY, source.length);
		for (int i = 1; i < SMALL_ARRAY + 1; i++) {
			Assert.assertEquals(i, source[i - 1]);
		}
	}

	@Test
	public void testFromInputStream_ReadBig() throws Exception {
		final int result = testedInstance.fromInputStream(inputStream, BIG_ARRAY);
		Assert.assertEquals(FULL_ARRAY, result);
		final byte[] source = (byte[]) getFieldValue(testedInstance, "_source");
		Assert.assertEquals(FULL_ARRAY, source.length);
		for (int i = 1; i < FULL_ARRAY + 1; i++) {
			Assert.assertEquals(i, source[i - 1]);
		}
	}

	@Test
	public void testFromInputStream_ReadFull() throws Exception {
		final int result = testedInstance.fromInputStream(inputStream, FULL_ARRAY);
		Assert.assertEquals(FULL_ARRAY, result);
		final byte[] source = (byte[]) getFieldValue(testedInstance, "_source");
		Assert.assertEquals(FULL_ARRAY, source.length);
		for (int i = 1; i < FULL_ARRAY + 1; i++) {
			Assert.assertEquals(i, source[i - 1]);
		}
	}

	private static Object getFieldValue(Object source, String fieldName) throws NoSuchFieldException, IllegalAccessException {
		final Class<?> clazz = source.getClass();
		final Field field = getField(clazz, fieldName);
		field.setAccessible(true);
		return field.get(source);
	}

	private static Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
		if (clazz == null) {
			throw new NoSuchFieldException(fieldName);
		}
		for (Field item : clazz.getDeclaredFields()) {
			if (item.getName().equals(fieldName)) {
				return item;
			}
		}
		return getField(clazz.getSuperclass(), fieldName);
	}
}
