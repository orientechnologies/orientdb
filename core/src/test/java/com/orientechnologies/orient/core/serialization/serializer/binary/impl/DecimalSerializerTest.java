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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author Andrey Lomakin
 * @since 04.04.12
 */
public class DecimalSerializerTest {
	private final static int FIELD_SIZE = 9;
	private static final BigDecimal OBJECT = new BigDecimal(new BigInteger("20"), 2);
	private ODecimalSerializer decimalSerializer;
	private static final byte[] stream = new byte[FIELD_SIZE];

	@BeforeClass
	public void beforeClass() {
		decimalSerializer = new ODecimalSerializer();
	}

	@Test
	public void testFieldSize() {
		Assert.assertEquals(decimalSerializer.getObjectSize(OBJECT), FIELD_SIZE);
	}

	@Test
	public void testSerialize() {
		decimalSerializer.serialize(OBJECT, stream, 0);
		Assert.assertEquals(decimalSerializer.deserialize(stream, 0), OBJECT);
	}
}
