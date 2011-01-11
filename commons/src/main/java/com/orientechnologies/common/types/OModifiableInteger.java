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
package com.orientechnologies.common.types;

/**
 * Modifiable Integer. Like java.lang.Integer but the value is modifiable.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class OModifiableInteger extends Number implements Comparable<OModifiableInteger> {
	public int	value;

	public OModifiableInteger() {
		value = 0;
	}

	public OModifiableInteger(final int iValue) {
		value = iValue;
	}

	public void setValue(final int iValue) {
		value = iValue;
	}

	public int getValue() {
		return value;
	}

	public void increment() {
		value++;
	}

	public void increment(final int iValue) {
		value += iValue;
	}

	public void decrement() {
		value--;
	}

	public void decrement(final int iValue) {
		value -= iValue;
	}

	public int compareTo(final OModifiableInteger anotherInteger) {
		int thisVal = value;
		int anotherVal = anotherInteger.value;

		return (thisVal < anotherVal) ? -1 : ((thisVal == anotherVal) ? 0 : 1);
	}

	@Override
	public byte byteValue() {
		return (byte) value;
	}

	@Override
	public short shortValue() {
		return (short) value;
	}

	@Override
	public float floatValue() {
		return value;
	}

	@Override
	public double doubleValue() {
		return value;
	}

	@Override
	public int intValue() {
		return value;
	}

	@Override
	public long longValue() {
		return value;
	}

	public Integer toInteger() {
		return Integer.valueOf(this.value);
	}

	@Override
	public boolean equals(final Object o) {
		if (o instanceof OModifiableInteger) {
			return value == ((OModifiableInteger) o).value;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}
}
