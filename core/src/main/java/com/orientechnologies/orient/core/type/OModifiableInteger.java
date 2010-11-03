package com.orientechnologies.orient.core.type;

/**
 * 
 * @author Luca
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

	public byte byteValue() {
		return (byte) value;
	}

	public short shortValue() {
		return (short) value;
	}

	public float floatValue() {
		return value;
	}

	public double doubleValue() {
		return value;
	}

	public int intValue() {
		return value;
	}

	public long longValue() {
		return value;
	}

	public Integer toInteger() {
		return Integer.valueOf(this.value);
	}

	public boolean equals(final Object o) {
		if (o instanceof OModifiableInteger) {
			return value == ((OModifiableInteger) o).value;
		}
		return false;
	}

	public int hashCode() {
		return value;
	}

	public String toString() {
		return String.valueOf(this.value);
	}
}
