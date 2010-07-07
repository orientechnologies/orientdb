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
package com.orientechnologies.orient.core.serialization;

/**
 * Static helper class to transform any kind of basic data in bytes and viceversa.
 * 
 * @author Luca Garulli
 * 
 */
public class OBinaryProtocol {

	public static final byte[] string2bytes(final String input) {
		if (input == null)
			return null;

		// worst case, all chars could require 3-byte encodings.
		final byte[] output = new byte[input.length() * 3];

		// index output[]
		int j = 0;

		for (int i = 0; i < input.length(); i++) {
			int c = input.charAt(i);

			if (c < 0x80) {
				// 7-bits done in one byte.
				output[j++] = (byte) c;
			} else if (c < 0x800) {
				// 8-11 bits done in 2 bytes
				output[j++] = (byte) (0xC0 | c >> 6);
				output[j++] = (byte) (0x80 | c & 0x3F);
			} else {
				// 12-16 bits done in 3 bytes
				output[j++] = (byte) (0xE0 | c >> 12);
				output[j++] = (byte) (0x80 | c >> 6 & 0x3F);
				output[j++] = (byte) (0x80 | c & 0x3F);
			}
		}// end for
			// Prune back our byte array. For efficiency we could hand item back
			// partly filled, which is only a minor inconvenience to the caller
			// most of the time to save copying the array.
		final byte[] chopped = new byte[j];
		System.arraycopy(output, 0, chopped, 0, j/* length */);
		return chopped;
	}// end encode

	public static final String bytes2string(final byte[] input) {
		if (input == null)
			return null;

		return OBinaryProtocol.bytes2string(input, 0, input.length);
	}

	public static final String bytes2string(final byte[] input, final int iBeginOffset, final int iLenght) {
		final char[] output = new char[iLenght];
		// index input[]
		int i = iBeginOffset;
		// index output[]
		int j = 0;
		while (i < iLenght + iBeginOffset) {
			// get next byte unsigned
			int b = input[i++] & 0xff;
			// classify based on the high order 3 bits
			switch (b >>> 5) {
			default:
				// one byte encoding
				// 0xxxxxxx
				// use just low order 7 bits
				// 00000000 0xxxxxxx
				output[j++] = (char) (b & 0x7f);
				break;
			case 6:
				// two byte encoding
				// 110yyyyy 10xxxxxx
				// use low order 6 bits
				int y = b & 0x1f;
				// use low order 6 bits of the next byte
				// It should have high order bits 10, which we don't check.
				int x = input[i++] & 0x3f;
				// 00000yyy yyxxxxxx
				output[j++] = (char) (y << 6 | x);
				break;
			case 7:
				// three byte encoding
				// 1110zzzz 10yyyyyy 10xxxxxx
				assert (b & 0x10) == 0 : "UTF8Decoder does not handle 32-bit characters";
				// use low order 4 bits
				final int z = b & 0x0f;
				// use low order 6 bits of the next byte
				// It should have high order bits 10, which we don't check.
				y = input[i++] & 0x3f;
				// use low order 6 bits of the next byte
				// It should have high order bits 10, which we don't check.
				x = input[i++] & 0x3f;
				// zzzzyyyy yyxxxxxx
				final int asint = (z << 12 | y << 6 | x);
				output[j++] = (char) asint;
				break;
			}// end switch
		}// end while
		return new String(output, 0/* offset */, j/* count */);
	}

	public static byte[] char2bytes(char value) {
		return OBinaryProtocol.char2bytes(value, new byte[2], 0);
	}

	public static byte[] char2bytes(char value, byte[] b, int iBeginOffset) {
		b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
		b[iBeginOffset + 1] = (byte) ((value >>> 0) & 0xFF);
		return b;
	}

	public static byte[] long2bytes(long value) {
		return OBinaryProtocol.long2bytes(value, new byte[8], 0);
	}

	public static byte[] long2bytes(long value, byte[] b, int iBeginOffset) {
		b[iBeginOffset] = (byte) ((value >>> 56) & 0xFF);
		b[iBeginOffset + 1] = (byte) ((value >>> 48) & 0xFF);
		b[iBeginOffset + 2] = (byte) ((value >>> 40) & 0xFF);
		b[iBeginOffset + 3] = (byte) ((value >>> 32) & 0xFF);
		b[iBeginOffset + 4] = (byte) ((value >>> 24) & 0xFF);
		b[iBeginOffset + 5] = (byte) ((value >>> 16) & 0xFF);
		b[iBeginOffset + 6] = (byte) ((value >>> 8) & 0xFF);
		b[iBeginOffset + 7] = (byte) ((value >>> 0) & 0xFF);
		return b;
	}

	public static byte[] int2bytes(int value) {
		return OBinaryProtocol.int2bytes(value, new byte[4], 0);
	}

	public static byte[] int2bytes(int value, byte[] b, int iBeginOffset) {
		b[iBeginOffset] = (byte) ((value >>> 24) & 0xFF);
		b[iBeginOffset + 1] = (byte) ((value >>> 16) & 0xFF);
		b[iBeginOffset + 2] = (byte) ((value >>> 8) & 0xFF);
		b[iBeginOffset + 3] = (byte) ((value >>> 0) & 0xFF);
		return b;
	}

	public static byte[] short2bytes(short value) {
		return OBinaryProtocol.short2bytes(value, new byte[2], 0);
	}

	public static byte[] short2bytes(short value, byte[] b, int iBeginOffset) {
		b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
		b[iBeginOffset + 1] = (byte) ((value >>> 0) & 0xFF);
		return b;
	}

	public static long bytes2long(byte[] b) {
		return OBinaryProtocol.bytes2long(b, 0);
	}

	public static long bytes2long(byte[] b, int offset) {
		return ((0xff & b[offset + 7]) | (0xff & b[offset + 6]) << 8 | (0xff & b[offset + 5]) << 16
				| (long) (0xff & b[offset + 4]) << 24 | (long) (0xff & b[offset + 3]) << 32 | (long) (0xff & b[offset + 2]) << 40
				| (long) (0xff & b[offset + 1]) << 48 | (long) (0xff & b[offset]) << 56);
	}

	/**
	 * Convert the byte array to an int.
	 * 
	 * @param b
	 *          The byte array
	 * @return The integer
	 */
	public static int bytes2int(byte[] b) {
		return OBinaryProtocol.bytes2int(b, 0);
	}

	/**
	 * Convert the byte array to an int starting from the given offset.
	 * 
	 * @param b
	 *          The byte array
	 * @param offset
	 *          The array offset
	 * @return The integer
	 */
	public static int bytes2int(byte[] b, int offset) {
		return ((0xff & b[offset + 3]) | (0xff & b[offset + 2]) << 8 | (0xff & b[offset + 1]) << 16 | (b[offset]) << 24);
	}

	public static short bytes2short(byte[] b) {
		return OBinaryProtocol.bytes2short(b, 0);
	}

	public static short bytes2short(byte[] b, int offset) {
		return (short) ((b[offset] << 8) | (b[offset + 1] & 0xff));
	}

	public static char bytes2char(byte[] b) {
		return OBinaryProtocol.bytes2char(b, 0);
	}

	public static char bytes2char(byte[] b, int offset) {
		return (char) ((b[offset] << 8) + (b[offset + 1] << 0));
	}
}
