/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Static helper class to transform any kind of basic data in bytes and viceversa.
 * 
 * @author Luca Garulli
 * 
 */
public class OBinaryProtocol {

	public static final int	SIZE_BYTE		= 1;
	public static final int	SIZE_CHAR		= 2;
	public static final int	SIZE_SHORT	= 2;
	public static final int	SIZE_INT		= 4;
	public static final int	SIZE_LONG		= 8;

	public static int string2bytes(final String iInputText, final OutputStream iStream) throws IOException {
		if (iInputText == null)
			return -1;

		final int beginOffset = iStream instanceof OMemoryStream ? ((OMemoryStream) iStream).getPosition() : -1;

		final int len = iInputText.length();
		for (int i = 0; i < len; i++) {
			int c = iInputText.charAt(i);

			if (c < 0x80) {
				// 7-bits done in one byte.
				iStream.write(c);
			} else if (c < 0x800) {
				// 8-11 bits done in 2 bytes
				iStream.write(0xC0 | c >> 6);
				iStream.write(0x80 | c & 0x3F);
			} else {
				// 12-16 bits done in 3 bytes
				iStream.write(0xE0 | c >> 12);
				iStream.write(0x80 | c >> 6 & 0x3F);
				iStream.write(0x80 | c & 0x3F);
			}
		}

		return beginOffset;
	}

	public static final byte[] string2bytes(final String iInputText) {
		if (iInputText == null)
			return null;

		final int len = iInputText.length();

		// worst case, all chars could require 3-byte encodings.
		final byte[] output = new byte[len * 3];

		// index output[]
		int j = 0;

		for (int i = 0; i < len; i++) {
			int c = iInputText.charAt(i);

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

	public static final String bytes2string(final OMemoryStream input, final int iLenght) {
		final char[] output = new char[iLenght];
		// index input[]
		int i = 0;
		// index output[]
		int j = 0;
		while (i < iLenght) {
			// get next byte unsigned
			int b = input.getAsByte() & 0xff;
			i++;
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
				int x = input.getAsByte() & 0x3f;
				i++;
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
				y = input.getAsByte() & 0x3f;
				i++;
				// use low order 6 bits of the next byte
				// It should have high order bits 10, which we don't check.
				x = input.getAsByte() & 0x3f;
				i++;
				// zzzzyyyy yyxxxxxx
				final int asint = (z << 12 | y << 6 | x);
				output[j++] = (char) asint;
				break;
			}// end switch
		}// end while
		return new String(output, 0/* offset */, j/* count */);
	}

	public static final String bytes2string(final byte[] iInput) {
		if (iInput == null)
			return null;

		return OBinaryProtocol.bytes2string(iInput, 0, iInput.length);
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

	public static byte[] char2bytes(final char value) {
		return OBinaryProtocol.char2bytes(value, new byte[2], 0);
	}

	public static byte[] char2bytes(final char value, final byte[] b, final int iBeginOffset) {
		b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
		b[iBeginOffset + 1] = (byte) ((value >>> 0) & 0xFF);
		return b;
	}

	public static int long2bytes(final long value, final OutputStream iStream) throws IOException {
		final int beginOffset = iStream instanceof OMemoryStream ? ((OMemoryStream) iStream).getPosition() : -1;

		iStream.write((int) (value >>> 56) & 0xFF);
		iStream.write((int) (value >>> 48) & 0xFF);
		iStream.write((int) (value >>> 40) & 0xFF);
		iStream.write((int) (value >>> 32) & 0xFF);
		iStream.write((int) (value >>> 24) & 0xFF);
		iStream.write((int) (value >>> 16) & 0xFF);
		iStream.write((int) (value >>> 8) & 0xFF);
		iStream.write((int) (value >>> 0) & 0xFF);

		return beginOffset;
	}

	public static byte[] long2bytes(final long value) {
		return OBinaryProtocol.long2bytes(value, new byte[8], 0);
	}

	public static byte[] long2bytes(final long value, final byte[] b, final int iBeginOffset) {
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

	public static int int2bytes(final int value, final OutputStream iStream) throws IOException {
		final int beginOffset = iStream instanceof OMemoryStream ? ((OMemoryStream) iStream).getPosition() : -1;

		iStream.write((value >>> 24) & 0xFF);
		iStream.write((value >>> 16) & 0xFF);
		iStream.write((value >>> 8) & 0xFF);
		iStream.write((value >>> 0) & 0xFF);

		return beginOffset;
	}

	public static byte[] int2bytes(final int value) {
		return OBinaryProtocol.int2bytes(value, new byte[4], 0);
	}

	public static byte[] int2bytes(final int value, final byte[] b, final int iBeginOffset) {
		b[iBeginOffset] = (byte) ((value >>> 24) & 0xFF);
		b[iBeginOffset + 1] = (byte) ((value >>> 16) & 0xFF);
		b[iBeginOffset + 2] = (byte) ((value >>> 8) & 0xFF);
		b[iBeginOffset + 3] = (byte) ((value >>> 0) & 0xFF);
		return b;
	}

	public static int short2bytes(final short value, final OutputStream iStream) throws IOException {
		final int beginOffset = iStream instanceof OMemoryStream ? ((OMemoryStream) iStream).getPosition() : -1;
		iStream.write((value >>> 8) & 0xFF);
		iStream.write((value >>> 0) & 0xFF);
		return beginOffset;
	}

	public static byte[] short2bytes(final short value) {
		return OBinaryProtocol.short2bytes(value, new byte[2], 0);
	}

	public static byte[] short2bytes(final short value, final byte[] b, final int iBeginOffset) {
		b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
		b[iBeginOffset + 1] = (byte) ((value >>> 0) & 0xFF);
		return b;
	}

	public static long bytes2long(final byte[] b) {
		return OBinaryProtocol.bytes2long(b, 0);
	}

	public static long bytes2long(final InputStream iStream) throws IOException {
		return ((long) (0xff & iStream.read()) << 56 | (long) (0xff & iStream.read()) << 48 | (long) (0xff & iStream.read()) << 40
				| (long) (0xff & iStream.read()) << 32 | (long) (0xff & iStream.read()) << 24 | (0xff & iStream.read()) << 16
				| (0xff & iStream.read()) << 8 | (0xff & iStream.read()));
	}

	public static long bytes2long(final byte[] b, final int offset) {
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
	public static int bytes2int(final byte[] b) {
		return bytes2int(b, 0);
	}

	public static int bytes2int(final InputStream iStream) throws IOException {
		return ((0xff & iStream.read()) << 24 | (0xff & iStream.read()) << 16 | (0xff & iStream.read()) << 8 | (0xff & iStream.read()));
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
	public static int bytes2int(final byte[] b, final int offset) {
		return (b[offset]) << 24 | (0xff & b[offset + 1]) << 16 | (0xff & b[offset + 2]) << 8 | ((0xff & b[offset + 3]));
	}

	public static int bytes2short(final InputStream iStream) throws IOException {
		return (short) ((iStream.read() << 8) | (iStream.read() & 0xff));
	}

	public static short bytes2short(final byte[] b) {
		return bytes2short(b, 0);
	}

	public static short bytes2short(final byte[] b, final int offset) {
		return (short) ((b[offset] << 8) | (b[offset + 1] & 0xff));
	}

	public static char bytes2char(final byte[] b) {
		return OBinaryProtocol.bytes2char(b, 0);
	}

	public static char bytes2char(final byte[] b, final int offset) {
		return (char) ((b[offset] << 8) + (b[offset + 1] << 0));
	}
}
