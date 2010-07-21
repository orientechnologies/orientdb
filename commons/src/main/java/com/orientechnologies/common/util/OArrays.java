package com.orientechnologies.common.util;

import java.lang.reflect.Array;

@SuppressWarnings("unchecked")
public class OArrays {
	public static <T> T[] copyOf(final T[] iSource, final int iNewSize) {
		return (T[]) copyOf(iSource, iNewSize, iSource.getClass());
	}

	public static <T, U> T[] copyOf(final U[] iSource, final int iNewSize, final Class<? extends T[]> iNewType) {
		final T[] copy = ((Object) iNewType == (Object) Object[].class) ? (T[]) new Object[iNewSize] : (T[]) Array.newInstance(
				iNewType.getComponentType(), iNewSize);
		System.arraycopy(iSource, 0, copy, 0, Math.min(iSource.length, iNewSize));
		return copy;
	}

	public static <S> S[] copyOfRange(final S[] iSource, final int iBegin, final int iEnd) {
		return copyOfRange(iSource, iBegin, iEnd, (Class<S[]>) iSource.getClass());
	}

	public static <D, S> D[] copyOfRange(final S[] iSource, final int iBegin, final int iEnd, final Class<? extends D[]> iClass) {
		final int newLength = iEnd - iBegin;
		if (newLength < 0)
			throw new IllegalArgumentException(iBegin + " > " + iEnd);
		final D[] copy = ((Object) iClass == (Object) Object[].class) ? (D[]) new Object[newLength] : (D[]) Array.newInstance(
				iClass.getComponentType(), newLength);
		System.arraycopy(iSource, iBegin, copy, 0, Math.min(iSource.length - iBegin, newLength));
		return copy;
	}

	public static byte[] copyOfRange(final byte[] iSource, final int iBegin, final int iEnd) {
		final int newLength = iEnd - iBegin;
		if (newLength < 0)
			throw new IllegalArgumentException(iBegin + " > " + iEnd);
		final byte[] copy = new byte[newLength];
		System.arraycopy(iSource, iBegin, copy, 0, Math.min(iSource.length - iBegin, newLength));
		return copy;
	}

	public static int[] copyOf(final int[] iSource, final int iNewSize) {
		final int[] copy = new int[iNewSize];
		System.arraycopy(iSource, 0, copy, 0, Math.min(iSource.length, iNewSize));
		return copy;
	}
}
