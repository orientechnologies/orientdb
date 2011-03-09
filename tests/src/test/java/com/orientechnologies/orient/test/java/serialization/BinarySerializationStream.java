package com.orientechnologies.orient.test.java.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;

public class BinarySerializationStream {

	public static void main(String[] args) {

		long time = System.currentTimeMillis();
		ByteArrayOutputStream s = new ByteArrayOutputStream();
		try {
			DataOutputStream str = new DataOutputStream(s);
			for (int i = 0; i < 1000000; i++) {
				str.writeBytes("adfsdfsdfadfsdfsdfadfsdfsdfadfsdfsdf");
				str.writeInt(32);
				str.writeLong(32);
				str.writeByte(32);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Data Output Stream " + (System.currentTimeMillis() - time));
		time = System.currentTimeMillis();
		OMemoryOutputStream mou = new OMemoryOutputStream();
		try {

			for (int i = 0; i < 1000000; i++) {
				mou.add("adfsdfsdfadfsdfsdfadfsdfsdfadfsdfsdf");
				mou.add(32);
				mou.add(32l);
				mou.add((byte) 32);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("OMemoryOutputStream " + (System.currentTimeMillis() - time));

		System.out.println("" + s.toByteArray().length + " " + mou.toByteArray().length);

	}
}
