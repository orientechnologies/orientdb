package com.orientechnologies.orient.core.util;

import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

public class StringBenchmarks extends BaseMicroBenchmarks {

    public static final Random RANDOM = new Random();
    @Benchmark
    public void testConcatenationString() {
        String s = "test" + getRandomString();
        print(s);
    }

    @Benchmark
    public void testConcatenationCharacter() {
        String s = "test" + getRandomChar();
        print(s);
    }


    private String getRandomString() {
        final int r = RANDOM.nextInt(3);
        if (r == 0) {
            return "0";
        } else if (r == 1) {
            return "1";
        }
        return "2";
    }

    private char getRandomChar() {
        final int r = RANDOM.nextInt(3);
        if (r == 0) {
            return '0';
        } else if (r == 1) {
            return '1';
        }
        return '2';
    }


    private void print(final String s) {
        if (s == null) {
            System.out.println("s = " + s);
        }
    }

    public static void main(String[] args) throws RunnerException {
        BaseMicroBenchmarks.main(StringBenchmarks.class.getSimpleName());
    }

}
