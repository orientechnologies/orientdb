package com.orientechnologies.orient.core.sql.parser;

import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;

import com.diffblue.deeptestutils.Reflector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@RunWith(PowerMockRunner.class)
public class JavaCharStreamTest {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();
    @Rule
    public final Timeout globalTimeout = new Timeout(10000);

    /* testedClasses: JavaCharStream */
    // Test written by Diffblue Cover.
    @Test
    public void adjustBeginLineColumnInputPositivePositiveOutputVoid()
            throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {1, 1, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = -133_693_441;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {25, 16_779_281, 16_777_224, 16_777_235,
                16_777_240, 16_777_737, 16_777_241};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 134_217_732;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        final char[] myCharArray = {};
        objectUnderTest.nextCharBuf = myCharArray;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 524_292;
        objectUnderTest.maxNextCharInd = 0;
        final int newLine = 16_777_240;
        final int newCol = 1;

        // Act
        objectUnderTest.adjustBeginLineColumn(newLine, newCol);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(25, objectUnderTest.line);
        Assert.assertEquals(1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void adjustBeginLineColumnInputPositivePositiveOutputVoid2()
            throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {1, 1, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 2_013_790_207;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {25, 16_779_281, 16_777_224, 16_777_235,
                16_777_240, 16_777_737, 16_777_241};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = -2_013_265_916;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        final char[] myCharArray = {};
        objectUnderTest.nextCharBuf = myCharArray;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 524_292;
        objectUnderTest.maxNextCharInd = 0;
        final int newLine = 16_777_240;
        final int newCol = 1;

        // Act
        objectUnderTest.adjustBeginLineColumn(newLine, newCol);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(25, objectUnderTest.line);
        Assert.assertEquals(1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void adjustBeginLineColumnInputPositivePositiveOutputVoid3()
            throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {1, 1, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 4;
        objectUnderTest.bufsize = 1;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {17};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = -6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        final char[] myCharArray = {};
        objectUnderTest.nextCharBuf = myCharArray;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = -1;
        objectUnderTest.maxNextCharInd = 0;
        final int newLine = 16_777_216;
        final int newCol = 1;

        // Act
        objectUnderTest.adjustBeginLineColumn(newLine, newCol);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertArrayEquals(new int[]{16_777_216}, objectUnderTest.bufline);
        Assert.assertEquals(16_777_216, objectUnderTest.line);
        Assert.assertEquals(1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void adjustBeginLineColumnInputPositivePositiveOutputVoid4()
            throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {1, 1, 0, 0, 1, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 270_749_418;
        objectUnderTest.bufsize = -2_251_672;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {58_917_457, 58_917_457, 61_014_609,
                61_014_609, 58_917_457, 58_917_457};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 555_614_208;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        final char[] myCharArray = {};
        objectUnderTest.nextCharBuf = myCharArray;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 824_111_952;
        objectUnderTest.maxNextCharInd = 0;
        final int newLine = 61_014_609;
        final int newCol = 1;

        // Act
        objectUnderTest.adjustBeginLineColumn(newLine, newCol);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertArrayEquals(
                new int[]{61_014_609, 61_014_609, 61_014_610, 61_014_610, 58_917_457, 58_917_457},
                objectUnderTest.bufline);
        Assert.assertEquals(61_014_610, objectUnderTest.line);
    }

    // Test written by Diffblue Cover.
    @Test
    public void AdjustBuffSizeOutputVoid() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 1;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        objectUnderTest.AdjustBuffSize();

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(0, objectUnderTest.available);
    }

    // Test written by Diffblue Cover.
    @Test
    public void AdjustBuffSizeOutputVoid2() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = -2_147_483_647;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        objectUnderTest.AdjustBuffSize();

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(0, objectUnderTest.available);
    }

    // Test written by Diffblue Cover.
    @Test
    public void AdjustBuffSizeOutputVoid3() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = -2_147_483_648;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = -2_147_483_648;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 536_870_912;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        objectUnderTest.AdjustBuffSize();

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(536_870_912, objectUnderTest.available);
    }

    // Test written by Diffblue Cover.
    @Test
    public void backupInputZeroOutputVoid() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final int amount = 0;

        // Act
        objectUnderTest.backup(amount);

        // Method returns void, testing that no exception is thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void backupInputZeroOutputVoid2() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = -2_147_483_648;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = -2_147_483_648;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final int amount = 0;

        // Act
        objectUnderTest.backup(amount);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(0, objectUnderTest.bufpos);
    }

    // Test written by Diffblue Cover.
    @Test
    public void BeginTokenOutputNotNull() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 2_097_152;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 27;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final char retval = objectUnderTest.BeginToken();

        // Assert side effects
        Assert.assertEquals(2_097_151, objectUnderTest.inBuf);
        Assert.assertEquals(28, objectUnderTest.bufpos);
        Assert.assertEquals(28, objectUnderTest.tokenBegin);

        // Assert result
        Assert.assertEquals('\u0000', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void BeginTokenOutputNotNull2() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 262_144;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = -1;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final char retval = objectUnderTest.BeginToken();

        // Assert side effects
        Assert.assertEquals(262_143, objectUnderTest.inBuf);
        Assert.assertEquals(0, objectUnderTest.bufpos);

        // Assert result
        Assert.assertEquals('\u0000', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void BeginTokenOutputNotNull3() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 1;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(512);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = true;
        final int[] myIntArray1 = {0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 22;
        final char[] myCharArray1 = {'\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b',
                '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b',
                '\b', '\b', '\b', '\t', '\b', '\b', '\b', '\b', '\b', '\b'};
        objectUnderTest.nextCharBuf = myCharArray1;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 54;

        // Act
        final char retval = objectUnderTest.BeginToken();

        // Assert side effects
        Assert.assertArrayEquals(new int[]{512}, objectUnderTest.bufcolumn);
        Assert.assertFalse(objectUnderTest.prevCharIsLF);
        Assert.assertArrayEquals(new int[]{1}, objectUnderTest.bufline);
        Assert.assertEquals(1, objectUnderTest.line);
        Assert.assertArrayEquals(new char[]{'\t'}, objectUnderTest.buffer);
        Assert.assertEquals(23, objectUnderTest.nextCharInd);
        Assert.assertEquals(512, objectUnderTest.column);

        // Assert result
        Assert.assertEquals('\t', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void DoneOutputVoid() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        objectUnderTest.Done();

        // Method returns void, testing that no exception is thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void FillBuffOutputIndexOutOfBoundsException()
            throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 1;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        final StringReader stringReader = (StringReader) Reflector.getInstance("java.io.StringReader");
        Reflector.setField(stringReader, "mark", 0);
        Reflector.setField(stringReader, "next", 0);
        Reflector.setField(stringReader, "length", 3);
        Reflector.setField(stringReader, "str", "foo");
        Reflector.setField(stringReader, "skipBuffer", null);
        Reflector.setField(stringReader, "lock", 0);
        objectUnderTest.inputStream = stringReader;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = -2_099_200;

        // Act
        thrown.expect(IndexOutOfBoundsException.class);
        objectUnderTest.FillBuff();

        // Method is not expected to return due to exception thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void getBeginColumnOutputZero() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final int retval = objectUnderTest.getBeginColumn();

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getBeginLineOutputZero() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray = {0};
        objectUnderTest.bufline = myIntArray;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final int retval = objectUnderTest.getBeginLine();

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getColumnOutputZero() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final int retval = objectUnderTest.getColumn();

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getEndColumnOutputZero() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final int retval = objectUnderTest.getEndColumn();

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getEndLineOutputZero() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray = {0};
        objectUnderTest.bufline = myIntArray;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final int retval = objectUnderTest.getEndLine();

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void GetImageOutputNotNull() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = -2_146_566_136;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0001', '\u0000', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final String retval = objectUnderTest.GetImage();

        // Assert result
        Assert.assertEquals("\u0001", retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void GetImageOutputNotNull2() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 1;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0001', '\u0000', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0001'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = -1;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final String retval = objectUnderTest.GetImage();

        // Assert result
        Assert.assertEquals("\u0001", retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void GetImageOutputStringIndexOutOfBoundsException() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 258;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0001', '\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 256;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 257;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        thrown.expect(StringIndexOutOfBoundsException.class);
        objectUnderTest.GetImage();

        // Method is not expected to return due to exception thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void GetImageOutputStringIndexOutOfBoundsException2() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = -2_147_483_390;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0001', '\u0000', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 256;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = -2_147_483_391;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        thrown.expect(StringIndexOutOfBoundsException.class);
        objectUnderTest.GetImage();

        // Method is not expected to return due to exception thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void getLineOutputZero() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray = {0};
        objectUnderTest.bufline = myIntArray;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final int retval = objectUnderTest.getLine();

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @PrepareForTest({JavaCharStream.class, System.class})
    @Test
    public void GetSuffixInputPositiveOutputNullPointerException()
            throws Exception, InvocationTargetException {

        // Setup mocks
        PowerMockito.mockStatic(System.class);

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = -2_147_483_648;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = -2_147_383_369;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final int len = 34_472;
        final NullPointerException nullPointerException = new NullPointerException();
        final NullPointerException nullPointerException1 = new NullPointerException();
        Reflector.setField(nullPointerException1, "cause", nullPointerException1);
        Reflector.setField(nullPointerException1, "detailMessage", null);
        Reflector.setField(nullPointerException, "cause", nullPointerException1);
        Reflector.setField(nullPointerException, "detailMessage", null);
        PowerMockito.doThrow(nullPointerException).when(System.class);
        System.arraycopy(or(isA(Object.class), isNull(Object.class)), anyInt(),
                or(isA(Object.class), isNull(Object.class)), anyInt(), anyInt());

        // Act
        thrown.expect(NullPointerException.class);
        objectUnderTest.GetSuffix(len);

        // Method is not expected to return due to exception thrown
    }

    // Test written by Diffblue Cover.
    @PrepareForTest({JavaCharStream.class, System.class})
    @Test
    public void GetSuffixInputZeroOutputNullPointerException()
            throws Exception, InvocationTargetException {

        // Setup mocks
        PowerMockito.mockStatic(System.class);

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = -1;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final int len = 0;
        final NullPointerException nullPointerException = new NullPointerException();
        final NullPointerException nullPointerException1 = new NullPointerException();
        Reflector.setField(nullPointerException1, "cause", nullPointerException1);
        Reflector.setField(nullPointerException1, "detailMessage", null);
        Reflector.setField(nullPointerException, "cause", nullPointerException1);
        Reflector.setField(nullPointerException, "detailMessage", null);
        PowerMockito.doThrow(nullPointerException).when(System.class);
        System.arraycopy(or(isA(Object.class), isNull(Object.class)), anyInt(),
                or(isA(Object.class), isNull(Object.class)), anyInt(), anyInt());

        // Act
        thrown.expect(NullPointerException.class);
        objectUnderTest.GetSuffix(len);

        // Method is not expected to return due to exception thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTabSizeInputZeroOutputZero() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final int i = 0;

        // Act
        final int retval = objectUnderTest.getTabSize(i);

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput0OutputZero() throws IOException {

        // Arrange
        final char c = '0';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(0, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput1OutputPositive() throws IOException {

        // Arrange
        final char c = '1';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(1, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput2OutputPositive() throws IOException {

        // Arrange
        final char c = '2';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(2, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput3OutputPositive() throws IOException {

        // Arrange
        final char c = '3';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(3, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput4OutputPositive() throws IOException {

        // Arrange
        final char c = '4';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(4, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput5OutputPositive() throws IOException {

        // Arrange
        final char c = '5';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(5, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput6OutputPositive() throws IOException {

        // Arrange
        final char c = '6';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(6, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput7OutputPositive() throws IOException {

        // Arrange
        final char c = '7';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(7, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput8OutputPositive() throws IOException {

        // Arrange
        final char c = '8';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(8, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInput9OutputPositive() throws IOException {

        // Arrange
        final char c = '9';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(9, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInputaOutputPositive() throws IOException {

        // Arrange
        final char c = 'a';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(10, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInputbOutputPositive() throws IOException {

        // Arrange
        final char c = 'b';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(11, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInputcOutputPositive() throws IOException {

        // Arrange
        final char c = 'c';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(12, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInputdOutputPositive() throws IOException {

        // Arrange
        final char c = 'd';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(13, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInputeOutputPositive() throws IOException {

        // Arrange
        final char c = 'e';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(14, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInputfOutputPositive() throws IOException {

        // Arrange
        final char c = 'f';

        // Act
        final int retval = JavaCharStream.hexval(c);

        // Assert result
        Assert.assertEquals(15, retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void hexvalInputNotNullOutputIOException() throws IOException {

        // Arrange
        final char c = '<';

        // Act
        thrown.expect(IOException.class);
        JavaCharStream.hexval(c);

        // Method is not expected to return due to exception thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void ReadByteOutputNotNull() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 1;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray = {0, 0};
        objectUnderTest.bufline = myIntArray;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = -1;
        final char[] myCharArray = {'\u0000', '\u0000', '\u0000'};
        objectUnderTest.nextCharBuf = myCharArray;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 7_536_659;

        // Act
        final char retval = objectUnderTest.ReadByte();

        // Assert side effects
        Assert.assertEquals(0, objectUnderTest.nextCharInd);

        // Assert result
        Assert.assertEquals('\u0000', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void readCharOutputNotNull() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 2_097_152;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0001', '\u0001',
                '\u0001', '\u0001', '\u0001', '\u0001', '\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 27;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final char retval = objectUnderTest.readChar();

        // Assert side effects
        Assert.assertEquals(2_097_151, objectUnderTest.inBuf);
        Assert.assertEquals(28, objectUnderTest.bufpos);

        // Assert result
        Assert.assertEquals('\u0000', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void readCharOutputNotNull2() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 262_144;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {'\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = -1;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;

        // Act
        final char retval = objectUnderTest.readChar();

        // Assert side effects
        Assert.assertEquals(262_143, objectUnderTest.inBuf);
        Assert.assertEquals(0, objectUnderTest.bufpos);

        // Assert result
        Assert.assertEquals('\u0000', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void readCharOutputNotNull3() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 14;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(1);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = true;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = -1;
        final char[] myCharArray = {'\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000',
                '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000',
                '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000',
                '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000',
                '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 14;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 28;
        final char[] myCharArray1 = {'\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b',
                '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b',
                '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\b', '\t'};
        objectUnderTest.nextCharBuf = myCharArray1;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 60;

        // Act
        final char retval = objectUnderTest.readChar();

        // Assert side effects
        Assert.assertArrayEquals(
                new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0},
                objectUnderTest.bufcolumn);
        Assert.assertFalse(objectUnderTest.prevCharIsLF);
        Assert.assertEquals(0, objectUnderTest.line);
        Assert.assertArrayEquals(
                new char[]{'\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000',
                        '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\t',
                        '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000',
                        '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000'},
                objectUnderTest.buffer);
        Assert.assertEquals(15, objectUnderTest.bufpos);
        Assert.assertEquals(29, objectUnderTest.nextCharInd);
        Assert.assertEquals(1, objectUnderTest.column);

        // Assert result
        Assert.assertEquals('\t', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void readCharOutputNotNull4() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 2;
        objectUnderTest.prevCharIsCR = true;
        final int[] myIntArray = {1, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = -1;
        objectUnderTest.bufsize = 2;
        objectUnderTest.prevCharIsLF = true;
        final int[] myIntArray1 = {5, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 4;
        final char[] myCharArray = {'\b', '\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 2;
        final char[] myCharArray1 = {'\\', '\u0000', '\u0000', '\\', 'X'};
        objectUnderTest.nextCharBuf = myCharArray1;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 1_969_225_735;
        objectUnderTest.maxNextCharInd = 261;

        // Act
        final char retval = objectUnderTest.readChar();

        // Assert side effects
        Assert.assertEquals(1_969_225_735, objectUnderTest.available);
        Assert.assertFalse(objectUnderTest.prevCharIsCR);
        Assert.assertArrayEquals(new int[]{1, 1, 0, 0, 0}, objectUnderTest.bufcolumn);
        Assert.assertEquals(0, objectUnderTest.inBuf);
        Assert.assertFalse(objectUnderTest.prevCharIsLF);
        Assert.assertArrayEquals(new int[]{6, 5, 0}, objectUnderTest.bufline);
        Assert.assertEquals(6, objectUnderTest.line);
        Assert.assertArrayEquals(new char[]{'X', '\\'}, objectUnderTest.buffer);
        Assert.assertEquals(1, objectUnderTest.bufpos);
        Assert.assertEquals(4, objectUnderTest.nextCharInd);
        Assert.assertEquals(1, objectUnderTest.column);

        // Assert result
        Assert.assertEquals('\\', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void readCharOutputNotNull5() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 2;
        objectUnderTest.prevCharIsCR = true;
        final int[] myIntArray = {1, 1, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(1);
        objectUnderTest.inBuf = -1;
        objectUnderTest.bufsize = 2;
        objectUnderTest.prevCharIsLF = true;
        final int[] myIntArray1 = {5, 5};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 4;
        final char[] myCharArray = {'\u0000', '\\'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = -1;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 2;
        final char[] myCharArray1 = {'\\', '\u0000', '\u0000', '\\', '\t'};
        objectUnderTest.nextCharBuf = myCharArray1;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 1_969_225_735;
        objectUnderTest.maxNextCharInd = 261;

        // Act
        final char retval = objectUnderTest.readChar();

        // Assert side effects
        Assert.assertFalse(objectUnderTest.prevCharIsCR);
        Assert.assertEquals(0, objectUnderTest.inBuf);
        Assert.assertFalse(objectUnderTest.prevCharIsLF);
        Assert.assertArrayEquals(new int[]{5, 6}, objectUnderTest.bufline);
        Assert.assertEquals(6, objectUnderTest.line);
        Assert.assertArrayEquals(new char[]{'\\', '\t'}, objectUnderTest.buffer);
        Assert.assertEquals(0, objectUnderTest.bufpos);
        Assert.assertEquals(4, objectUnderTest.nextCharInd);
        Assert.assertEquals(1, objectUnderTest.column);

        // Assert result
        Assert.assertEquals('\\', retval);
    }

    // Test written by Diffblue Cover.
    @Test
    public void readCharOutputNotNull6() throws IOException, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 8;
        objectUnderTest.prevCharIsCR = true;
        final int[] myIntArray = {1, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(1);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 98_296;
        objectUnderTest.prevCharIsLF = true;
        final int[] myIntArray1 = {5, 5, 0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 4;
        final char[] myCharArray = {'\\', '\\', '\u0000', '\u0000', '\u0000',
                '\u0000', '\u0000', '\u0000', '\u0000', '\u0000'};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 7;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = -1;
        final char[] myCharArray1 = {'\\', 'u', '4', '4', '0', '9'};
        objectUnderTest.nextCharBuf = myCharArray1;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 5;
        objectUnderTest.maxNextCharInd = 6;

        // Act
        final char retval = objectUnderTest.readChar();

        // Assert side effects
        Assert.assertEquals(98_296, objectUnderTest.available);
        Assert.assertFalse(objectUnderTest.prevCharIsCR);
        Assert.assertArrayEquals(new int[]{1, 1, 0, 0, 0, 0, 0, 0, 1, 1}, objectUnderTest.bufcolumn);
        Assert.assertFalse(objectUnderTest.prevCharIsLF);
        Assert.assertArrayEquals(new int[]{5, 5, 0, 0, 0, 0, 0, 0, 5, 6}, objectUnderTest.bufline);
        Assert.assertEquals(6, objectUnderTest.line);
        Assert.assertArrayEquals(new char[]{'\\', '\\', '\u0000', '\u0000', '\u0000', '\u0000',
                        '\u0000', '\u0000', '\u4409', 'u'},
                objectUnderTest.buffer);
        Assert.assertEquals(8, objectUnderTest.bufpos);
        Assert.assertEquals(5, objectUnderTest.nextCharInd);
        Assert.assertEquals(5, objectUnderTest.column);

        // Assert result
        Assert.assertEquals('\u4409', retval);
    }

    // Test written by Diffblue Cover.
    @PrepareForTest(JavaCharStream.class)
    @Test
    public void ReInitInputNotNullNotNullZeroZeroZeroOutputVoid()
            throws Exception, InvocationTargetException, UnsupportedEncodingException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final ByteArrayInputStream dstream =
                (ByteArrayInputStream) Reflector.getInstance("java.io.ByteArrayInputStream");
        Reflector.setField(dstream, "count", 0);
        Reflector.setField(dstream, "mark", 0);
        Reflector.setField(dstream, "pos", 0);
        Reflector.setField(dstream, "buf", null);
        final String encoding = ",";
        final int startline = 0;
        final int startcolumn = 0;
        final int buffersize = 0;
        final InputStreamReader inputStreamReader = PowerMockito.mock(InputStreamReader.class);
        Reflector.setField(inputStreamReader, "skipBuffer", null);
        Reflector.setField(inputStreamReader, "lock", dstream);
        PowerMockito.whenNew(InputStreamReader.class)
                .withParameterTypes(InputStream.class, String.class)
                .withArguments(or(isA(InputStream.class), isNull(InputStream.class)),
                        or(isA(String.class), isNull(String.class)))
                .thenReturn(inputStreamReader);

        // Act
        objectUnderTest.ReInit(dstream, encoding, startline, startcolumn, buffersize);

        // Assert side effects
        Assert.assertEquals(-1, objectUnderTest.bufpos);
        Assert.assertEquals(-1, objectUnderTest.nextCharInd);
        Assert.assertEquals(-1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @PrepareForTest(JavaCharStream.class)
    @Test
    public void ReInitInputNotNullNullZeroZeroZeroOutputVoid()
            throws Exception, InvocationTargetException, UnsupportedEncodingException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.tabSize = 0;
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final ByteArrayInputStream dstream =
                (ByteArrayInputStream) Reflector.getInstance("java.io.ByteArrayInputStream");
        Reflector.setField(dstream, "count", 0);
        Reflector.setField(dstream, "mark", 0);
        Reflector.setField(dstream, "pos", 0);
        Reflector.setField(dstream, "buf", null);
        final String encoding = null;
        final int startline = 0;
        final int startcolumn = 0;
        final int buffersize = 0;
        final InputStreamReader inputStreamReader = PowerMockito.mock(InputStreamReader.class);
        Reflector.setField(inputStreamReader, "skipBuffer", null);
        Reflector.setField(inputStreamReader, "lock", dstream);
        PowerMockito.whenNew(InputStreamReader.class)
                .withParameterTypes(InputStream.class)
                .withArguments(or(isA(InputStream.class), isNull(InputStream.class)))
                .thenReturn(inputStreamReader);

        // Act
        objectUnderTest.ReInit(dstream, encoding, startline, startcolumn, buffersize);

        // Assert side effects
        Assert.assertEquals(-1, objectUnderTest.bufpos);
        Assert.assertEquals(-1, objectUnderTest.nextCharInd);
        Assert.assertEquals(-1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void ReInitInputNotNullZeroZeroZeroOutputVoid() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final StringReader dstream = (StringReader) Reflector.getInstance("java.io.StringReader");
        Reflector.setField(dstream, "mark", 0);
        Reflector.setField(dstream, "next", 0);
        Reflector.setField(dstream, "length", 1);
        Reflector.setField(dstream, "str", "2");
        Reflector.setField(dstream, "skipBuffer", null);
        Reflector.setField(dstream, "lock", 0);
        final int startline = 0;
        final int startcolumn = 0;
        final int buffersize = 0;

        // Act
        objectUnderTest.ReInit(dstream, startline, startcolumn, buffersize);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(-1, objectUnderTest.bufpos);
        Assert.assertNotNull(objectUnderTest.inputStream);
        Assert.assertEquals(0, Reflector.getInstanceField(objectUnderTest.inputStream, "mark"));
        Assert.assertEquals(0, Reflector.getInstanceField(objectUnderTest.inputStream, "next"));
        Assert.assertEquals(1, Reflector.getInstanceField(objectUnderTest.inputStream, "length"));
        Assert.assertEquals("2", Reflector.getInstanceField(objectUnderTest.inputStream, "str"));
        Assert.assertNull(Reflector.getInstanceField(objectUnderTest.inputStream, "skipBuffer"));
        Assert.assertEquals(0, Reflector.getInstanceField(objectUnderTest.inputStream, "lock"));
        Assert.assertEquals(-1, objectUnderTest.nextCharInd);
        Assert.assertEquals(-1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @PrepareForTest(JavaCharStream.class)
    @Test
    public void ReInitInputNotNullZeroZeroZeroOutputVoid2()
            throws Exception, InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        final char[] myCharArray = {};
        objectUnderTest.buffer = myCharArray;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final ByteArrayInputStream dstream =
                (ByteArrayInputStream) Reflector.getInstance("java.io.ByteArrayInputStream");
        Reflector.setField(dstream, "count", 0);
        Reflector.setField(dstream, "mark", 0);
        Reflector.setField(dstream, "pos", 0);
        Reflector.setField(dstream, "buf", null);
        final int startline = 0;
        final int startcolumn = 0;
        final int buffersize = 0;
        final InputStreamReader inputStreamReader = PowerMockito.mock(InputStreamReader.class);
        Reflector.setField(inputStreamReader, "skipBuffer", null);
        Reflector.setField(inputStreamReader, "lock", dstream);
        PowerMockito.whenNew(InputStreamReader.class)
                .withParameterTypes(InputStream.class)
                .withArguments(or(isA(InputStream.class), isNull(InputStream.class)))
                .thenReturn(inputStreamReader);

        // Act
        objectUnderTest.ReInit(dstream, startline, startcolumn, buffersize);

        // Assert side effects
        Assert.assertEquals(-1, objectUnderTest.bufpos);
        Assert.assertEquals(-1, objectUnderTest.nextCharInd);
        Assert.assertEquals(-1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void setTabSizeInputZeroOutputVoid() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        objectUnderTest.bufcolumn = null;
        objectUnderTest.setTabSize(0);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        objectUnderTest.bufline = null;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 0;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final int i = 0;

        // Act
        objectUnderTest.setTabSize(i);

        // Method returns void, testing that no exception is thrown
    }

    // Test written by Diffblue Cover.
    @Test
    public void UpdateLineColumnInputNotNullOutputVoid() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(1);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = -1;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final char c = '\u0000';

        // Act
        objectUnderTest.UpdateLineColumn(c);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(0, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void UpdateLineColumnInputNotNullOutputVoid2() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(1);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = -1;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final char c = '\t';

        // Act
        objectUnderTest.UpdateLineColumn(c);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertEquals(0, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void UpdateLineColumnInputNotNullOutputVoid3() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(2);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = true;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = -1;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final char c = '\t';

        // Act
        objectUnderTest.UpdateLineColumn(c);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 2, 0}, objectUnderTest.bufcolumn);
        Assert.assertFalse(objectUnderTest.prevCharIsLF);
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 1, 0}, objectUnderTest.bufline);
        Assert.assertEquals(1, objectUnderTest.line);
        Assert.assertEquals(2, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void UpdateLineColumnInputNotNullOutputVoid4() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = true;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(2);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = -1;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final char c = '\t';

        // Act
        objectUnderTest.UpdateLineColumn(c);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertFalse(objectUnderTest.prevCharIsCR);
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 2, 0}, objectUnderTest.bufcolumn);
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 1, 0}, objectUnderTest.bufline);
        Assert.assertEquals(1, objectUnderTest.line);
        Assert.assertEquals(2, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void UpdateLineColumnInputNotNullOutputVoid5() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = true;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(2);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = -1;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final char c = '\r';

        // Act
        objectUnderTest.UpdateLineColumn(c);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 1, 0}, objectUnderTest.bufcolumn);
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 1, 0}, objectUnderTest.bufline);
        Assert.assertEquals(1, objectUnderTest.line);
        Assert.assertEquals(1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void UpdateLineColumnInputNotNullOutputVoid6() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = false;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(2);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final char c = '\n';

        // Act
        objectUnderTest.UpdateLineColumn(c);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 1, 0}, objectUnderTest.bufcolumn);
        Assert.assertTrue(objectUnderTest.prevCharIsLF);
        Assert.assertEquals(1, objectUnderTest.column);
    }

    // Test written by Diffblue Cover.
    @Test
    public void UpdateLineColumnInputNotNullOutputVoid7() throws InvocationTargetException {

        // Arrange
        final JavaCharStream objectUnderTest = (JavaCharStream) Reflector.getInstance(
                "com.orientechnologies.orient.core.sql.parser.JavaCharStream");
        objectUnderTest.available = 0;
        objectUnderTest.prevCharIsCR = true;
        final int[] myIntArray = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufcolumn = myIntArray;
        objectUnderTest.setTabSize(2);
        objectUnderTest.inBuf = 0;
        objectUnderTest.bufsize = 0;
        objectUnderTest.prevCharIsLF = false;
        final int[] myIntArray1 = {0, 0, 0, 0, 0, 0, 0, 0};
        objectUnderTest.bufline = myIntArray1;
        objectUnderTest.line = 0;
        objectUnderTest.buffer = null;
        objectUnderTest.bufpos = 6;
        objectUnderTest.inputStream = null;
        objectUnderTest.nextCharInd = 0;
        objectUnderTest.nextCharBuf = null;
        objectUnderTest.column = 0;
        objectUnderTest.tokenBegin = 0;
        objectUnderTest.maxNextCharInd = 0;
        final char c = '\n';

        // Act
        objectUnderTest.UpdateLineColumn(c);

        // Method returns void, testing that no exception is thrown

        // Assert side effects
        Assert.assertFalse(objectUnderTest.prevCharIsCR);
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 1, 0}, objectUnderTest.bufcolumn);
        Assert.assertTrue(objectUnderTest.prevCharIsLF);
        Assert.assertEquals(1, objectUnderTest.column);
    }
}
