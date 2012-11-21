package com.orientechnologies.orient.core.id;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 16.11.12
 */
@Test
public class NodeIdTest {

  public void testOneOneAddValues() {
    ONodeId one = ONodeId.valueOf(1);
    ONodeId two = one.add(one);

    String result = two.toHexString();
    Assert.assertEquals(result, "000000000000000000000000000000000000000000000002");
  }

  public void testAddOverflowValue() {
    ONodeId one = ONodeId.valueOf(0xFFFFFFFFFL);
    ONodeId two = one.add(ONodeId.ONE);

    String result = two.toHexString();
    Assert.assertEquals(result, "000000000000000000000000000000000000001000000000");
  }

  public void testAddOverflowValues() {
    ONodeId one = ONodeId.valueOf(0xFFFFFFFFFL);
    ONodeId two = one.add(ONodeId.valueOf(0xFFFFFFFFFL));

    String result = two.toHexString();
    Assert.assertEquals(result, "000000000000000000000000000000000000001ffffffffe");
  }

  public void testAddOverflow() {
    ONodeId one = ONodeId.MAX_VALUE;
    ONodeId two = one.add(ONodeId.ONE);

    String result = two.toHexString();
    Assert.assertEquals(result, "000000000000000000000000000000000000000000000000");
  }

  public void testAddSamePositiveAndNegativeNumbers() {
    ONodeId one = ONodeId.parseString("1234567895623");
    ONodeId two = ONodeId.parseString("-1234567895623");

    ONodeId result = one.add(two);

    Assert.assertEquals(result, ONodeId.ZERO);
  }

  public void testAddPositiveMoreNegativeNumbers() {
    ONodeId one = ONodeId.parseString("12358971234567895622");
    ONodeId two = ONodeId.parseString("-1234567895623");

    ONodeId result = one.add(two);

    Assert.assertEquals(result, ONodeId.parseString("12358969999999999999"));
  }

  public void testAddPositiveLessNegativeNumbers() {
    ONodeId one = ONodeId.parseString("1234567895623");
    ONodeId two = ONodeId.parseString("-12358971234567895622");

    ONodeId result = one.add(two);

    Assert.assertEquals(result, ONodeId.parseString("-12358969999999999999"));
  }

  public void testAddToZeroPositive() {
    ONodeId one = ONodeId.parseString("1234567895623");
    ONodeId two = ONodeId.parseString("0");

    ONodeId result = one.add(two);

    Assert.assertEquals(result, ONodeId.parseString("1234567895623"));
  }

  public void testAddToZeroNegative() {
    ONodeId one = ONodeId.parseString("-1234567895623");
    ONodeId two = ONodeId.parseString("0");

    ONodeId result = one.add(two);

    Assert.assertEquals(result, ONodeId.parseString("-1234567895623"));
  }

	public void testAddZeroToPositive() {
		ONodeId two = ONodeId.ZERO.add(ONodeId.parseString("1234567895623"));

		Assert.assertEquals(two, ONodeId.parseString("1234567895623"));
	}

	public void testAddZeroToNegative() {
		ONodeId two = ONodeId.ZERO.add(ONodeId.parseString("-1234567895623"));

		Assert.assertEquals(two, ONodeId.parseString("-1234567895623"));
	}

	public void testSubtractTwoMinusOne() {
    ONodeId one = ONodeId.valueOf(2);
    ONodeId two = one.subtract(ONodeId.ONE);

    String result = two.toHexString();
    Assert.assertEquals(result, "000000000000000000000000000000000000000000000001");
  }

  public void testSubtractOverflowValue() {
    ONodeId one = ONodeId.valueOf(0xF0000000L);
    ONodeId two = one.subtract(ONodeId.ONE);

    String result = two.toHexString();
    Assert.assertEquals(result, "0000000000000000000000000000000000000000efffffff");
  }

  public void testSubtractOverflowValueTwo() {
    ONodeId one = ONodeId.valueOf(0xF0000000L);
    ONodeId two = ONodeId.ONE.subtract(one);

    String result = two.toHexString();
    Assert.assertEquals(result, "-0000000000000000000000000000000000000000efffffff");
  }

  public void testSubtractToNegativeResult() {
    ONodeId one = ONodeId.ZERO;
    ONodeId two = one.subtract(ONodeId.ONE);

    String result = two.toHexString();
    Assert.assertEquals(result, "-000000000000000000000000000000000000000000000001");
  }

  public void testSubtractZero() {
    ONodeId one = ONodeId.parseString("451234567894123456987465");
    ONodeId two = one.subtract(ONodeId.ZERO);

    Assert.assertEquals(two, ONodeId.parseString("451234567894123456987465"));
  }

  public void testSubtractFromZeroPositive() {
    ONodeId one = ONodeId.parseString("451234567894123456987465");
    ONodeId two = ONodeId.ZERO.subtract(one);

    Assert.assertEquals(two, ONodeId.parseString("-451234567894123456987465"));
  }

  public void testSubtractFromZeroNegative() {
    ONodeId one = ONodeId.parseString("-451234567894123456987465");
    ONodeId two = ONodeId.ZERO.subtract(one);

    Assert.assertEquals(two, ONodeId.parseString("451234567894123456987465"));
  }

  public void testSubtractZeroFromZero() {
    ONodeId two = ONodeId.ZERO.subtract(ONodeId.ZERO);

    Assert.assertEquals(two, ONodeId.ZERO);
  }

  public void testSubtractFromNegativePositive() {
    ONodeId one = ONodeId.parseString("-99999999999999");
    ONodeId two = ONodeId.parseString("10");

    ONodeId result = one.subtract(two);

    Assert.assertEquals(result, ONodeId.parseString("-100000000000009"));
  }

  public void testSubtractFromPositiveNegative() {
    ONodeId one = ONodeId.parseString("99999999999999");
    ONodeId two = ONodeId.parseString("-10");

    ONodeId result = one.subtract(two);

    Assert.assertEquals(result, ONodeId.parseString("100000000000009"));
  }

  public void testSubtractSamePositiveNumbers() {
    ONodeId one = ONodeId.parseString("1245796317821536854785");
    ONodeId two = ONodeId.parseString("1245796317821536854785");

    ONodeId result = one.subtract(two);

    Assert.assertEquals(result, ONodeId.ZERO);
  }

  public void testSubtractSameNegativeNumbers() {
    ONodeId one = ONodeId.parseString("-1245796317821536854785");
    ONodeId two = ONodeId.parseString("-1245796317821536854785");

    ONodeId result = one.subtract(two);

    Assert.assertEquals(result, ONodeId.ZERO);
  }

	public void testSubtractFromMaxNegativeOnePositive() {
		ONodeId result = ONodeId.MIN_VALUE.subtract(ONodeId.ONE);

		Assert.assertEquals(result, ONodeId.ZERO);
	}

	public void testSubtractFromMaxPositiveOneNegative() {
		ONodeId result = ONodeId.MAX_VALUE.subtract(ONodeId.parseString("-1"));

		Assert.assertEquals(result, ONodeId.ZERO);
	}

	public void testMultiplyTwoAndFive() {
    ONodeId one = ONodeId.valueOf(2);
    ONodeId two = one.multiply(5);

    String result = two.toHexString();
    Assert.assertEquals(result, "00000000000000000000000000000000000000000000000a");
  }

  public void testMultiplyOnZero() {
    ONodeId one = ONodeId.valueOf(2);
    ONodeId two = one.multiply(0);

    Assert.assertEquals(two, ONodeId.ZERO);
  }

  public void testMultiplyOverflowNumbers() {
    ONodeId one = ONodeId.valueOf(0xFFFFFFFFFL);
    ONodeId two = one.multiply(26);

    String result = two.toHexString();
    Assert.assertEquals(result, "000000000000000000000000000000000000019fffffffe6");
  }

  public void testLeftShift2Bits() {
    ONodeId nodeOne = ONodeId.valueOf(0xFFFFFFFFDL);
    ONodeId two = nodeOne.shiftLeft(2);

    String result = two.toHexString();

    Assert.assertEquals(result, "000000000000000000000000000000000000003ffffffff4");
  }

  public void testLeftShift32Bits() {
    ONodeId nodeOne = ONodeId.valueOf(0xFFFFFFFFDL);
    ONodeId two = nodeOne.shiftLeft(32);

    String result = two.toHexString();

    Assert.assertEquals(result, "0000000000000000000000000000000ffffffffd00000000");
  }

  public void testLeftShift34Bits() {
    ONodeId nodeOne = ONodeId.valueOf(0xFFFFFFFFDL);
    ONodeId two = nodeOne.shiftLeft(34);

    String result = two.toHexString();

    Assert.assertEquals(result, "0000000000000000000000000000003ffffffff400000000");
  }

  public void testLeftShiftTillZero() {
    ONodeId nodeOne = ONodeId.valueOf(0xFFFFFFFFDL);
    ONodeId two = nodeOne.shiftLeft(192);

    Assert.assertEquals(two, ONodeId.ZERO);
  }

  public void testLeftShiftTillZeroTwo() {
    ONodeId nodeOne = ONodeId.parseHexSting("0000000000000000000000000000003ffffffff400000000");
    ONodeId two = nodeOne.shiftLeft(160);

    Assert.assertEquals(two, ONodeId.ZERO);
  }

  public void testRightShift2Bits() {
    ONodeId nodeOne = ONodeId.valueOf(0xAAAFFFFFFFFDL);
    ONodeId two = nodeOne.shiftRight(2);

    String result = two.toHexString();

    Assert.assertEquals(result, "0000000000000000000000000000000000002aabffffffff");
  }

  public void testRightShift32Bits() {
    ONodeId nodeOne = ONodeId.valueOf(0xAAAFFFFFFFFDL);
    ONodeId two = nodeOne.shiftRight(32);

    String result = two.toHexString();

    Assert.assertEquals(result, "00000000000000000000000000000000000000000000aaaf");
  }

  public void testRightShift34Bits() {
    ONodeId nodeOne = ONodeId.valueOf(0xAAAFFFFFFFFDL);
    ONodeId two = nodeOne.shiftRight(34);

    String result = two.toHexString();

    Assert.assertEquals(result, "000000000000000000000000000000000000000000002aab");
  }

  public void testRightShiftTillZero() {
    ONodeId nodeOne = ONodeId.valueOf(0xFFFFFFFFDL);
    ONodeId two = nodeOne.shiftRight(192);

    Assert.assertEquals(two, ONodeId.ZERO);
  }

  public void testRightShiftTillZeroTwo() {
    ONodeId nodeOne = ONodeId.parseHexSting("0000000000000000000000000000003ffffffff400000000");
    ONodeId two = nodeOne.shiftRight(72);

    Assert.assertEquals(two, ONodeId.ZERO);
  }

  public void testIntValue() {
    final ONodeId nodeId = ONodeId.valueOf(0xAAAFFFFFFFFDL);

    Assert.assertEquals(0xFFFFFFFD, nodeId.intValue());
  }

  public void testToValueOfFromString() {
    final ONodeId nodeId = ONodeId.parseHexSting("00123456789abcdef0000000000123000000000000002aab");
    Assert.assertEquals(nodeId.toHexString(), "00123456789abcdef0000000000123000000000000002aab");

  }

  public void testToStream() {
    final ONodeId nodeId = ONodeId.parseHexSting("00123456789abcdef0000000000123000000000000002aab");

    byte[] expectedResult = new byte[25];
    expectedResult[0] = 0;
    expectedResult[1] = 0x12;
    expectedResult[2] = 0x34;
    expectedResult[3] = 0x56;
    expectedResult[4] = 0x78;
    expectedResult[5] = (byte) 0x9A;
    expectedResult[6] = (byte) 0xBC;
    expectedResult[7] = (byte) 0xDE;
    expectedResult[8] = (byte) 0xF0;
    expectedResult[9] = (byte) 0x00;
    expectedResult[10] = (byte) 0x00;
    expectedResult[11] = (byte) 0x00;
    expectedResult[12] = (byte) 0x00;
    expectedResult[13] = (byte) 0x01;
    expectedResult[14] = (byte) 0x23;
    expectedResult[15] = (byte) 0x00;
    expectedResult[16] = (byte) 0x00;
    expectedResult[17] = (byte) 0x00;
    expectedResult[18] = (byte) 0x00;
    expectedResult[19] = (byte) 0x00;
    expectedResult[20] = (byte) 0x00;
    expectedResult[21] = (byte) 0x00;
    expectedResult[22] = (byte) 0x2a;
    expectedResult[23] = (byte) 0xab;
    expectedResult[24] = (byte) 1;

    Assert.assertEquals(nodeId.toStream(), expectedResult);
  }

	public void testChunksToByteArray() {
		final ONodeId nodeId = ONodeId.parseHexSting("00123456789abcdef0000000000123000000000000002aab");

		byte[] expectedResult = new byte[24];
		expectedResult[0] = 0;
		expectedResult[1] = 0x12;
		expectedResult[2] = 0x34;
		expectedResult[3] = 0x56;
		expectedResult[4] = 0x78;
		expectedResult[5] = (byte) 0x9A;
		expectedResult[6] = (byte) 0xBC;
		expectedResult[7] = (byte) 0xDE;
		expectedResult[8] = (byte) 0xF0;
		expectedResult[9] = (byte) 0x00;
		expectedResult[10] = (byte) 0x00;
		expectedResult[11] = (byte) 0x00;
		expectedResult[12] = (byte) 0x00;
		expectedResult[13] = (byte) 0x01;
		expectedResult[14] = (byte) 0x23;
		expectedResult[15] = (byte) 0x00;
		expectedResult[16] = (byte) 0x00;
		expectedResult[17] = (byte) 0x00;
		expectedResult[18] = (byte) 0x00;
		expectedResult[19] = (byte) 0x00;
		expectedResult[20] = (byte) 0x00;
		expectedResult[21] = (byte) 0x00;
		expectedResult[22] = (byte) 0x2a;
		expectedResult[23] = (byte) 0xab;

		Assert.assertEquals(nodeId.chunksToByteArray(), expectedResult);
	}

	public void testLongValuePositive() {
		final ONodeId nodeId = ONodeId.parseHexSting("00123456789abcdef000000000012300ecffaabb12342aab");

		Assert.assertEquals(nodeId.longValue(), 0xecffaabb12342aabL);
	}

	public void testLongValueNegative() {
		final ONodeId nodeId = ONodeId.parseHexSting("-00123456789abcdef000000000012300ecffaabb12342aab");

		Assert.assertEquals(nodeId.longValue(), 0xecffaabb12342aabL);
	}

	public void testIntValuePositive() {
		final ONodeId nodeId = ONodeId.parseHexSting("00123456789abcdef000000000012300ecffaabb12342aab");

		Assert.assertEquals(nodeId.intValue(), 0x12342aab);
	}

	public void testIntValueNegative() {
		final ONodeId nodeId = ONodeId.parseHexSting("-00123456789abcdef000000000012300ecffaabb12342aab");

		Assert.assertEquals(nodeId.intValue(), 0x12342aab);
	}


	public void testFromStreamPositive() {
    final ONodeId nodeId = ONodeId.parseString("1343412555467812");
    final byte[] content = nodeId.toStream();

    final ONodeId deserializedNodeId = ONodeId.fromStream(content, 0);
    Assert.assertEquals(nodeId, deserializedNodeId);
  }

  public void testFromStreamNegative() {
    final ONodeId nodeId = ONodeId.parseString("-1343412555467812");
    final byte[] content = nodeId.toStream();

    final ONodeId deserializedNodeId = ONodeId.fromStream(content, 0);
    Assert.assertEquals(nodeId, deserializedNodeId);
  }

  public void testFromStreamZero() {
    final ONodeId nodeId = ONodeId.parseString("0");
    final byte[] content = nodeId.toStream();

    final ONodeId deserializedNodeId = ONodeId.fromStream(content, 0);
    Assert.assertEquals(nodeId, deserializedNodeId);
  }

  public void testFromStreamPositiveWithOffset() {
    final ONodeId nodeId = ONodeId.parseString("1343412555467812");
    final byte[] content = nodeId.toStream();
    final byte[] contentWithOffset = new byte[content.length + 10];
    System.arraycopy(content, 0, contentWithOffset, 5, content.length);

    final ONodeId deserializedNodeId = ONodeId.fromStream(contentWithOffset, 5);
    Assert.assertEquals(nodeId, deserializedNodeId);
  }

  public void testFromStreamNegativeWithOffset() {
    final ONodeId nodeId = ONodeId.parseString("-1343412555467812");
    final byte[] content = nodeId.toStream();
    final byte[] contentWithOffset = new byte[content.length + 10];
    System.arraycopy(content, 0, contentWithOffset, 5, content.length);

    final ONodeId deserializedNodeId = ONodeId.fromStream(contentWithOffset, 5);
    Assert.assertEquals(nodeId, deserializedNodeId);
  }

  public void testFromStreamZeroWithOffset() {
    final ONodeId nodeId = ONodeId.parseString("0");
    final byte[] content = nodeId.toStream();
    final byte[] contentWithOffset = new byte[content.length + 10];
    System.arraycopy(content, 0, contentWithOffset, 5, content.length);

    final ONodeId deserializedNodeId = ONodeId.fromStream(contentWithOffset, 5);
    Assert.assertEquals(nodeId, deserializedNodeId);
  }

  public void testCompareToRIDNodeIdCompatibility() {
    final TreeSet<ONodeId> nodeIds = new TreeSet<ONodeId>();
    final TreeSet<ORecordId> recordIds = new TreeSet<ORecordId>();

    for (int i = 0; i < 10000; i++) {
      final ONodeId nodeId = ONodeId.generateUniqueId();
      nodeIds.add(nodeId);

      recordIds.add(new ORecordId(1, new OClusterPositionNodeId(nodeId)));
    }

    final Iterator<ORecordId> recordIdIterator = recordIds.iterator();

    for (final ONodeId nodeId : nodeIds) {
      final ORecordId recordId = recordIdIterator.next();

      Assert.assertEquals(recordId, new ORecordId(1, new OClusterPositionNodeId(nodeId)));
    }

    Assert.assertFalse(recordIdIterator.hasNext());
  }

  public void testNodeIdSerializaion() throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);

    final List<ONodeId> serializedNodes = new ArrayList<ONodeId>();

    for (int i = 0; i < 10000; i++) {
      final ONodeId nodeId = ONodeId.generateUniqueId();
      objectOutputStream.writeObject(nodeId);

      serializedNodes.add(nodeId);
    }

    objectOutputStream.close();

    byte[] serializedContent = out.toByteArray();
    final ByteArrayInputStream in = new ByteArrayInputStream(serializedContent);
    final ObjectInputStream objectInputStream = new ObjectInputStream(in);

    for (ONodeId nodeId : serializedNodes) {
      final ONodeId deserializedNodeId = (ONodeId) objectInputStream.readObject();
      Assert.assertEquals(deserializedNodeId, nodeId);
    }

    Assert.assertEquals(objectInputStream.available(), 0);
  }
}
