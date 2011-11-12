package com.orientechnologies.common.collection;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class OMVRBTreeCompositeTest {
	protected OMVRBTree<OCompositeKey, Double>	tree;

	@BeforeMethod
	public void beforeMethod() throws Exception {
		tree = new OMVRBTreeMemory<OCompositeKey, Double>(4, 0.5f);
		for (double i = 1; i < 4; i++) {
			for (double j = 1; j < 10; j++) {
				final OCompositeKey compositeKey = new OCompositeKey();
				compositeKey.addKey(i);
				compositeKey.addKey(j);
				tree.put(compositeKey, i * 4 + j);
			}
		}
	}

	@Test
	public void testGetEntrySameKeys() {
		OMVRBTreeEntry<OCompositeKey, Double> result = tree.getEntry(compositeKey(1.0, 2.0), OMVRBTree.PartialSearchMode.NONE);
		assertEquals(result.getKey(), compositeKey(1.0, 2.0));

		result = tree.getEntry(compositeKey(1.0, 2.0), OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
		assertEquals(result.getKey(), compositeKey(1.0, 2.0));

		result = tree.getEntry(compositeKey(1.0, 2.0), OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
		assertEquals(result.getKey(), compositeKey(1.0, 2.0));
	}

	@Test
	public void testGetEntryPartialKeys() {
		OMVRBTreeEntry<OCompositeKey, Double> result = tree.getEntry(compositeKey(2.0), OMVRBTree.PartialSearchMode.NONE);
		assertEquals(result.getKey().getKeys().get(0), 2.0);

		result = tree.getEntry(compositeKey(2.0), OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
		assertEquals(result.getKey(), compositeKey(2.0, 1.0));

		result = tree.getEntry(compositeKey(2.0), OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
		assertEquals(result.getKey(), compositeKey(2.0, 9.0));
	}

	@Test
	public void testSubMapInclusiveDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), true, compositeKey(3.0), true)
				.descendingMap();

		assertEquals(navigableMap.size(), 18);

		for (double i = 2; i <= 3; i++) {
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
		}
	}

	@Test
	public void testSubMapFromInclusiveDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), true, compositeKey(3.0), false)
				.descendingMap();

		assertEquals(navigableMap.size(), 9);

		for (double j = 1; j <= 9; j++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, j)));
		}
	}

	@Test
	public void testSubMapToInclusiveDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), false, compositeKey(3.0), true)
				.descendingMap();

		assertEquals(navigableMap.size(), 9);

		for (double i = 1; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(3.0, i)));
		}
	}

	@Test
	public void testSubMapNonInclusiveDescending() {
		ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), false, compositeKey(3.0), false)
				.descendingMap();

		assertEquals(navigableMap.size(), 0);
		assertTrue(navigableMap.isEmpty());

		navigableMap = tree.subMap(compositeKey(1.0), false, compositeKey(3.0), false);

		assertEquals(navigableMap.size(), 9);

		for (double i = 1; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, i)));
		}
	}

	@Test
	public void testSubMapInclusive() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), true, compositeKey(3.0), true);

		assertEquals(navigableMap.size(), 18);

		for (double i = 2; i <= 3; i++) {
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
		}
	}

	@Test
	public void testSubMapFromInclusive() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), true, compositeKey(3.0), false);

		assertEquals(navigableMap.size(), 9);

		for (double j = 1; j <= 9; j++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, j)));
		}
	}

	@Test
	public void testSubMapToInclusive() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), false, compositeKey(3.0), true);

		assertEquals(navigableMap.size(), 9);

		for (double i = 1; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(3.0, i)));
		}
	}

	@Test
	public void testSubMapNonInclusive() {
		ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0), false, compositeKey(3.0), false);

		assertEquals(navigableMap.size(), 0);
		assertTrue(navigableMap.isEmpty());

		navigableMap = tree.subMap(compositeKey(1.0), false, compositeKey(3.0), false);

		assertEquals(navigableMap.size(), 9);

		for (double i = 1; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, i)));
		}
	}

	@Test
	public void testSubMapInclusivePartialKey() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), true, compositeKey(3.0), true);

		assertEquals(navigableMap.size(), 15);

		for (double i = 2; i <= 3; i++) {
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j < 4)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
		}
	}

	@Test
	public void testSubMapFromInclusivePartialKey() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), true, compositeKey(3.0), false);

		assertEquals(navigableMap.size(), 6);

		for (double j = 4; j <= 9; j++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, j)));
		}
	}

	@Test
	public void testSubMapToInclusivePartialKey() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), false, compositeKey(3.0), true);

		assertEquals(navigableMap.size(), 14);

		for (double i = 2; i <= 3; i++) {
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j <= 4)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
		}
	}

	@Test
	public void testSubMapNonInclusivePartial() {
		ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), false, compositeKey(3.0), false);

		assertEquals(navigableMap.size(), 5);

		for (double i = 5; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, i)));
		}
	}

	@Test
	public void testSubMapInclusivePartialKeyDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), true, compositeKey(3.0), true)
				.descendingMap();

		assertEquals(navigableMap.size(), 15);

		for (double i = 2; i <= 3; i++) {
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j < 4)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
		}
	}

	@Test
	public void testSubMapFromInclusivePartialKeyDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), true, compositeKey(3.0), false)
				.descendingMap();

		assertEquals(navigableMap.size(), 6);

		for (double j = 4; j <= 9; j++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, j)));
		}
	}

	@Test
	public void testSubMapToInclusivePartialKeyDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), false, compositeKey(3.0), true)
				.descendingMap();

		assertEquals(navigableMap.size(), 14);

		for (double i = 2; i <= 3; i++) {
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j <= 4)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
		}
	}

	@Test
	public void testSubMapNonInclusivePartialDescending() {
		ONavigableMap<OCompositeKey, Double> navigableMap = tree.subMap(compositeKey(2.0, 4.0), false, compositeKey(3.0), false)
				.descendingMap();

		assertEquals(navigableMap.size(), 5);

		for (double i = 5; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(2.0, i)));
		}
	}

	@Test
	public void testTailMapInclusivePartial() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0), true);
		assertEquals(navigableMap.size(), 18);

		for (double i = 2; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testTailMapNonInclusivePartial() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0), false);
		assertEquals(navigableMap.size(), 9);

		for (double i = 1; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(3.0, i)));
		}
	}

	@Test
	public void testTailMapInclusivePartialDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0), true).descendingMap();
		assertEquals(navigableMap.size(), 18);

		for (double i = 2; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testTailMapNonInclusivePartialDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0), false).descendingMap();
		assertEquals(navigableMap.size(), 9);

		for (double i = 1; i <= 9; i++) {
			assertTrue(navigableMap.containsKey(compositeKey(3.0, i)));
		}
	}

	@Test
	public void testTailMapInclusive() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0, 3.0), true);
		assertEquals(navigableMap.size(), 16);

		for (double i = 2; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j < 3)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testTailMapNonInclusive() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0, 3.0), false);
		assertEquals(navigableMap.size(), 15);

		for (double i = 2; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j <= 3)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testTailMapInclusiveDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0, 3.0), true).descendingMap();
		assertEquals(navigableMap.size(), 16);

		for (double i = 2; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j < 3)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testTailMapNonInclusiveDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.tailMap(compositeKey(2.0, 3.0), false).descendingMap();
		assertEquals(navigableMap.size(), 15);

		for (double i = 2; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 2 && j <= 3)
					continue;
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapInclusivePartial() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0), true);
		assertEquals(navigableMap.size(), 27);

		for (double i = 1; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapNonInclusivePartial() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0), false);
		assertEquals(navigableMap.size(), 18);

		for (double i = 1; i < 3; i++)
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapInclusivePartialDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0), true).descendingMap();
		assertEquals(navigableMap.size(), 27);

		for (double i = 1; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapNonInclusivePartialDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0), false).descendingMap();
		assertEquals(navigableMap.size(), 18);

		for (double i = 1; i < 3; i++)
			for (double j = 1; j <= 9; j++) {
				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapInclusive() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0, 2.0), true);
		assertEquals(navigableMap.size(), 20);

		for (double i = 1; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 3 && j > 2)
					continue;

				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapNonInclusive() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0, 2.0), false);
		assertEquals(navigableMap.size(), 19);

		for (double i = 1; i < 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 3 && j >= 2)
					continue;

				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapInclusiveDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0, 2.0), true).descendingMap();
		assertEquals(navigableMap.size(), 20);

		for (double i = 1; i <= 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 3 && j > 2)
					continue;

				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testHeadMapNonInclusiveDescending() {
		final ONavigableMap<OCompositeKey, Double> navigableMap = tree.headMap(compositeKey(3.0, 2.0), false).descendingMap();
		assertEquals(navigableMap.size(), 19);

		for (double i = 1; i < 3; i++)
			for (double j = 1; j <= 9; j++) {
				if (i == 3 && j >= 2)
					continue;

				assertTrue(navigableMap.containsKey(compositeKey(i, j)));
			}
	}

	@Test
	public void testGetCeilingEntryKeyExistPartial() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getCeilingEntry(compositeKey(3.0), OMVRBTree.PartialSearchMode.NONE);
		assertEquals(entry.getKey().getKeys().get(0), 3.0);

		entry = tree.getCeilingEntry(compositeKey(3.0), OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(3.0, 9.0));

		entry = tree.getCeilingEntry(compositeKey(3.0), OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(3.0, 1.0));
	}

	@Test
	public void testGetCeilingEntryKeyNotExistPartial() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getCeilingEntry(compositeKey(1.3), OMVRBTree.PartialSearchMode.NONE);
		assertEquals(entry.getKey().getKeys().get(0), 2.0);

		entry = tree.getCeilingEntry(compositeKey(1.3), OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(2.0, 9.0));

		entry = tree.getCeilingEntry(compositeKey(1.3), OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(2.0, 1.0));
	}

	@Test
	public void testGetFloorEntryKeyExistPartial() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getFloorEntry(compositeKey(2.0), OMVRBTree.PartialSearchMode.NONE);
		assertEquals(entry.getKey().getKeys().get(0), 2.0);

		entry = tree.getFloorEntry(compositeKey(2.0), OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(2.0, 9.0));

		entry = tree.getFloorEntry(compositeKey(2.0), OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(2.0, 1.0));
	}

	@Test
	public void testGetFloorEntryKeyNotExistPartial() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getFloorEntry(compositeKey(1.3), OMVRBTree.PartialSearchMode.NONE);
		assertEquals(entry.getKey().getKeys().get(0), 1.0);

		entry = tree.getFloorEntry(compositeKey(1.3), OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(1.0, 9.0));

		entry = tree.getFloorEntry(compositeKey(1.3), OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
		assertEquals(entry.getKey(), compositeKey(1.0, 1.0));
	}

	@Test
	public void testHigherEntryKeyExistPartial() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getHigherEntry(compositeKey(2.0));
		assertEquals(entry.getKey(), compositeKey(3.0, 1.0));
	}

	@Test
	public void testHigherEntryKeyNotExist() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getHigherEntry(compositeKey(1.3));
		assertEquals(entry.getKey(), compositeKey(2.0, 1.0));
	}

	@Test
	public void testHigherEntryNullResult() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getHigherEntry(compositeKey(12.0));
		assertNull(entry);
	}

	@Test
	public void testLowerEntryNullResult() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getLowerEntry(compositeKey(0.0));
		assertNull(entry);
	}

	@Test
	public void testLowerEntryKeyExist() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getLowerEntry(compositeKey(2.0));
		assertEquals(entry.getKey(), compositeKey(1.0, 9.0));
	}

	@Test
	public void testLowerEntryKeyNotExist() {
		OMVRBTreeEntry<OCompositeKey, Double> entry = tree.getLowerEntry(compositeKey(2.5));
		assertEquals(entry.getKey(), compositeKey(2.0, 9.0));
	}

	private OCompositeKey compositeKey(Comparable<?>... params) {
		return new OCompositeKey((List<Comparable<?>>) Arrays.asList(params));
	}
}
