package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.util.Collection;

import static org.testng.Assert.*;

@Test
public class ODefaultCacheTest {

	public void enabledAfterStartup() {
		// Given cache created
		// And not started
		// And not enabled
		OCache sut = newCache();

		// When started
		sut.startup();

		// Then it should be enabled
		assertTrue(sut.isEnabled());
	}

	public void disabledAfterShutdown() {
		// Given running cache
		OCache sut = runningCache();

		// When started
		sut.shutdown();

		// Then it should be disabled
		assertFalse(sut.isEnabled());
	}

	public void disablesOnlyIfWasEnabled() {
		// Given enabled cache
		OCache sut = enabledCache();

		// When disabled more than once
		boolean disableConfirmed = sut.disable();
		boolean disableNotConfirmed = sut.disable();

		// Then should return confirmation of switching from enabled to disabled state for first time
		// And no confirmation on subsequent disables
		assertTrue(disableConfirmed);
		assertFalse(disableNotConfirmed);
	}

	public void enablesOnlyIfWasDisabled() {
		// Given disabled cache
		OCache sut = newCache();

		// When enabled more than once
		boolean enableConfirmed = sut.enable();
		boolean enableNotConfirmed = sut.enable();

		// Then should return confirmation of switching from disabled to enabled state for first time
		// And no confirmation on subsequent enables
		assertTrue(enableConfirmed);
		assertFalse(enableNotConfirmed);
	}

	public void doesNothingWhileDisabled() {
		// Given cache created
		// And not started
		// And not enabled
		OCache sut = new ODefaultCache(1);

		// When any operation called on it
		ODocument record = new ODocument();
		ORID recordId = record.getIdentity();
		sut.put(record);
		ORecordInternal<?> recordGot = sut.get(recordId);
		int cacheSizeAfterPut = sut.size();
		ORecordInternal<?> recordRemoved = sut.remove(recordId);
		int cacheSizeAfterRemove = sut.size();

		// Then it has no effect on cache's state
		assertEquals(sut.isEnabled(), false, "Cache should be disabled at creation");
		assertEquals(recordGot, null, "Cache should return empty records while disabled");
		assertEquals(recordRemoved, null, "Cache should return empty records while disabled");
		assertEquals(cacheSizeAfterPut, 0, "Cache should ignore insert while disabled");
		assertEquals(cacheSizeAfterRemove, cacheSizeAfterPut, "Cache should ignore remove while disabled");
	}

	public void hasZeroSizeAfterClear() {
		// Given enabled non-empty cache
		OCache sut = enabledNonEmptyCache();

		// When cleared
		sut.clear();

		// Then size of cache should be zero
		assertEquals(sut.size(), 0, "Cache was not cleaned up");
	}

	public void providesAccessToAllKeysInCache() {
		// Given enabled non-empty cache
		OCache sut = enabledNonEmptyCache();

		// When asked for keys
		Collection<ORID> keys = sut.keys();

		// Then keys count should be same as size of cache
		// And records available for keys
		assertEquals(keys.size(), sut.size(), "Cache provided not all keys?");
		for (ORID key : keys) {
			assertNotNull(sut.get(key));
		}
	}

	public void storesRecordsUsingTheirIdentity() {
		// Given an enabled cache
		OCache sut = enabledCache();

		// When new record put into
		ORecordId id = new ORecordId(1, 1);
		ODocument record = new ODocument(id);
		sut.put(record);

		// Then it can be retrieved later by it's id
		assertEquals(sut.get(id), record);
	}

	public void storesRecordsOnlyOnceForEveryIdentity() {
		// Given an enabled cache
		OCache sut = enabledCache();
		final int initialSize = sut.size();

		// When some records with same identity put in several times
		ODocument first = new ODocument(new ORecordId(1, 1));
		ODocument last = new ODocument(new ORecordId(1, 1));
		sut.put(first);
		sut.put(last);

		// Then cache ends up storing only one item
		assertEquals(sut.size(), initialSize + 1);
	}

	public void removesOnlyOnce() {
		// Given an enabled cache with records in it
		OCache sut = enabledCache();
		ORecordId id = new ORecordId(1, 1);
		ODocument record = new ODocument(id);
		sut.put(record);
		sut.remove(id);

		// When removing already removed record
		ORecordInternal<?> removedSecond = sut.remove(id);

		// Then empty result returned
		assertNull(removedSecond);
	}

	public void storesNoMoreElementsThanSpecifiedLimit() {
		// Given an enabled cache
		OCache sut = enabledCache();

		// When stored more distinct elements than cache limit allows
		for (int i = sut.limit() + 2; i > 0; i--)
			sut.put(new ODocument(new ORecordId(i, i)));

		// Then size of cache should be exactly as it's limit
		assertEquals(sut.size(), sut.limit(), "Cache doesn't meet limit requirements");
	}

	private ODefaultCache newCache() {
		return new ODefaultCache(5);
	}

	private OCache enabledCache() {
		ODefaultCache cache = newCache();
		cache.enable();
		return cache;
	}

	private OCache enabledNonEmptyCache() {
		OCache cache = enabledCache();
		cache.put(new ODocument(new ORecordId(1, 1)));
		cache.put(new ODocument(new ORecordId(2, 2)));
		return cache;
	}

	private OCache runningCache() {
		ODefaultCache cache = newCache();
		cache.startup();
		return cache;
	}
}
