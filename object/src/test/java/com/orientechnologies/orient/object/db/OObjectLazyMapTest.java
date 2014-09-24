package com.orientechnologies.orient.object.db;

import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author JN <a href="mailto:jn@brain-activit.com">Julian Neuhaus</a>
 * @since 21.08.2014
 */
public class OObjectLazyMapTest
{
	private OObjectDatabaseTx databaseTx;
	
	@BeforeClass
	protected void setUp() throws Exception 
	{
		databaseTx = new OObjectDatabaseTx("memory:OObjectLazyMapTest");
		databaseTx.create();

		databaseTx.getEntityManager().registerEntityClass(EntityWithMap.class);
	}

	@AfterClass
	protected void tearDown() 
	{
		databaseTx.drop();
	}
	
	@Test
	public void getOrDefaultTest()
	{
		EntityWithMap toStore = new EntityWithMap();
		toStore.setId(0);
		
		EntityWithMap mapElement1 = new EntityWithMap();
		mapElement1.setId(1);
		
		EntityWithMap mapElement2 = new EntityWithMap();
		mapElement2.setId(2);
		
		Map<String,EntityWithMap> mapToStore = new HashMap<String, OObjectLazyMapTest.EntityWithMap>();
		mapToStore.put(String.valueOf(mapElement1.getId()),mapElement1);
		mapToStore.put(String.valueOf(mapElement2.getId()),mapElement2);
		
		toStore.setMap(mapToStore);
		
		EntityWithMap fromDb = this.databaseTx.save(toStore);
		
		assertTrue(fromDb != null);
		assertTrue(fromDb.getMap() != null);
		
		Map<String,EntityWithMap> testMap = fromDb.getMap();
		
		assertTrue(testMap != null);
		assertTrue(testMap.getClass() == OObjectLazyMap.class);		
		assertTrue(testMap.getOrDefault(String.valueOf(mapElement1.getId()),null) != null);
		assertTrue(testMap.getOrDefault(String.valueOf(mapElement2.getId()),null) != null);
		assertTrue(testMap.getOrDefault("3",null) == null);
		assertTrue(testMap.getOrDefault("3",mapElement1) == mapElement1);
	}
	
	public class EntityWithMap
	{
		private int id;
		private Map<String,EntityWithMap> map;

		public Map<String,EntityWithMap> getMap()
		{
			return map;
		}

		public void setMap(Map<String,EntityWithMap> map)
		{
			this.map = map;
		}

		public int getId()
		{
			return id;
		}

		public void setId(int id)
		{
			this.id = id;
		}		
	}
}
