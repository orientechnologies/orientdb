package com.orientechnologies.orient.core.db;

import java.util.Collection;
import java.util.Collections;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 06/11/14
 */
public class OPartitionedDatabasePoolFactory {

  private volatile int                                                          maxPoolSize = 64;
  private boolean                                                               closed      = false;

  private final ConcurrentLinkedHashMap<PoolIdentity, OPartitionedDatabasePool> poolStore;

  public OPartitionedDatabasePoolFactory() {
    this(100);
  }

  public OPartitionedDatabasePoolFactory(int capacity) {
    poolStore = new ConcurrentLinkedHashMap.Builder<PoolIdentity, OPartitionedDatabasePool>().maximumWeightedCapacity(capacity)
        .build();

  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) {
		checkForClose();

		this.maxPoolSize = maxPoolSize;
  }


	public OPartitionedDatabasePool get(String url, String userName, String userPassword) {
		checkForClose();

    final PoolIdentity poolIdentity = new PoolIdentity(url, userName, userPassword);

    OPartitionedDatabasePool pool = poolStore.get(poolIdentity);
    if (pool != null)
      return pool;

    pool = new OPartitionedDatabasePool(url, userName, userPassword, maxPoolSize);

    final OPartitionedDatabasePool oldPool = poolStore.putIfAbsent(poolIdentity, pool);
    if (oldPool != null)
      return oldPool;

    return pool;
  }

  public Collection<OPartitionedDatabasePool> getPools() {
		checkForClose();

    return Collections.unmodifiableCollection(poolStore.values());
  }

  public void close() {
		if (closed)
			return;

		closed = true;

		for (OPartitionedDatabasePool pool : poolStore.values())
		  pool.close();

    poolStore.clear();
  }

	private void checkForClose() {
		if (closed)
			throw  new IllegalStateException("Pool factory is closed");
	}


	private static final class PoolIdentity {
    private final String url;
    private final String userName;
    private final String userPassword;

    private PoolIdentity(String url, String userName, String userPassword) {
      this.url = url;
      this.userName = userName;
      this.userPassword = userPassword;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PoolIdentity that = (PoolIdentity) o;

      if (!url.equals(that.url))
        return false;
      if (!userName.equals(that.userName))
        return false;
      if (!userPassword.equals(that.userPassword))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = url.hashCode();
      result = 31 * result + userName.hashCode();
      result = 31 * result + userPassword.hashCode();
      return result;
    }
  }
}
