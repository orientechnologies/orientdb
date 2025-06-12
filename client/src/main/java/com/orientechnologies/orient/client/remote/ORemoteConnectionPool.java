package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/** Created by tglman on 01/10/15. */
public class ORemoteConnectionPool
    implements OResourcePoolListener<String, OChannelBinaryAsynchClient> {
  private static final OLogger logger = OLogManager.instance().logger(ORemoteConnectionPool.class);

  private final OResourcePool<String, OChannelBinaryAsynchClient> pool;
  private final String host;
  private final int port;
  private final OSocketFactory socketFactory;
  private final OContextConfiguration conf;

  public ORemoteConnectionPool(
      int iMaxResources, String host, int port, OContextConfiguration conf) {
    this.pool = new OResourcePool<>(iMaxResources, this);
    this.host = host;
    this.port = port;
    this.conf = conf;
    this.socketFactory = new OSocketFactory(conf);
  }

  protected OChannelBinaryAsynchClient createNetworkConnection() throws OIOException {
    // TRY WITH CURRENT URL IF ANY
    try {
      logger.debug("Trying to connect to the remote host %s:%d...", this.host, this.port);

      final OChannelBinaryAsynchClient ch =
          new OChannelBinaryAsynchClient(
              host,
              port,
              this.conf,
              socketFactory,
              OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

      return ch;

    } catch (OIOException e) {
      // RE-THROW IT
      throw e;
    } catch (Exception e) {
      logger.debug("Error on connecting to  %s:%d", e, this.host, this.port);
      throw OException.wrapException(
          new OIOException("Error on connecting to " + this.host + ":" + this.port), e);
    }
  }

  @Override
  public OChannelBinaryAsynchClient createNewResource(final String iKey) {
    return createNetworkConnection();
  }

  @Override
  public boolean reuseResource(final String iKey, final OChannelBinaryAsynchClient iValue) {
    final boolean canReuse = iValue.isConnected();
    if (!canReuse)
      // CANNOT REUSE: CLOSE IT PROPERLY
      try {
        iValue.close();
      } catch (Exception e) {
        logger.debug("Error on closing socket connection", e);
      }
    iValue.markInUse();
    return canReuse;
  }

  public OResourcePool<String, OChannelBinaryAsynchClient> getPool() {
    return pool;
  }

  public OChannelBinaryAsynchClient acquire(
      final String iServerURL,
      final long timeout,
      final OContextConfiguration clientConfiguration) {
    return pool.getResource(iServerURL, timeout, clientConfiguration);
  }

  public void checkIdle(long timeout) {
    for (OChannelBinaryAsynchClient resource : pool.getResources()) {
      if (!resource.isInUse() && resource.getLastUse() + timeout < System.currentTimeMillis()) {
        resource.close();
      }
    }
  }
}
