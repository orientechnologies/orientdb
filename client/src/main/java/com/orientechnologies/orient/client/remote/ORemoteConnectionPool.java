package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/** Created by tglman on 01/10/15. */
public class ORemoteConnectionPool
    implements OResourcePoolListener<String, OChannelBinaryAsynchClient> {

  private OResourcePool<String, OChannelBinaryAsynchClient> pool;

  public ORemoteConnectionPool(int iMaxResources) {
    pool = new OResourcePool<>(iMaxResources, this);
  }

  protected OChannelBinaryAsynchClient createNetworkConnection(
      String serverURL, final OContextConfiguration clientConfiguration) throws OIOException {
    if (serverURL == null) throw new IllegalArgumentException("server url is null");

    // TRY WITH CURRENT URL IF ANY
    try {
      OLogManager.instance().debug(this, "Trying to connect to the remote host %s...", serverURL);

      int sepPos = serverURL.indexOf(":");
      final String remoteHost = serverURL.substring(0, sepPos);
      final int remotePort = Integer.parseInt(serverURL.substring(sepPos + 1));

      final OChannelBinaryAsynchClient ch =
          new OChannelBinaryAsynchClient(
              remoteHost,
              remotePort,
              clientConfiguration,
              OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

      return ch;

    } catch (OIOException e) {
      // RE-THROW IT
      throw e;
    } catch (Exception e) {
      OLogManager.instance().debug(this, "Error on connecting to %s", e, serverURL);
      throw OException.wrapException(new OIOException("Error on connecting to " + serverURL), e);
    }
  }

  @Override
  public OChannelBinaryAsynchClient createNewResource(
      final String iKey, final Object... iAdditionalArgs) {
    return createNetworkConnection(iKey, (OContextConfiguration) iAdditionalArgs[0]);
  }

  @Override
  public boolean reuseResource(
      final String iKey, final Object[] iAdditionalArgs, final OChannelBinaryAsynchClient iValue) {
    final boolean canReuse = iValue.isConnected();
    if (!canReuse)
      // CANNOT REUSE: CLOSE IT PROPERLY
      try {
        iValue.close();
      } catch (Exception e) {
        OLogManager.instance().debug(this, "Error on closing socket connection", e);
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
