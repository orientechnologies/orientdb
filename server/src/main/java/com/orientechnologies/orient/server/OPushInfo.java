package com.orientechnologies.orient.server;

import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.lang.ref.WeakReference;

public record OPushInfo(
    OClientConnection connection, WeakReference<ONetworkProtocolBinary> protocol) {}
