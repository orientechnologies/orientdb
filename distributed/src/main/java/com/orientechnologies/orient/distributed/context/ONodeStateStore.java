package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import java.util.Optional;

public record ONodeStateStore(
    Optional<OTransactionSequenceStatus> sequence,
    Optional<ONetworkTopologyStore> network,
    Optional<ODatabasesTopologyStore> databases) {}
