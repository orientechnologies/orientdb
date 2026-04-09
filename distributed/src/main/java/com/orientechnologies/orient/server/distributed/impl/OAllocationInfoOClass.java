package com.orientechnologies.orient.server.distributed.impl;

import java.util.List;

public record OAllocationInfoOClass(String name, List<OAllocationInfoOClassNode> nodes) {}
