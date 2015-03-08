#!/bin/sh
#
# Copyright (c) Orient Technologies LTD (http://www.orientechnologies.com)
#
exec ${0/%dserver.sh/server.sh} -Ddistributed=true $*
