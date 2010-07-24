#!/bin/sh
#
# Copyright (c) 1999-2010 Luca Garulli
#
ab -n100000 -A admin:admin -k -c10 http://127.0.0.1:2480/document/demo/15:0
