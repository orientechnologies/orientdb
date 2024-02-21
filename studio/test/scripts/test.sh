#!/bin/sh

npm run test
echo $? > target/test-status.txt
