#!/usr/bin/env bash

STUDIO=studio-2.1.zip

bower install
grunt build
cd dist/studio
zip -r ../../bin/$STUDIO plugin.json www
