#!/usr/bin/env bash

STUDIO=studio-2.2.zip
rm studio-2.2.zip
npm install
npm run build
cp plugin.json dist/
cd dist
zip -r ../$STUDIO plugin.json www
