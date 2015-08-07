# Contribute to OrientDB

In order to contribute issues and pull requests, please sign OrientDB's [Contributor License Agreement](https://www.clahub.com/agreements/orientechnologies/orientdb). The purpose of this agreement is to protect users of this codebase by ensuring that all code is free to use under the stipulations of the [Apache2 license](http://www.apache.org/licenses/LICENSE-2.0.html).

## Pushing into main repository
If you'd like to contribute to OrientDB with a patch follow the following steps:
* fork the repository interested in your change. The main one is https://github.com/orientechnologies/orientdb, but plugins, drivers and other components reside in other projects under [Orient Technologies](https://github.com/orientechnologies/) umbrella.
* select the "develop" branch i present
* apply your changes,
* test that Test Suite hasn't been broken by running:
 * `mvn clean test`
* if all the tests pass, then do a **Pull Request** (PR) against **"develop"** branch on GitHub repository and write a comment about the change. Please don't send PR to "master" because we use that branch only for releasing
* if you want the fix is backported to a previous version, please write it in your comments and if the OrientDB team agree they will do that as soon as the PR is merged

## Documentation

If you want to contribute to the OrientDB documentation, the right repository is: https://github.com/orientechnologies/orientdb-docs. Every 24-48 hours all the contributions are reviewed and published on the public [documentation](http://orientdb.com/docs/last/).

## Code formatting
You can find eclipse java formatter config file here: [_base/ide/eclipse-formatter.xml](https://github.com/orientechnologies/orientdb/blob/master/_base/ide/eclipse-formatter.xml).

If you use IntelliJ IDEA you can install [this](http://plugins.jetbrains.com/plugin/?id=6546) plugin and use formatter profile mentioned above.

