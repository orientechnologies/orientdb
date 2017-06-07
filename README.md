
Develop: [![Build Status](http://helios.orientdb.com/job/orientdb-studio-multibranch/job/develop/badge/icon)](http://helios.orientdb.com/job/orientdb-studio-multibranch/job/develop/) 2.2.x: [![Build Status](http://helios.orientdb.com/job/orientdb-studio-multibranch/job/2.2.x/badge/icon)](http://helios.orientdb.com/job/orientdb-studio-multibranch/job/2.2.x/)

# [OrientDB Studio](https://github.com/orientechnologies/orientdb-studio)

## Developers

### Quick Start

0. Install [Node.js](http://nodejs.org/) and NPM 

1. Clone the repository:

    ```bash
    $ git clone https://github.com/orientechnologies/orientdb-studio.git
    ```

2. Install [yarn](https://yarnpkg.com) 

3. Install local dependencies:

    ```bash
    $ cd orientdb-studio
    $ yarn install
    ```

4. Start OrientDB server.

5. Start webpack server and open your browser at `http://localhost:8080`:

    ```bash
	$ npm run watch
    ```
    
### Distribution

To create the Studio package just run

```
$ mvn clean install
```

and the package will be available in `target` directory

```
target/orientdb-studio.*.zip
```



