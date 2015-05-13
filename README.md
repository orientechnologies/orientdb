# [OrientDB Studio](https://github.com/orientechnologies/orientdb-studio)
 
## Users
#### OrientDB v1.6 or major

Copy the **studio.zip** file inside the directory "plugins" in OrientDB Server.

#### OrientDB v1.4 - v1.5

Unpack the **studio.zip** file inside the directory "www/studio" in OrientDB Server. If you want also to keep the previous version of studio, rename the existent "www/studio" directory in some other like "www/studioprev".

## Developers

### Quick Start

0. Install [Node.js](http://nodejs.org/) and NPM 

1. Clone the repository:

    ```bash
    $ git clone https://github.com/orientechnologies/orientdb-studio.git
    ```

2. Install global dependencies `yo`, `bower` and `compass` (remove "sudo" if your account already has the permissions to install software):

    ```bash
    $ sudo npm install -g yo bower compass
    ```

3. Install local dependencies:

    ```bash
    $ npm install
    $ bower install
    ```

4. Start OrientDB server.


5. Start the server grunt and your browser will be opened at `http://localhost:9000`:

    ```bash
	$ grunt server
    ```
    
