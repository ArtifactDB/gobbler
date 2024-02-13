# Gobbling data on shared filesystems

## About

The Gobbler is a service that gobbles up user-supplied files on shared filesystems like those used by high-performance computing clusters.
No HTTP requests are required, as all communication between the user and Gobbler is performed through filesystem events and/or polling.
No authentication is required, either, as we can pick up the ID of the user directly from the file permissions.
This eliminates the need for any network communication, reduces maintainence/DevOps requirements, and improves efficiency by directly copying user files to the global registry.

## How it works

Let's say that we want to hold a "registry" of user-supplied content on a shared filesystem.
This registry can be read by anyone 

to have a "staging" directory with global write permissions.
Any users on the shared filesystem can write files directly into the staging directory.
The Gobbler watches for changes in this directory (via the [`fsnotify`](httsp://github.com/fsnotify/fsnotify) package) and cop

## Setting up

First, clone this repository and build the binary. 
This requires the [Go toolchain](https://go.dev/dl) (version 1.16 or higher).

```sh
git clone https://github.com/ArtifactDB/gobbler
cd gobbler && go build
```

Then, set up a staging directory with global read/write permissions.

- The staging directory should be on a filesystem supported by the [`fsnotify`](httsp://github.com/fsnotify/fsnotify) package.
- All parent directories of the staging directory should be at least globally executable.

```sh
mkdir STAGING
chmod 777 STAGING
```

Finally, set up a registry with global read-only permissions.

- The registry should be on a filesystem that supports file locking.
- The registry and staging directories do not need to be on the same filesystem (e.g., for mounted shares), as long as both are accessible to users. 

```sh
mkdir REGISTRY
chmod 755 REGISTRY
```

The Gobbler can then be started by running the binary with a few arguments:

```sh
./gobbler -staging STAGING -registry REGISTRY -admin ADMIN1,ADMIN2
```

## Making requests 

### Concepts 

Users (and administrators) can submit requests to the Gobbler by simply writing a JSON file with the request parameters inside the staging directory.
Each request file's name should have a prefix of `request-<ACTION>-` where `ACTION` specifies the action to be performed.
Upon creation of a request file, the Gobbler will parse it and execute the request with the specified parameters.

After completing the request, the Gobbler will write a JSON response to the `responses` subdirectory of the staging directory.
This has the same name as the initial request file, so users can easily poll for the existence of this file.
Each response will have at least the `type` property (either `SUCCESS` or `FAILED`).
For failures, this will be an additional `reason` string property to specify the reason.

When writing the request file, it is recommended to use the write-and-rename paradigm.
Specifically, users should write the JSON request body to a file inside the staging directory that does _not_ have the `request-<ACTION>-` prefix.
Once the write is complete, this file can be renamed to a file with said prefix.
This ensures that the Gobbler does not read a partially-written file.



