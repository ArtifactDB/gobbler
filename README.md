# Gobbling data on shared filesystems

[![Test and build](https://github.com/ArtifactDB/gobbler/actions/workflows/build.yaml/badge.svg)](https://github.com/ArtifactDB/gobbler/actions/workflows/build.yaml)
[![Publish version](https://github.com/ArtifactDB/gobbler/actions/workflows/publish.yaml/badge.svg)](https://github.com/ArtifactDB/gobbler/actions/workflows/publish.yaml)
[![Latest version](https://img.shields.io/github/v/tag/ArtifactDB/gobbler?label=Version)](https://github.com/ArtifactDB/gobbler/releases)

## Overview 

The Gobbler implements [**gypsum**](https://github.com/ArtifactDB/gypsum-worker)-like storage of ArtifactDB-managed files on a shared filesystem.
This replaces cloud storage with a world-readable local directory, reducing costs and improving efficiency by avoiding network traffic for uploads/downloads.
We simplify authentication by using Unix file permissions to determine ownership, avoiding the need for a separate identity provider like GitHub.

This document is intended for system administrators who want to spin up their own instance or developers of new clients to the Gobbler service.
Users should never have to interact with the Gobbler directly, as this should be mediated by client packages in relevant frameworks like R or Python.
For example, the [**gobbler** R package](https://github.com/ArtifactDB/gobbler-R) provides an R interface to a local Gobbler service.

## Concepts

The Gobbler aims to mirror the concepts used by [**gypsum**](https://github.com/ArtifactDB/gypsum).
In particular, the file organization and structure of the `..`-prefixed metadata files should be the same between the Gobbler and **gypsum**.
This provides a degree of cloud-readiness, as administrators can easily switch from the Gobbler to **gypsum** by just uploading the contents of the local directory to a Cloudflare R2 bucket.

### File organization

The Gobbler stores all files in a local directory called the "registry", organized in a hierarchy of project, asset and versions.
That is, each project may have multiple assets, and each asset may have multiple versions.
All user-supplied files are associated with a particular project-asset-version combination.
For consistency with **gypsum**'s terminology, we will define an "upload" as a filesystem copy of user-supplied files into the registry. 

Within the directory, files associated with a project-asset-version combination will be stored in the `{project}/{asset}/{version}/` subdirectory.
For each project-asset-version combination, the set of all user-supplied files is recorded in the `{project}/{asset}/{version}/..manifest` file.
This contains a JSON object where each key/value pair describes a user-supplied file.
The key is a relative path to the file within the `{project}/{asset}/{version}/` subdirectory.
The value is another object with the following properties:
- `size`: an integer specifying the size of the file in bytes.
- `md5sum`: a string containing the hex-encoded MD5 checksum of the file.
- `link` (optional): an object specifying the link destination for a file (see [below](#link-deduplication) for details).
  This contains the strings `project`, `asset`, `version` and `path`, and possibly an `ancestor` object.

**gypsum** keeps track of the latest version of each asset in the `{project}/{asset}/..latest` file.
This contains a JSON object with the following properties:
- `latest`: String containing the name of the latest version of this asset.
  This is defined as the version with the most recent `upload_finish` time in the `..summary`.

For any given project-asset-version combination, the `{project}/{asset}/{version}/..summary` file records some more details about the upload process.
This contains a JSON object with the following properties:
- `upload_user_id`, a string containing the identity of the uploading user.
- `upload_start`, an Internet date/time-formatted string containing the upload start time.
- `upload_finish`, an Internet date/time-formatted string containing the upload finish time.
  This property is absent if the upload for this version is currently in progress, but will be added on upload completion. 
- `on_probation` (optional), a boolean indicating whether this upload is on probation, see [below](#upload-probation).
  If not present, this can be assumed to be `false`.

### Link deduplication

When creating a new version of a project's assets, the Gobbler will attempt deduplication based on the file size and MD5 checksum.
Specifically, it will inspect the immediate previous version of the asset to see if any other files have a matching size/checksum.
If so, it will create a symbolic link to the file in the previous version rather than wasting disk space with a redundant copy.
Users can also directly instruct the Gobbler to create links by supplying symlinks to existing files in the registry.

Any "linked-from" files (i.e., those identified as copies of other existing files) will be present as symbolic links in the registry.
The existence of linked-from files can also be determined from the `..manifest` file for each project-asset-version;
or from `..links` files, which describe the links present in each nested subdirectory.
The latter allows clients to avoid reading the entire manifest if only a subset of files are of interest.
To illustrate, consider a hypothetical `..links` file at the following path:

```
{project}/{asset}/{version}/x/y/z/..links
```

This contains a JSON object where each key/value pair describes a linked-from path with the same subdirectory.
The key is a relative path, to be appended to `{project}/{asset}/{version}/x/y/z/` to obtain the full path of the linked-from file.
The value is another object that contains the strings `project`, `asset`, `version` and `path`, which collectively specify the link destination supplied by the user.
If the user-supplied destination is itself another link, the object will contain a nested `ancestor` object that specifies the final link destination to the actual file.

If no `..links` file is present in a particular subdirectory, it can be assumed that there are no linked-from files in the same subdirectory. 
This guarantee does not apply recursively, i.e., linked-from files may still be present in nested subdirectories.

### Permissions

The Gobbler supports three levels of permissions - adminstrators, project owners and uploaders.

- Uploaders can upload new assets or versions to an existing project.
  Upload authorization is provided by the project's owners, and can be limited to particular asset/version names, or within a certain time frame.
  Project owners can also specify whether an uploader is untrusted (and thus whether their uploads should be probational, see below).
- Project owners can modify the permissions of their project, including the addition/removal of new owners or changes to uploader authorizations.
  They can also do anything that uploaders can do.
- Adminstrators can create projects and projects (or particular assets/versions thereof).
  They can also do anything that project owners can do.

The permissions for a project are stored in the `{project}/..permissions` file.
This is a JSON-formatted file that contains a JSON object with the following properties:
- `owners`: An array of strings containing the GitHub user names or organizations that own this project.
- `uploaders`: An array of objects specifying GitHub users or organizations that are authorized to be uploaders.
  Each object has the following properties:
  - `id`: String containing the identity of the user/organization.
  - `asset` (optional): String containing the name of the asset that the uploader is allowed to upload to.
    If not specified, no restrictions are placed on the asset name.
  - `version` (optional): String containing the name of the version that the uploader is allowed to upload to.
    This can be used with or without `asset`, in which case it applies to all new and existing assets. 
    If not specified, no restrictions are placed on the version name.
  - `until` (optional): An Internet date/time-formatted string specifying the lifetime of the authorization.
    After this time, any upload attempt is rejected.
    If not specified, the authorization does not expire by default.
  - `trusted` (optional): Boolean indicating whether the uploader is trusted.
    If `false`, all uploads are considered to be probational.
    If not specified, the uploader is untrusted by default.

User identities are defined by the UIDs on the operating system.
All users are authenticated by examining the ownership of files provided to the Gobbler.
Note that, when switching from the Gobbler to **gypsum**, the project permissions need to be updated from UIDs to GitHub user names.

### Upload probation

Uploads can be specified as "probational" if they come from untrusted sources.
The uploaded files are present in the registry and accessible to readers;
however, they are not immutable and are not used to set the latest version of an asset in `..latest`.
This is useful when considering third-party contributions to a project, where project owners can review the upload before approving/rejecting it.
Approved probational uploads are immutable and have the same status as a trusted upload from the project owner themselves, while rejected probational uploads are deleted entirely from the registry.
Probational uploads can also be rejected by the uploading user themselves, e.g., to fix known problems before a project owner's review.

Uploads from untrusted uploaders are always probational.
For trusted uploaders or project owners, users can specify whether their upload is probational.
This is useful for testing before committing to the long-term immutability of the uploaded files. 

### Storage quotas

**gypsum**-like storage quotas are not yet implemented.

Each project's current usage is tracked in `{project}/..usage`, which contains a JSON object with the following properties:
- `total`: the total number of bytes allocated to user-supplied files (i.e., not including `..`-prefixed internal files).

## Making requests 

### General instructions 

The Gobbler requires a "staging directory", a world-writeable directory on the shared filesystem.
Users submit requests to the Gobbler by writing a JSON file with the request parameters inside the staging directory.
Each request file's name should have a prefix of `request-<ACTION>-` where `ACTION` specifies the action to be performed.
Once this file is written, users should perform a POST request to the Gobbler API to trigger execution;
this will return a JSON response that has at least the `status` property (either `SUCCESS` or `FAILED`).
For failures, this will be an additional `reason` string property to specify the reason;
for successes, additional properties may be present depending on the request action.

### Creating projects (admin)

Administrators are responsible for creating new projects within the registry.
This is done using the write-and-rename paradigm to create a file with the `request-create_project-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the new project.
  This should not contain `/`, `\`, or `.`.
- `permissions` (optional): an object containing either or both of `owners` and `uploaders`.
  Each of these properties has the same type as described [above](#permissions).
  If `owners` is not supplied, it is automatically set to a length-1 array containing only the uploading user.
  This property is ignored when uploading a new version of an asset to an existing project.

On success, a new project is created with the designated permissions and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

### Uploads and updates

To upload a new version of an asset of a project, users should create a temporary directory within the staging directory.
The directory may have any name but should avoid starting with `request-`.
Files within this temporary directory will be transferred to the appropriate subdirectory within the registry, subject to the following rules:

- Hidden files (i.e., prefixed with `.`) are ignored.
- Symbolic links to directories are not allowed.
- Symbolic links to files only allowed if the symlink target is an existing file within a project-asset-version subdirectory of the registry.

Once this directory is constructed and populated, the user should use the write-and-rename paradigm to create a file with the `request-upload-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of an existing project.
- `asset`: string containing the name of the asset.
  This should not contain `/`, `\`, or `.`.
- `version`: string containing the name of the version.
  This should not contain `/`, `\`, or `.`.
- `source`: string containing the name of the temporary directory, itself containing the files to be uploaded for this version of the asset.
  This temporary directory is expected to be inside the staging directory.
- `on_probation` (optional): boolean specifying whether this version of the asset should be considered as probational.

On success, the files will be transferred to the registry and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

### Setting permissions

Users should use the write-and-rename paradigm to create a file with the `request-set_permissions-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
- `permissions`: an object containing either or both of `owners` and `uploaders`.
  Each of these properties has the same type as described [above](#permissions).
  If any property is missing, the value in the existing permissions is used.

On success, the permissions in the registry are modified and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

### Handling probation

To approve probation, a user should use the write-and-rename paradigm to create a file with the `request-approve_probation-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
- `asset`: string containing the name of the asset.
- `version`: string containing the name of the version.

On success, the probational status is removed and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

To reject probation, a user should use the write-and-rename paradigm to create a file with the `request-reject_probation-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
- `asset`: string containing the name of the asset.
- `version`: string containing the name of the version.

On success, the relevant version is removed from the registry and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

### Refreshing statistics (admin)

On rare occasions involving frequent updates, some of the inter-version statistics may not be correct.
For example, the latest version in `..latest` may not keep in sync when many probational versions are approved at once.
Administrators can fix this manually by requesting a refresh of the relevant statistics.

To refresh project usage, use the write-and-rename paradigm to create a file with the `request-refresh_usage-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.

On success, the usage is updated and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

To refresh the latest version of an asset, use the write-and-rename paradigm to create a file with the `request-refresh_latest-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
- `asset`: string containing the name of the asset.

On success, the latest version is updated and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

### Deleting content (admin)

Administrators have the ability to delete files from the registry.
This violates **gypsum**'s immutability contract and should be done sparingly.
In particular, administrators must ensure that no other project links to the to-be-deleted files, otherwise those links will be invalidated.
This check involves going through all the manifest files and is currently a manual process.

To delete a project, use the write-and-rename paradigm to create a file with the `request-delete_project-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.

On success, the project is deleted and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.
A success is still reported even if the project is not present, in which case the operation is a no-op.

To delete an asset, use the write-and-rename paradigm to create a file with the `request-delete_asset-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
- `asset`: string containing the name of the asset.

On success, the asset is deleted and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.
A success is still reported even if the asset or its project is not present, in which case the operation is a no-op.

To delete a version, use the write-and-rename paradigm to create a file with the `request-delete_version-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
- `asset`: string containing the name of the asset.
- `version`: string containing the name of the version.

On success, the version is deleted and a JSON formatted file will be created in `responses` with the `type` property set to `SUCCESS`.
A success is still reported even if the version, its asset or its project is not present, in which case the operation is a no-op.

### Health check

To check if a Gobbler service is active, a user should touch a file with the `request-health_check-` prefix.
The contents of this file are ignored.
On success, the asset is deleted and a JSON formatted file will be created in `responses` with the `status` property set to `SUCCESS`.

## Accessing the registry

Most applications on the shared filesystem should be able to directly access the world-readable registry via the usual system calls.
This is the most efficient access pattern as it avoids any data transfer.

Remote applications can obtain a listing of the registry by performing a GET request to the `/list` endpoint,
This accepts some optional query parameters:

- `path`, a string specifying a relative path to a subdirectory within the registry.
  The listing is performed within this subdirectory.
  If not provided, the entire registry is listed.
- `recursive`, a boolean indicating whether to list recursively.
  Defaults to false.

The response is a JSON-encoded array of the relative paths within the registry or one of its requested subdirectories.
If `recursive=true`, all paths refer to files; otherwise, paths may refer to subdirectories, which are denoted by a `/` suffix.

Any file of interest within the registry can then be obtained via the `/fetch/{path}` endpoint.
Once downloaded, clients should consider caching the files to reduce future data transfer.

## Parsing logs

For some actions, the Gobbler creates a log within the `..logs/` subdirectory of the registry.
The file is named after the date/time of the action's completion, followed by an underscore, followed by a random 6-digit integer for disambiguation purposes.
The file contains a JSON object that details the type of action in the `type` property:

- `add-version` indicates that a new version was added, or a probational version was approved.
  This has the `project`, `asset`, `version` string properties to describe the version.
  It also has the `latest` boolean property to indicate whether the added version is the latest one for its asset.
- `delete-version` indicates that a version was deleted.
  This has the `project`, `asset`, `version` string properties to describe the version.
  It also has the `latest` boolean property to indicate whether the deleted version was the latest one for its asset.
- `delete-asset` indicates that an asset was deleted.
  This has the `project` and `asset` string property.
- `delete-project` indicates that a project was deleted.
  This has the `project` string property.

Downstream systems can inspect these files to determine what changes have occurred in the registry.
This is intended for systems that need to maintain a database index on top of the bucket's contents.
By routinely scanning for changes, databases can incrementally perform updates rather than reindexing the entire bucket.

Log files are held for 7 days before deletion.

## Deployment instructions

First, clone this repository and build the binary. 
This requires the [Go toolchain](https://go.dev/dl) (version 1.16 or higher).

```sh
git clone https://github.com/ArtifactDB/gobbler
cd gobbler && go build
```

Then, set up a staging directory with global read/write permissions.
All parent directories of the staging directory should be at least globally executable.

```sh
mkdir STAGING
chmod 777 STAGING
```

Next, set up a registry directory with global read-only permissions.
Note that the registry and staging directories do not need to be on the same filesystem (e.g., for mounted shares), as long as both are accessible to users. 

```sh
mkdir REGISTRY
chmod 755 REGISTRY
```

Finally, start the Gobbler by running the binary with a few arguments, including the UIDs of administrators:

```sh
./gobbler \
    -staging STAGING \
    -registry REGISTRY \
    -admin ADMIN1,ADMIN2 \
    -port PORT
```

Multiple Gobbler instances can target the same `REGISTRY` with different `STAGING`.
This is useful for complex configurations where the same filesystem is mounted in multiple compute environments,
whereby a separate Gobbler instance can be set up in each environment to enable uploads. 

Clients need to know `STAGING`, `REGISTRY` and the URL of the REST API.
The location of the staging directory and the URL will be used to make requests as described [above](#general-instructions).
The contents of the registry can be directly read from the filesystem.
