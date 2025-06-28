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

The Gobbler implements a REST API that mirrors the concepts used by [**gypsum**](https://github.com/ArtifactDB/gypsum) but on a shared filesystem.
In particular, the file organization and structure of the `..`-prefixed metadata files should be the same between the Gobbler and **gypsum**.
This provides a degree of cloud-readiness, as administrators can easily switch from the Gobbler to **gypsum** by just uploading the contents of the local directory to a Cloudflare R2 bucket.

### File organization

The Gobbler stores all files in a local directory called the "registry", organized in a hierarchy of project, asset and versions.
That is, each project may have multiple assets, and each asset may have multiple versions.
All user-supplied files are associated with a particular project-asset-version combination.
For consistency with **gypsum**'s terminology, we will define an "upload" as a filesystem copy of user-supplied files into the registry. 

Within the registry, files associated with a project-asset-version combination will be stored in the `{project}/{asset}/{version}/` subdirectory (i.e., the "version directory").
Files can be organized in any number of possibly-nested subdirectories within the version directory.
Empty subdirectories are allowed. 
Symbolic links can be formed to [other files in the registry](#link-deduplication) or to certain [whitelisted directories](#administration).
Symbolic links to other directories are not allowed. 
Files starting with `..` are reserved for Gobbler's internal files.

For each project-asset-version combination, the set of all user-supplied files is recorded in the `{project}/{asset}/{version}/..manifest` file.
This contains a JSON object where each key/value pair describes a user-supplied file.
The key is a relative path to the file within the version directory.
The value is another object with the following properties:

- `size`: an integer specifying the size of the file in bytes.
- `md5sum`: a string containing the hex-encoded MD5 checksum of the file.
- `link` (optional): an object specifying the link destination for a file (see [below](#link-deduplication) for details).
  This contains the strings `project`, `asset`, `version` and `path`, and possibly an `ancestor` object.

An empty subdirectory within the version directory is recorded in the `..manifest` file as an entry with an empty `md5sum` string. 
In addition, the `size` is set to zero and no `link` field is present. 
Non-empty subdirectories are not reported as their existence is implied by other files in the manifest. 
(Note that an "empty" subdirectory may still contain `..`-prefixed files, as only the user-supplied contents of a subdirectory are considered.)

The Gobbler keeps track of the latest version of each asset in the `{project}/{asset}/..latest` file.
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
Users can also directly instruct the Gobbler to create links by supplying symlinks during upload,
either to existing files in the registry or to other files in the same to-be-uploaded version of the asset.

Any "linked-from" files (i.e., those identified as copies of other existing files) will be present as symbolic links in the registry.
The existence of linked-from files can also be determined from the `..manifest` file for each project-asset-version;
or from `..links` files, which describe the links present in each nested subdirectory.
The latter allows clients to avoid reading the entire manifest if only a subset of files are of interest.
To illustrate, consider a hypothetical `..links` file at the following path:

```
{project}/{asset}/{version}/x/y/z/..links
```

This contains a JSON object where each key/value pair describes a linked-from path with the same subdirectory.
The key is a file name to be appended to `{project}/{asset}/{version}/x/y/z/` to obtain the full path of the linked-from file.
The value is another object that contains the strings `project`, `asset`, `version` and `path`, which collectively specify the link destination supplied by the user.
If the user-supplied destination is itself another link, the object will contain a nested `ancestor` object that specifies the final link destination to the actual file.

If no `..links` file is present in a particular subdirectory, it can be assumed that there are no linked-from files in the same subdirectory. 
This guarantee does not apply recursively, i.e., linked-from files may still be present in nested subdirectories.

If a symbolic link in the registry would refer to another symbolic link, the Gobbler will automatically create a symbolic link to the actual "ancestral" file during upload or reindexing.
This avoids potential problems with operating system limits on the depth of symbolic link chains.
An exception is made for links to files in whitelisted directories (see the [`-whitelist`](#administration) option), which are treated as the files themselves.

### Permissions

The Gobbler supports several levels of permissions:

- Uploaders can upload new assets or versions to an existing project.
  Upload authorization is provided by the project/asset owners and can be limited to a asset, version or time frame.
  Project/asset owners can also specify whether an uploader is untrusted (and thus whether their uploads should be probational, see below).
- Asset owners can modify the permissions of their asset within a project, including the addition/removal of new asset owners or changes to uploader authorizations for their asset.
  They can also upload new versions to their asset.
- Project owners can modify the permissions of their project, including the addition/removal of new project/asset owners or changes to any uploader authorizations.
  They can also upload new versions of new or existing assets to their project.
- Adminstrators can create new projects; change permissions of any project or asset; delete projects, assets and versions; and upload new versions of new or existing assets in any project.

The permissions for a project are stored in the `{project}/..permissions` file.
This is a JSON-formatted file that contains a JSON object with the following properties:

- `owners`: An array of strings containing the identities of users who own this project.
- `uploaders`: An array of objects specifying the users who are authorized to be uploaders.
  Each object has the following properties:
  - `id`: String containing the identity of the uploading user.
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
- `global_write` (optional): a boolean indicating whether "global writes" are enabled.
  With global writes enabled, any user of the filesystem can create a new asset within this project.
  Once the asset is created, its creating user is added as a trusted uploader to the `{project}/{asset}/..permissions` file (see below).
  If not specified, global writes are disabled by default.

Additional permissions for a specific asset may be specified in an optional `{project}/{asset}/..permissions` file. 
This should be a JSON-formatted file that contains a JSON object with the following properties:

- `owners`: An array of strings containing the identities of users who own this asset.
- `uploaders`: An array of objects specifying the users who are authorized to be uploaders for this asset.
  Each object has the same properties as described in the project-level permissions, except that any `asset` is ignored as it will be replaced by the name of the asset.
  During [upload requests](#uploads-and-updates), these `uploaders` will be appended to the `uploaders` in `{project}/..permissions` before authorization checks.

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

🚧🚧🚧 **gypsum**-like storage quotas are not yet implemented. 🚧🚧🚧

Each project's current usage is tracked in `{project}/..usage`, which contains a JSON object with the following properties:
- `total`: the total number of bytes allocated to user-supplied files (i.e., not including `..`-prefixed internal files).

## Reading from the registry

The Gobbler expects to operate on a shared filesystem, so any applications on the same filesystem should be able to directly access the world-readable registry via the usual system calls.
This is the most efficient access method as it avoids any data transfer.

That said, some support is provided for remote applications.
We can obtain a listing of the registry by performing a GET request to the `/list` endpoint of the Gobbler API.
This accepts some optional query parameters:

- `path`, a string specifying a relative path to a subdirectory within the registry.
  The listing is performed within this subdirectory.
  If not provided, the entire registry is listed.
- `recursive`, a boolean indicating whether to list recursively.
  Defaults to false.

The response is a JSON-encoded array of the relative paths within the registry or one of its requested subdirectories.
If `recursive=true`, all paths refer to files; otherwise, paths may refer to subdirectories, which are denoted by a `/` suffix.

Any file of interest within the registry can then be obtained via a GET request to the `/fetch/{path}` endpoint,
where `path` is the relative path to the file inside the registry.
Once downloaded, clients should consider caching the files to reduce future data transfer.

For a Gobbler instance, the location of its registry can be obtained via a GET request to the `/info` endpoint.
This can be used to avoid hard-coded paths in the clients. 

## Modifying the registry 

### General instructions 

The Gobbler requires a "staging directory", a world-writeable directory on the shared filesystem.
Users submit requests to the Gobbler by writing a JSON file with the request parameters inside the staging directory.
Each request file's name should have a prefix of `request-<ACTION>-` where `ACTION` specifies the action to be performed.

Once this file is written, users should perform a POST request to the `/new/{request}` endpoint, where `request` is the name of the request file inside the staging directory.
The HTTP response will contain a JSON object that has at least the `status` property, set to either `SUCCESS` or `ERROR`.
For failures, there will be an additional `reason` string property to specify the reason.
For successes, additional properties may be present depending on the request action.

For a Gobbler instance, the location of its staging directory can be obtained via a GET request to the `/info` endpoint.
This can be used to avoid hard-coded paths in the clients. 

### Creating projects (admin)

Administrators are responsible for creating new projects within the registry.
This is done by creating a file with the `request-create_project-` prefix, which should be JSON-formatted with the following properties:

- `project`: string containing the name of the new project.
  This should not contain `/` or `\`, or start with `..`.
- `permissions` (optional): an object containing either or both of `owners` and `uploaders`.
  Each of these properties has the same type as described [above](#permissions).
  If not supplied, `owners` is set as described above and `uploaders` is empty.
  If only `owners` is not supplied, it is automatically set to a length-1 array containing only the uploading user.
  This property is ignored when uploading a new version of an asset to an existing project.

On success, a new project is created with the designated permissions.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.

### Uploads and updates

To upload a new version of an asset of a project, users should create a temporary directory within the staging directory.
The temporary directory may have any name but should avoid starting with `request-`.
Files within this temporary directory may be organized in any number of (possibly nested, possibly empty) subdirectories, subject to the following rules:

- Files prefixed with `..` are reserved for Gobbler's internal files and are ignored.
- Symbolic links to directories are not allowed.
- Symbolic links to files are allowed if:
  - The symlink target is an existing file within a project-asset-version subdirectory of the registry.
  - The symlink target is a file in the same temporary directory.
  - The symlink target is in a [whitelisted directory](#administration).

Once this directory is constructed and populated, the user should create a file with the `request-upload-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of an existing project.
  This should not contain `/` or `\`, or start with `..`.
- `asset`: string containing the name of the asset.
  This should not contain `/` or `\`, or start with `..`.
- `version`: string containing the name of the version.
  This should not contain `/` or `\`, or start with `..`.
- `source`: string containing the name of the temporary directory to be uploaded to the specified version of the asset.
  This temporary directory should be inside the staging directory.
- `on_probation` (optional): boolean specifying whether this version of the asset should be considered as probational.
  If not provided, this defaults to false.
- `ignore_dot` (optional): boolean specifying whether to ignore hidden files (i.e., dotfiles) within the `source` directory.
  If not provided, this defaults to false.
- `consume` (optional): boolean specifying whether the Gobbler is allowed to attempt to move files from `source` into the registry.
  If successful, this consumes the files in the temporary directory, avoiding an extra copy but invalidating the contents of `source`.
  If not provided, this defaults to false.
- `spoof` (optional): string specifying the name of a user, on whose behalf this request is performed.
  Only supported if [spoofing permissions](#administration) are provided and the current user is allowed to make a request on behalf of the spoofed user.

On success, the files will be transferred to the appropriate version directory within the registry
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.

Users should consider setting the permissions of this temporary directory (and any of its subdirectories) to `777`.
This ensures that the Gobbler instance is able to free up space by periodically deleting old files.

### Setting permissions

Users should create a file with the `request-set_permissions-` prefix, which should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.
- `asset` (optional): string containing the name of an asset.
  This should not contain `/` or `\`, or start with `..`.
  If provided, asset-level uploader permissions will be modified instead of project-level permissions.
- `permissions`: an object containing zero, one or more of `owners`, `uploaders` and `global_write`.
  Each of these properties has the same type as described [above](#permissions).
  If any property is missing, the value in the existing permissions is used.
  If `asset` is provided, only `uploaders` will be used.
- `spoof` (optional): string specifying the name of a user, on whose behalf this request is performed.
  Only supported if [spoofing permissions](#administration) are provided and the current user is allowed to make a request on behalf of the spoofed user.

On success, the permissions in the registry are modified.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.

### Handling probation

To approve probation, a user should create a file with the `request-approve_probation-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.
- `asset`: string containing the name of the asset.
  This should not contain `/` or `\`, or start with `..`.
- `version`: string containing the name of the version.
  This should not contain `/` or `\`, or start with `..`.

On success, the probational status is removed.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.

To reject probation, a user should create a file with the `request-reject_probation-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.
- `asset`: string containing the name of the asset.
  This should not contain `/` or `\`, or start with `..`.
- `version`: string containing the name of the version.
  This should not contain `/` or `\`, or start with `..`.
- `force` (optional): boolean indicating whether a probational version should be forcibly deleted.
  Occasionally necessary if the version contains corrupted summary or manifest files,
  in which case they will be deleted but the project usage will need to be refreshed manually.
  Defaults to false if not supplied.
- `spoof` (optional): string specifying the name of a user, on whose behalf this request is performed.
  Only supported if [spoofing permissions](#administration) are provided and the current user is allowed to make a request on behalf of the spoofed user.

On success, the relevant version is removed from the registry.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.

### Refreshing statistics (admin)

On rare occasions involving frequent updates, some of the inter-version statistics may not be correct.
For example, the latest version in `..latest` may not keep in sync when many probational versions are approved at once.
Administrators can fix this manually by requesting a refresh of the relevant statistics.

To refresh project usage, create a file with the `request-refresh_usage-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.

On success, the usage is updated.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`,
along with a `usage` property specifying the current usage.

To refresh the latest version of an asset, create a file with the `request-refresh_latest-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
- `asset`: string containing the name of the asset.

On success, the latest version is updated.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`,
along with an optional `version` property specifying the latest non-probational version.
(If no non-probational version exists, the `version` property is omitted.)

### Reindexing a version (admin)

Administrators of a Gobbler instance can directly reindex the contents of a version directory, regenerating the various `..manifest` and `..links` files.
This is useful for correcting the Gobbler's internal files after manual changes to the contents of the version directory.
It also allows for more efficient bulk uploads where administrators can write directly to the Gobbler registry and then generate the internal files afterwards,
thus avoiding an unnecessary copy from the staging directory.

To trigger a reindexing job, create a file with the `request-reindex_version-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.
- `asset`: string containing the name of the asset.
  This should not contain `/` or `\`, or start with `..`.
- `version`: string containing the name of the version.
  This should not contain `/` or `\`, or start with `..`.

To create the manifest, reindexing will recompute the MD5 checksum and size for each non-symlink file. 
For symbolic links that are not defined in an existing `..links` file, reindexing will retrieve information about the target file,
as well as the ancestor if the target is itself a symlink.
All `..`-prefixed files are considered to be Gobbler's internal files and are excluded from the manifest.

Reindexing assumes that a `..summary` file is already present in the version directory.
This file will not be modified in order to preserve the details of the original upload.
For bulk uploads, administrators should create this file manually before submitting a reindexing request.

If any `..links` files are present in the to-be-reindexed version directory or its subdirectories, they will be used to (re)create symbolic links in their respective directories.
Any existing symbolic link of the same name will be overwritten.
This behavior ensures that information about the immediate target of a link is not lost, 
given that the symbolic links themselves only target [ancestral files](#link-deduplication).

On success, the internal `..manifest` and `..links` files inside the version directory will be created or updated.
All other files will not be modified.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.

Reindexing will not update any of the project or asset statistics.
Specifically, reindexing will not update the latest version for the asset as the `..summary` files have not changed.
Similarly, the project usage cannot be quickly updated as the Gobbler does not know whether the to-be-reindexed directory was already included in the usage.
Administrators should refresh these statistics manually as described above after completing all reindexing tasks.

### Validating a version (admin)

Administrators of a Gobbler instance can validate a version directory to check that the `..manifest` and `..links` files are accurate.
This is useful for checking the consistency of Gobbler's internal files after backups, registry relocation, etc.
It can also be used to check the correctness of manual updates to the internal files, e.g., to match manual changes to the user-supplied files.

To trigger a validation job, create a file with the `request-validate_version-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.
- `asset`: string containing the name of the asset.
  This should not contain `/` or `\`, or start with `..`.
- `version`: string containing the name of the version.
  This should not contain `/` or `\`, or start with `..`.

Validation will check that all files are captured in the manifest with the correct file sizes and MD5 checksums;
all link information in `..manifest` and `..links` are consistent with the symbolic link targets;
and the `..summary` file is correctly formatted with valid user names and upload start/end times. 

If validation is successful, the HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.
Otherwise, the response will contain a HTTP error code with a JSON object specifying the `reason` for validation failure.

Unlike reindexing, validation will not alter any files in the registry.
Any validation failures should be resolved manually by administrators.
For example, checksum mismatches may require restoration of the correct file from backups.

### Deleting content (admin)

Administrators have the ability to delete files from the registry.
This violates the Gobbler's immutability contract and should be done sparingly.
In particular, administrators must ensure that no other project links to the to-be-deleted files, otherwise those links will be invalidated -
see the ["Rerouting symlinks"](#rerouting-symlinks-admin) section for details.

To delete a project, create a file with the `request-delete_project-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.

On success, the project is deleted.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.
A success is still reported even if the project is not present, in which case the operation is a no-op.

To delete an asset, create a file with the `request-delete_asset-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.
- `asset`: string containing the name of the asset.
  This should not contain `/` or `\`, or start with `..`.
- `force` (optional): boolean indicating whether the asset should be forcibly deleted.
  Occasionally necessary if the asset contains corrupted manifest files,
  in which case they will be deleted but the project usage will need to be refreshed manually.
  Defaults to false if not supplied.

On success, the asset is deleted.
The HTTP response will contain a JSON object with the `status` property set to `SUCCESS`.
A success is still reported even if the asset or its project is not present, in which case the operation is a no-op.

To delete a version, create a file with the `request-delete_version-` prefix.
This file should be JSON-formatted with the following properties:

- `project`: string containing the name of the project.
  This should not contain `/` or `\`, or start with `..`.
- `asset`: string containing the name of the asset.
  This should not contain `/` or `\`, or start with `..`.
- `version`: string containing the name of the version.
  This should not contain `/` or `\`, or start with `..`.
- `force` (optional): boolean indicating whether the version should be forcibly deleted.
  Occasionally necessary if the version contains corrupted summary or manifest files,
  in which case they will be deleted but the project usage will need to be refreshed manually.
  Defaults to false if not supplied.

On success, the version is deleted.
The HTTP response will contain a JSON object with the `type` property set to `SUCCESS`.
A success is still reported even if the version, its asset or its project is not present, in which case the operation is a no-op.

### Rerouting symlinks (admin)

In the (hopefully rare) scenario where one or more directories must be deleted from the registry,
administrators must consider the possibility that other projects in the registry contain symbolic links to the files in the to-be-deleted directories.
Deletion would result in dangling links that compromise the validity of those other projects. 
To avoid this, the Gobbler can reroute each affected link to a more appropriate location,
either by updating the link target or replacing it with a copy of the to-be-deleted file.
After successful rerouting, each project, asset or version can be safely deleted without damaging other projects.

To reroute links, create a file with the `request-reroute_links-` prefix.
This file should be JSON-formatted with the following properties:

- `to_delete`: an array of JSON objects.
  Each object corresponds to a project, asset or version directory to be deleted.
  For a project directory, the object should contain a `project` string property that names the project;
  for an asset directory, the object should contain the `project` and `asset` string properties;
  and for a version directory, the object should contain the `project`, `asset` and `version` string properties.
  Each of the project, asset or version names should not contain `/` or `\`, or start with `..`.
- `dry_run` (optional): boolean indicating whether to perform a dry-run of the rerouting.
  If true, an array of rerouting actions is still returned but no files in the registry are actually changed.
  Defaults to false if not provided.

On success, the HTTP response will contain a JSON object with the `status` property set to `SUCCESS` and a `changes` array of rerouting actions.
Each element of the array is an object with the following properties:

- `path`: string containing the path to a symbolic link inside the registry that was changed by rerouting.
- `copy`: boolean indicating whether the link at `path` was replaced by a copy of its target file.
  If false, the link was merely updated to refer to a new target file.
- `source`: string containing the path to the target file that caused rerouting of `path`.
  Specifically, this is a file in one of the to-be-deleted directories specified in `to_delete`.
  If `copy = true`, this is the original linked-to file that was copied to `path`.
- `usage`: integer specifying the increase in project usage due to file copying.
  This will be zero if `copy = false`.

If `dry_run = false`, the Gobbler will update any links in the registry to any file in the directories corresponding to `delete`. 
All internal metadata files (`..manifest`, `..links`) are similarly updated to mirror the changes on the filesystem.

Note that a rerouting request does not actually delete the directories corresponding to `to_delete`.
After rerouting, administrators still need to delete each project, asset or version [as described above](#deleting-content-admin).
If an administrator is sure that there are no links targeting a directory (e.g., it contains probational versions only), deletion can be performed directly without the expense of rerouting. 

**Comments on efficiency:**

- We use a `to_delete` array to batch together multiple deletion tasks.
  This improves efficiency by amortizing the cost of a full registry scan to find links that target any of the affected directories.
- Deletion of projects/assets/versions from the registry can actually _increase_ disk usage if rerouting creates multiple copies of the underlying files.
  Administrators may wish to use `dry_run = true` first to evaluate if deletion will trigger excessive copying.

## Parsing logs

For some actions, the Gobbler creates a log within the `..logs/` subdirectory of the registry.
The file is named after the date/time of the action's completion, followed by an underscore, followed by a random 6-digit integer for disambiguation purposes.
The file contains a JSON object that details the type of action in the `type` property:

- `add-version` indicates that a new (non-probational) version was added, or a probational version was approved.
  This has the `project`, `asset`, `version` string properties to describe the version.
  It also has the `latest` boolean property to indicate whether the added version is the latest one for its asset.
- `delete-version` indicates that a non-probational version was deleted.
  This has the `project`, `asset`, `version` string properties to describe the version.
  It also has the `latest` boolean property to indicate whether the deleted version was the latest one for its asset.
- `delete-asset` indicates that an asset was deleted.
  This has the `project` and `asset` string property.
- `delete-project` indicates that a project was deleted.
  This has the `project` string property.
- `reindex-version` indicates that a non-probational version was reindexed.
  This has the `project`, `asset`, `version` string properties to describe the version.
  It also has the `latest` boolean property to indicate whether the reindexed version is the latest one for its asset.

Downstream systems can inspect these files to determine what changes have occurred in the registry.
This is intended for systems that need to maintain a database index on top of the bucket's contents.
By routinely scanning for changes, databases can incrementally perform updates rather than reindexing the entire bucket.

Log files are held for 7 days before deletion.

## Deployment instructions

First, clone this repository and build the binary. 
This requires the [Go toolchain](https://go.dev/dl) (version 1.16 or higher).

```bash
git clone https://github.com/ArtifactDB/gobbler
cd gobbler && go build
```

Then, set up a staging directory with global read/write permissions.
All parent directories of the staging directory should be at least globally executable.
We enable the sticky bit so that users do not interfere with each other when writing request files or creating upload directories. 
We also recommend setting up file access control lists if these are available,
as this ensures that all user-created content in the staging directory can be eventually deleted by the Gobbler's service account.

```bash
mkdir STAGING
chmod 1777 STAGING # 1 for the sticky bit
setfacl -Rdm u:SERVICE_ACCOUNT_NAME:rwx STAGING
```

Next, set up a registry directory with global read-only permissions.
Note that the registry and staging directories do not need to be on the same filesystem (e.g., for mounted shares), as long as both are accessible to users. 

```bash
mkdir REGISTRY
chmod 755 REGISTRY
```

Finally, start the Gobbler by running the binary:

```bash
./gobbler \
    -staging STAGING \
    -registry REGISTRY
```

The following optional arguments can be used to fine-tune the Gobbler's behavior:

- `-admin` contains a comma-separated list of administrator UIDs. 
  This defaults to an empty string, i.e., no administrators.
- `-port` specifies the port for API calls.
  This defaults to 8080.
- `-prefix` adds an extra prefix to all endpoints, e.g., to disambiguate between versions.
  For example, a prefix of `api/v2` would change the list endpoint to `/api/v2/list`.
  This defaults to an empty string, i.e., no prefix.
- `-whitelist` contains a path to a text file where each line contains a path to a directory.
  Any symbolic link that targets a file in a whitelisted directory is treated as the file itself during upload and reindexing.
  This is useful for avoiding unnecessary copies from data archives.
  By default, no directories are whitelisted.
- `-spoof` contains a path to a text file containing the spoofing permissions.
  Each line should be formatted as `[spoofer]:[comma-separated list of users]`, where `spoofer` may perform requests on behalf of the listed users in supported endpoints.
  Alternatively, a line may contain `[spoofer]:*` to indicate that `spoofer` is allowed to pretend to be any user.
  Lines containing `[spoofer]:` without any users will be ignored.
  By default, no spoofing is permitted.
- `-probation` specifies the lifespan of probational versions in days, after which they will be automatically deleted.
  The default value of -1 will not perform any deletion. 
- `-concurrency` specifies the maximum number of active goroutines, mostly for filesystem operations.
  This defaults to 100 but can be changed according to the filesystem parallelism, number of available CPUs, maximum number of open file handles, etc.
  (Goroutines for processing HTTP requests are not considered in this limit.)

Multiple Gobbler instances can safely target the same `REGISTRY` with different `STAGING`.
This is useful for complex HPC configurations where the same filesystem is mounted in multiple compute environments;
a separate Gobbler instance can be set up in each environment to enable uploads.
