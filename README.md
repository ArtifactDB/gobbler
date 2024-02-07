# Genomics Platform Data Gobbler

## About

The Genomics Platform Data Gobbler is a service that gobbles up user-supplied files on a shared computing environment with a POSIX-compliant filesystem.
No HTTP requests are required, as all communication between the user and Gobbler is performed through filesystem events and/or polling.
No authentication is required, either, as we can pick up the ID of the user directly from the file permissions.
This eliminates the need for any network communication and improves efficiency by directly copying user files to the global registry.
