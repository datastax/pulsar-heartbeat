# Release

## How to build a Docker release image

The Docker release image is built by the automated Docker Hub build process. The build is triggered by Github remote tagging.

Here is an example how to tag and push to `origin` that will automatically trigger a Docker Hub build.

### Tag Requirements
1. The tag has to conform the regex `^[0-9.]+`. It's recommended to follow major.minor.patch pattern in decimal format.
2. The release should be tagged on the `master` branch.
3. The release tag must be incremented.

### Tag and Push to remote
A new tag pushed to the main repo (usually `origin`) will trigger a Docker Hub build.

1.  Checkout the desired branch/commit
    ```
    $ git checkout master && git pull
    ```

2.  Create a signed git tag with the desired version
    ```
    $ git -s tag 1.0.0
    ```

3.  Push to the remote repo (e.g. `origin`)
    ```
    $ git push origin 1.0.0
    ```

4. Wait for the image to show up on dockerhub (https://hub.docker.com/r/datastax/pulsar-heartbeat/tags)
