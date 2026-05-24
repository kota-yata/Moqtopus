# Moqtopus
MoQ Subscriber API over MsQUIC

## Build
Moqtopus requires [vcpkg](https://github.com/microsoft/vcpkg). Set
`VCPKG_ROOT` to the vcpkg checkout before configuring the project.

```sh
export VCPKG_ROOT=/path/to/vcpkg
cmake --preset=vcpkg
cmake --build build
```

## Run the Example

```sh
cmake --build build --target moqtail_video_subscriber
./build/moqtail_video_subscriber
```

It prints received object metadata and payload sizes until the publisher
finishes or you stop it with `Ctrl-C`.

The generic subscriber executable accepts connection details on the command
line:

```sh
./build/moqtopus <host> <port> <namespace[/field...]> <track-name> [path] [alpn]
```

Example:

```sh
./build/moqtopus localhost 4433 camera/front video / moqt-18
```
