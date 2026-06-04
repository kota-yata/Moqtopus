# Moqtopus
MoQ Subscriber API over MsQUIC. Moqtopus keeps the dependency graph fairly small, which helps a lot when the rest of your application already take a gazillion year building (like UE).

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

It prints received object metadata and payload sizes.

The generic subscriber executable accepts connection details on the command
line:

```sh
./build/moqtopus <host> <port> <namespace[/field...]> <track-name> [path] [alpn]
```

Example:

```sh
./build/moqtopus localhost 4433 camera/front video / moqt-18
```
