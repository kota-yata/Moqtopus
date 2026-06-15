# Moqtopus
MoQ Client API over MsQUIC. Moqtopus keeps the dependency graph fairly small, which helps a lot when the rest of your application already take a gazillion year building (like UE).

## Build
Moqtopus requires [vcpkg](https://github.com/microsoft/vcpkg). Set
`VCPKG_ROOT` to the vcpkg checkout before configuring the project.

```sh
export VCPKG_ROOT=/path/to/vcpkg
cmake --preset=vcpkg
cmake --build build
```

## Use from CMake

Once installed, Moqtopus provides a CMake config package:

```cmake
find_package(moqtopus CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE moqtopus::moq_subscriber)
```

To install a local checkout:

```sh
cmake --preset=vcpkg -DMOQTOPUS_BUILD_EXAMPLES=OFF -DMOQTOPUS_BUILD_TOOLS=OFF
cmake --build build
cmake --install build --prefix /path/to/prefix
```

## Run the Example

The subscriber example accepts connection details on the command line and
prints received object metadata and a payload preview until the publisher
finishes or the process receives SIGINT:

```sh
cmake --build build --target subscriber
./build/subscriber <host> <port> <namespace[/field...]> <track-name> [path] [alpn]
```

Example:

```sh
./build/subscriber localhost 4433 camera/front video / moqt-18
```
