# Fuzzer

To build protos:

```sh
protoc -I src/fuzzer/ src/fuzzer/fuzzer.proto --go_out=plugins=grpc:src/fuzzer
```
