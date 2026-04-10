#!/usr/bin/env bash
set -euo pipefail

# Generate Python gRPC stubs from proto
# Requires: pip install grpcio-tools

PROTO_DIR="$(dirname "$0")/../proto"
OUT_DIR="$(dirname "$0")/../sdk/python/vantadb_sdk"

python -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/vantadb.proto"

# Fix relative import in generated grpc file
sed -i 's/import vantadb_pb2/from . import vantadb_pb2/' "$OUT_DIR/vantadb_pb2_grpc.py"

echo "Python stubs generated in $OUT_DIR"
