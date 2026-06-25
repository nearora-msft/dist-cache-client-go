// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// To regenerate cache.pb.go from cache.proto:
//   go generate ./proto/

package proto

//go:generate protoc --go_out=. --go_opt=paths=source_relative cache.proto
