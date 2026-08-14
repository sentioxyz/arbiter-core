module github.com/sentioxyz/arbiter-core

go 1.26.3

replace github.com/wasmerio/wasmer-go => github.com/sentioxyz/wasmer-go v1.0.5-0.20250206064014-c65a8b154145

replace github.com/ClickHouse/ch-go => github.com/sentioxyz/ch-go v0.73.0-sentioxyz-20260629

replace github.com/ClickHouse/clickhouse-go/v2 => github.com/sentioxyz/clickhouse-go/v2 v2.47.0-sentioxyz-20260629

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.46.0
	github.com/ethereum/go-ethereum v1.17.2
	github.com/sentioxyz/arbiter-proto v0.4.0
	google.golang.org/grpc v1.82.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/ClickHouse/ch-go v0.73.0 // indirect
	github.com/ProjectZKM/Ziren/crates/go-runtime/zkvm_runtime v0.0.0-20251001021608-1fe7b43fc4d6 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/housegate/housegate v0.8.1
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/zeebo/blake3 v0.2.4 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)
