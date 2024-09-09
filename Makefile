generate_grpc_code:
	protoc \
    --go_out=ride_data \
    --go_opt=paths=source_relative \
    --go-grpc_out=ride_data \
    --go-grpc_opt=paths=source_relative \
    ride_data.proto