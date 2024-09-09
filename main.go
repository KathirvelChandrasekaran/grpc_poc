package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	ride "grpc_poc/ride_data"
	"io"
	"log"
	"net"
	"os"
)

type myRiderServer struct {
	ride.UnimplementedRideServer
}

type RideDataJSON struct {
	Key              string `json:"key"`
	FareAmount       string `json:"fare_amount"`
	PickupDatetime   string `json:"pickup_datetime"`
	PickupLongitude  string `json:"pickup_longitude"`
	PickupLatitude   string `json:"pickup_latitude"`
	DropoffLongitude string `json:"dropoff_longitude"`
	DropoffLatitude  string `json:"dropoff_latitude"`
	PassengerCount   string `json:"passenger_count"`
}

func (s myRiderServer) Create(ctx context.Context, req *ride.CreateRideRequest) (*ride.CreateRideResponse, error) {
	file, err := os.Open("30MB.json")
	if err != nil {
		log.Fatalf("Failed to open JSON file: %v", err)
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	// Expect an array at the beginning of the JSON
	_, err = decoder.Token() // Advance past the opening '['
	if err != nil {
		log.Fatalf("Failed to read opening array token: %v", err)
		return nil, err
	}

	var rideData []*ride.RideData
	for decoder.More() {
		var data RideDataJSON
		// Decode each object in the array
		if err := decoder.Decode(&data); err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Failed to decode JSON: %v", err)
			return nil, err
		}

		// Append the unmarshaled data
		rideData = append(rideData, &ride.RideData{
			Key:              data.Key,
			FareAmount:       data.FareAmount,
			PickupDatetime:   data.PickupDatetime,
			PickupLongitude:  data.PickupLongitude,
			PickupLatitude:   data.PickupLatitude,
			DropoffLongitude: data.DropoffLongitude,
			DropoffLatitude:  data.DropoffLatitude,
			PassengerCount:   data.PassengerCount,
		})
	}

	// Read the closing ']' for the array
	_, err = decoder.Token()
	if err != nil {
		log.Fatalf("Failed to read closing array token: %v", err)
		return nil, err
	}

	return &ride.CreateRideResponse{CreatedRides: rideData}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("cannot create listener: %s", err)
	}

	// Set custom message size limit
	serverRegistrar := grpc.NewServer(
		grpc.MaxRecvMsgSize(50*1024*1024), // 50MB for receiving messages
		grpc.MaxSendMsgSize(50*1024*1024), // 50MB for sending messages
	)
	service := &myRiderServer{}
	ride.RegisterRideServer(serverRegistrar, service)

	err = serverRegistrar.Serve(lis)
	if err != nil {
		log.Fatalf("impossible to serve: %s", err)
	}
}
