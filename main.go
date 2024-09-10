package main

import (
	"encoding/json"
	"fmt"
	ride "github.com/KathirvelChandrasekaran/grpc_poc/ride_data"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

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

// RideServer struct implementing the Ride service
type RideServer struct {
	ride.UnimplementedRideServer
}

// mapJSONToRideData maps a RideDataJSON to the gRPC RideData message
func mapJSONToRideData(jsonData *RideDataJSON) *ride.RideData {
	return &ride.RideData{
		Key:              jsonData.Key,
		FareAmount:       jsonData.FareAmount,
		PickupDatetime:   jsonData.PickupDatetime,
		PickupLongitude:  jsonData.PickupLongitude,
		PickupLatitude:   jsonData.PickupLatitude,
		DropoffLongitude: jsonData.DropoffLongitude,
		DropoffLatitude:  jsonData.DropoffLatitude,
		PassengerCount:   jsonData.PassengerCount,
	}
}

func (s *RideServer) Create(req *ride.CreateRideRequest, stream ride.Ride_CreateServer) error {
	jsonData, err := os.ReadFile("300MB.json")
	if err != nil {
		panic(err)
	}

	// Unmarshal the JSON data into a slice of RideDataJSON structs
	var rides []RideDataJSON
	err = json.Unmarshal(jsonData, &rides)
	if err != nil {
		panic(err)
	}

	// Divide the rides into four chunks
	chunkSize := len(rides) / 4
	chunks := make([][]RideDataJSON, 0, chunkSize)

	for i := 0; i < len(rides); i += chunkSize {
		end := i + chunkSize
		if end > len(rides) {
			end = len(rides)
		}
		chunks = append(chunks, rides[i:end])
	}

	// Print the chunks
	for i, chunk := range chunks {
		fmt.Printf("Chunk %d:\n Total Count - %d \n", i+1, len(chunk))
		rides := make([]*ride.RideData, 0, chunkSize)
		for _, ride := range chunk {
			rides = append(rides, mapJSONToRideData(&ride))
		}
		chunk := &ride.CreateRideResponse{
			CreatedRides: rides,
		}
		if err := stream.Send(chunk); err != nil {
			log.Printf("Failed to send chunk: %v", err)
			return err
		}
	}
	return nil
}

func main() {
	// Create a TCP listener on port 8080
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}

	// Set up the gRPC server
	server := grpc.NewServer()
	ride.RegisterRideServer(server, &RideServer{})

	// Start the gRPC server
	log.Println("Server is running on port 8080")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
}
