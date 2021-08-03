package main

import (
	proto2 "NATS-learning/proto"
	"fmt"
	"github.com/golang/protobuf/proto"
	stan "github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats"
	"log"
	"runtime"
)

const (
	clusterID = "test-cluster"
	clientID  = "user-server"
)

// protoc -Iproto proto/user.proto --go_out=./proto ======>>> proto generator
var nc *nats.Conn
var users []proto2.User

func main() {
	var err error
	// terminal : nats-server
	nc, err = nats.Connect(nats.DefaultURL)

	// terminal : nats-streaming-server -p 4233
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL("http://localhost:4233"))
	if err != nil {
		fmt.Println(err)
	}

	// Fake Data
	users = append(users, proto2.User{Id: 1, Name: "bob", Lastname: "bobby", City: "NY"})
	users = append(users, proto2.User{Id: 2, Name: "John", Lastname: "Johnny", City: "PARIS"})
	users = append(users, proto2.User{Id: 3, Name: "Dan", Lastname: "Danny", City: "LA"})
	users = append(users, proto2.User{Id: 4, Name: "Kate", Lastname: "Katey", City: "MILAN"})


	// get user id and send back user object (request- reply)
	_, err = nc.Subscribe("GetUserById", replyWithUserId)
	if err != nil {
		fmt.Println(err)
	}


	// stream users to client
	for _, u := range users {
		data, err := proto.Marshal(&u)
		if err != nil {
			fmt.Println(err)
			return
		}

		ackHandler := func(ackedNuid string, err error) {
			if err != nil {
				log.Printf("Error publishing message id %s: %v\n", ackedNuid, err.Error())
			} else {
				log.Printf("Received ACK for message id %s\n", ackedNuid)
			}
		}

		//fmt.Println("Replying to ", m.Reply)
		nuid, err := sc.PublishAsync("GetUsers", data, ackHandler)
		fmt.Println("nuid :", nuid)

	}

	runtime.Goexit()
}

func replyWithUserId(m *nats.Msg) {
	myUser := proto2.User{}
	err := proto.Unmarshal(m.Data, &myUser)
	fmt.Println(&myUser)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, u := range users {
		fmt.Println(u.String())
		if u.Id == myUser.Id {
			data, err := proto.Marshal(&u)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(data)

			fmt.Println("Replying to ", m.Reply)
			nc.Publish(m.Reply, data)
		}
	}
}
