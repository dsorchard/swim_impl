package main

import "flag"

func main() {
	var gossipPort int
	flag.IntVar(&gossipPort, "gossip", 8080, "port number for gossip protocol")
	flag.Parse()

	swimring := NewSwimRing(gossipPort)
	_, err := swimring.Bootstrap()
	if err != nil {
		panic(err)
	}
}
