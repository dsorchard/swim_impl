package main

import "swim/proj_memberlist/memberlist"

func main() {
	config := memberlist.DefaultLocalConfig()
	config.Name = "127.0.0.1:8001"
	config.BindAddr = "127.0.0.1"
	config.BindPort = 8000

	membershipChangeCh := make(chan memberlist.NodeEvent, 16)
	config.Events = &memberlist.ChannelEventDelegate{
		Ch: membershipChangeCh,
	}

	membershipList, err := memberlist.Create(config)
	if err != nil {
		panic(err)
	}

	existing := []string{"127.0.0.1:8000"}
	_, err = membershipList.Join(existing)
	if err != nil {
		panic(err)
	}

}
