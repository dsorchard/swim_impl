# SWIM Implementations

Here I'm trying to read/understand the SWIM implementations in existing libraries and relate it to the paper details.

- [SWIM Ring/ RingPop](proj_swimring_ringpop)
- [Hashicorp Memberlist](proj_memberlist)

## Good Resource
- [Visualizer](https://flopezluis.github.io/gossip-simulator/?ref=highscalability.com)
- [SWIM Paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)

## Core Ideas
- Dissemination Protocol 
- Failure Detection Protocol: Direct Send vs Indirect Send (Ping, PingRequest, PingResponse) 
- Incarnation Number & Suspect Protocol 
- Round Robin Selection of Next Member to Ping

## Good Videos (Watch later)
- https://www.youtube.com/watch?v=Gxf5glthqrk&t=1418s
- https://www.youtube.com/watch?v=QGMgktRBeeU
