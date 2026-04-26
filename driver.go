package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

var n = 4

func main() {
	err := spawnWorkers(n)
	if err != nil {
		log.Fatal(err)
	}
}

func spawnWorkers(n int) error {
	ctx := context.Background()

	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return err
	}

	containerPort, err := nat.NewPort("tcp", "2001")
	if err != nil {
		return err
	}

	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("rpc-worker%d", i)
		hostPort := strconv.Itoa(9002 + i) // 1->9003, 2->9004, etc.

		resp, err := cli.ContainerCreate(
			ctx,
			&container.Config{
				Image: "rpc-worker:latest",
				Cmd:   []string{fmt.Sprintf("worker%d", i)},
				ExposedPorts: nat.PortSet{
					containerPort: struct{}{},
				},
			},
			&container.HostConfig{
				NetworkMode: "distributed-web-crawler_default",
				AutoRemove: true,
				PortBindings: nat.PortMap{
					containerPort: []nat.PortBinding{
						{
							HostIP:   "0.0.0.0",
							HostPort: hostPort,
						},
					},
				},
			},
			nil,
			nil,
			name,
		)
		if err != nil {
			return fmt.Errorf("create %s: %w", name, err)
		}

		if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("start %s: %w", name, err)
		}

		fmt.Printf("started %s on %s:2001\n", name, hostPort)
	}

	return nil
}