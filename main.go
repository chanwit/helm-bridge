// Copyright (C) ConfigHub, Inc.
// SPDX-License-Identifier: MIT

package main

import (
	"log"
	"os"

	"github.com/confighub/sdk/worker"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func main() {
	// Initialize controller-runtime logger
	ctrllog.SetLogger(zap.New(zap.UseDevMode(true)))

	log.Printf("[INFO] Starting helm-bridge...")

	// Create bridge dispatcher
	bridgeDispatcher := worker.NewBridgeDispatcher()

	// Create and register the Helm bridge
	bridge, err := NewHelmBridge("helm-bridge")
	if err != nil {
		log.Fatalf("Failed to create bridge: %v", err)
	}
	bridgeDispatcher.RegisterBridge(bridge)

	// Create connector with ConfigHub credentials
	connector, err := worker.NewConnector(worker.ConnectorOptions{
		WorkerID:         os.Getenv("CONFIGHUB_WORKER_ID"),
		WorkerSecret:     os.Getenv("CONFIGHUB_WORKER_SECRET"),
		ConfigHubURL:     os.Getenv("CONFIGHUB_URL"),
		BridgeDispatcher: &bridgeDispatcher,
	})

	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}

	log.Printf("[INFO] Starting connector...")
	err = connector.Start()
	if err != nil {
		log.Fatalf("Failed to start connector: %v", err)
	}
}
