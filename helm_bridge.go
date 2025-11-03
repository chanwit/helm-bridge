// Copyright (C) ConfigHub, Inc.
// SPDX-License-Identifier: MIT

package main

import (
	"github.com/confighub/sdk/bridge-worker/api"
	"github.com/confighub/sdk/bridge-worker/impl"
	"github.com/confighub/sdk/workerapi"
)

// HelmBridge implements the Bridge interface for Helm operations
// It embeds KubernetesBridgeWorker and delegates all operations except Info()
type HelmBridge struct {
	*impl.KubernetesBridgeWorker
	name string
}

// NewHelmBridge creates a new HelmBridge instance
func NewHelmBridge(name string) (*HelmBridge, error) {
	// Create the embedded KubernetesBridgeWorker
	k8sWorker := impl.NewKubernetesBridgeWorker()

	return &HelmBridge{
		KubernetesBridgeWorker: k8sWorker,
		name:                   name,
	}, nil
}

// Info returns information about the bridge's capabilities
// This discovers available Kubernetes contexts as targets
// Delegates to the embedded KubernetesBridgeWorker but uses its own name
func (hb *HelmBridge) Info(opts api.InfoOptions) api.BridgeInfo {
	// Use the embedded worker's InfoForToolchainAndProvider with Kubernetes provider
	return hb.KubernetesBridgeWorker.InfoForToolchainAndProvider(
		opts,
		workerapi.ToolchainKubernetesYAML,
		api.ProviderKubernetes,
	)
}


// Ensure HelmBridge implements the Bridge interface
var _ api.Bridge = (*HelmBridge)(nil)
