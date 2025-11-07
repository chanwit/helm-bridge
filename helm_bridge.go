// Copyright (C) ConfigHub, Inc.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confighub/sdk/bridge-worker/api"
	"github.com/confighub/sdk/bridge-worker/impl"
	"github.com/confighub/sdk/bridge-worker/lib"
	"github.com/confighub/sdk/helmutils"
	"github.com/confighub/sdk/workerapi"
	ssautil "github.com/fluxcd/pkg/ssa/utils"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// HelmBridge implements the Bridge interface for Helm operations
// It embeds KubernetesBridgeWorker and overrides applier selection
type HelmBridge struct {
	*impl.KubernetesBridgeWorker
	name string
}

// NewHelmBridge creates a new HelmBridge instance
func NewHelmBridge(name string) (*HelmBridge, error) {
	k8sWorker := impl.NewKubernetesBridgeWorker()

	return &HelmBridge{
		KubernetesBridgeWorker: k8sWorker,
		name:                   name,
	}, nil
}

// Info returns information about the bridge's capabilities
func (hb *HelmBridge) Info(opts api.InfoOptions) api.BridgeInfo {
	return hb.KubernetesBridgeWorker.InfoForToolchainAndProvider(
		opts,
		workerapi.ToolchainKubernetesYAML,
		api.ProviderKubernetes,
	)
}

// parseObjects parses YAML data into Kubernetes objects
func parseObjects(data []byte) ([]*unstructured.Unstructured, error) {
	objects, err := ssautil.ReadObjects(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML resources: %v", err)
	}
	return objects, nil
}

// objectsToYAML converts Kubernetes objects to YAML string
func objectsToYAML(objects []*unstructured.Unstructured) (string, error) {
	yamlData, err := ssautil.ObjectsToYAML(objects)
	if err != nil {
		return "", err
	}
	return yamlData, nil
}

// selectApplier chooses between HelmApplier, CRDsApplier, and CLIUtilsSSA
// Logic: CRD-only resources → CRDsApplier, Helm chart → HelmApplier, everything else → CLIUtilsSSA
func (hb *HelmBridge) selectApplier(payload api.BridgePayload) (impl.K8sApplier, error) {
	// Parse objects first to check if they're all CRDs
	objects, err := parseObjects(payload.Data)
	if err != nil {
		log.Log.Info("Failed to parse objects, using CLIUtilsSSA", "error", err)
		return hb.createCLIUtilsApplier(payload)
	}

	// Check if all resources are CRDs - use specialized CRDsApplier
	if allResourcesAreCRDs(objects) {
		log.Log.Info("All resources are CRDs, using CRDsApplier",
			"crdCount", len(objects),
			"unitSlug", payload.UnitSlug)
		return hb.createCRDsApplier(payload)
	}

	// Check if this is a Helm chart
	if !helmutils.IsHelmChart(payload.UnitLabels) {
		log.Log.Info("Using CLIUtilsSSA", "unitSlug", payload.UnitSlug)
		return hb.createCLIUtilsApplier(payload)
	}

	// Use HelmApplier for Helm charts
	log.Log.Info("Using HelmApplier",
		"release", payload.UnitLabels["HelmRelease"],
		"chart", payload.UnitLabels["HelmChart"])
	return hb.createHelmApplier(payload)
}

// allResourcesAreCRDs checks if all objects are CRDs
func allResourcesAreCRDs(objects []*unstructured.Unstructured) bool {
	if len(objects) == 0 {
		return false
	}
	for _, obj := range objects {
		gvk := obj.GroupVersionKind()
		if gvk.Group != "apiextensions.k8s.io" || gvk.Kind != "CustomResourceDefinition" {
			return false
		}
	}
	return true
}

// createCLIUtilsApplier creates a CLIUtilsSSA applier
func (hb *HelmBridge) createCLIUtilsApplier(payload api.BridgePayload) (impl.K8sApplier, error) {
	config, err := createApplierConfig(payload)
	if err != nil {
		return nil, err
	}
	return impl.NewK8sApplier(impl.CLIUtilsSSA, config)
}

// createHelmApplier creates a HelmApplier
func (hb *HelmBridge) createHelmApplier(payload api.BridgePayload) (impl.K8sApplier, error) {
	config, err := createHelmApplierConfig(payload)
	if err != nil {
		return nil, err
	}
	return NewHelmApplier(config)
}

// createCRDsApplier creates a CRDsApplier wrapped in an adapter
func (hb *HelmBridge) createCRDsApplier(payload api.BridgePayload) (impl.K8sApplier, error) {
	workerParams, _, err := parseTargetParams(payload)
	if err != nil {
		return nil, err
	}

	// Parse timeout
	timeout := impl.DefaultTimeout
	if workerParams.WaitTimeout != "" {
		if t, parseErr := time.ParseDuration(workerParams.WaitTimeout); parseErr == nil {
			timeout = t
		}
	}

	// Create the native CRDsApplier
	crdsApplier, err := NewCRDsApplier(timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create CRDsApplier: %w", err)
	}

	// Wrap it in an adapter that implements impl.K8sApplier
	return NewCRDsApplierAdapter(crdsApplier), nil
}

// createApplierConfig creates impl.ApplierConfig from payload
func createApplierConfig(payload api.BridgePayload) (impl.ApplierConfig, error) {
	workerParams, kubeContext, err := parseTargetParams(payload)
	if err != nil {
		return impl.ApplierConfig{}, err
	}
	return impl.ApplierConfig{
		KubeContext: kubeContext,
		LiveState:   payload.LiveState,
		SpaceID:     payload.SpaceID.String(),
		UnitSlug:    payload.UnitSlug,
		WaitTimeout: workerParams.WaitTimeout,
	}, nil
}

// createHelmApplierConfig creates HelmApplierConfig from payload
func createHelmApplierConfig(payload api.BridgePayload) (HelmApplierConfig, error) {
	baseConfig, err := createApplierConfig(payload)
	if err != nil {
		return HelmApplierConfig{}, err
	}
	return HelmApplierConfig{
		ApplierConfig: baseConfig,
		UnitLabels:    payload.UnitLabels,
	}, nil
}

// parseTargetParams extracts parameters from payload
func parseTargetParams(payload api.BridgePayload) (impl.KubernetesWorkerParams, string, error) {
	var params impl.KubernetesWorkerParams
	if len(payload.TargetParams) > 0 {
		if err := json.Unmarshal(payload.TargetParams, &params); err != nil {
			return params, "", err
		}
	}
	return params, params.KubeContext, nil
}

// Override Apply to use custom applier selection
func (hb *HelmBridge) Apply(wctx api.BridgeContext, payload api.BridgePayload) error {
	applier, err := hb.selectApplier(payload)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyFailed,
			err.Error(),
		), err)
	}

	objects, err := parseObjects(payload.Data)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyFailed,
			err.Error(),
		), err)
	}

	if err := wctx.SendStatus(newActionResult(
		api.ActionStatusProgressing,
		api.ActionResultNone,
		"Applying resources...",
	)); err != nil {
		return err
	}

	result := applier.Apply(wctx.Context(), objects)
	if result.Error != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyFailed,
			result.Error.Error(),
		), result.Error)
	}

	return wctx.SendStatus(newActionResult(
		api.ActionStatusProgressing,
		api.ActionResultNone,
		"Resources applied successfully",
	))
}

// Override Refresh to use custom applier selection
func (hb *HelmBridge) Refresh(wctx api.BridgeContext, payload api.BridgePayload) error {
	applier, err := hb.selectApplier(payload)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultRefreshFailed,
			err.Error(),
		), err)
	}

	objects, err := parseObjects(payload.Data)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultRefreshFailed,
			err.Error(),
		), err)
	}

	if err := wctx.SendStatus(newActionResult(
		api.ActionStatusProgressing,
		api.ActionResultNone,
		"Retrieving resources...",
	)); err != nil {
		return err
	}

	_, err = applier.Refresh(wctx.Context(), objects)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultRefreshFailed,
			err.Error(),
		), err)
	}

	// Delegate to base implementation for drift detection
	return hb.KubernetesBridgeWorker.Refresh(wctx, payload)
}

// Override Destroy to use custom applier selection
func (hb *HelmBridge) Destroy(wctx api.BridgeContext, payload api.BridgePayload) error {
	applier, err := hb.selectApplier(payload)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyFailed,
			err.Error(),
		), err)
	}

	objects, err := parseObjects(payload.Data)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyFailed,
			err.Error(),
		), err)
	}

	if err := wctx.SendStatus(newActionResult(
		api.ActionStatusProgressing,
		api.ActionResultNone,
		"Destroying resources...",
	)); err != nil {
		return err
	}

	result := applier.Destroy(wctx.Context(), objects)
	if result.Error != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyFailed,
			result.Error.Error(),
		), result.Error)
	}

	return nil
}

// Override WatchForApply to use custom applier selection
func (hb *HelmBridge) WatchForApply(wctx api.BridgeContext, payload api.BridgePayload) error {
	applier, err := hb.selectApplier(payload)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyWaitFailed,
			err.Error(),
		), err)
	}

	objects, err := parseObjects(payload.Data)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyWaitFailed,
			err.Error(),
		), err)
	}

	if err := wctx.SendStatus(newActionResult(
		api.ActionStatusProgressing,
		api.ActionResultNone,
		"Waiting for resources...",
	)); err != nil {
		return err
	}

	workerParams, _, err := parseTargetParams(payload)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyWaitFailed,
			err.Error(),
		), err)
	}

	// Parse timeout
	timeout := impl.DefaultTimeout
	if workerParams.WaitTimeout != "" {
		if t, parseErr := time.ParseDuration(workerParams.WaitTimeout); parseErr == nil {
			timeout = t
		}
	}

	waitResult := applier.WaitForApply(wctx.Context(), objects, timeout)
	if waitResult.Error != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyWaitFailed,
			waitResult.Error.Error(),
		), waitResult.Error)
	}

	// Build final result from waitResult (don't delegate to base - it would re-poll all objects including hooks)
	yamlData, err := objectsToYAML(waitResult.LiveObjects)
	if err != nil {
		log.Log.Error(err, "Failed to convert objects to YAML")
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultApplyWaitFailed,
			fmt.Sprintf("Failed to convert objects to YAML: %v", err),
		), err)
	}

	// Log changeset info
	changesetCount := 0
	if waitResult.ResourceSet != nil {
		changesetCount = len(waitResult.ResourceSet.GetEntries())
	}
	log.Log.Info("✅ Resources are ready - operation complete", "changeset_entries", changesetCount, "liveObjects", len(waitResult.LiveObjects))

	status := newActionResult(
		api.ActionStatusCompleted,
		api.ActionResultApplyCompleted,
		fmt.Sprintf("Applied %d resources successfully at %s", len(waitResult.LiveObjects), time.Now().Format(time.RFC3339)),
	)
	status.LiveState = []byte(yamlData)

	wctx.SendStatus(status)
	return nil
}

// Override WatchForDestroy to use custom applier selection
func (hb *HelmBridge) WatchForDestroy(wctx api.BridgeContext, payload api.BridgePayload) error {
	applier, err := hb.selectApplier(payload)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyWaitFailed,
			err.Error(),
		), err)
	}

	objects, err := parseObjects(payload.Data)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyWaitFailed,
			err.Error(),
		), err)
	}

	if err := wctx.SendStatus(newActionResult(
		api.ActionStatusProgressing,
		api.ActionResultNone,
		"Waiting for resource deletion...",
	)); err != nil {
		return err
	}

	workerParams, _, err := parseTargetParams(payload)
	if err != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyWaitFailed,
			err.Error(),
		), err)
	}

	// Parse timeout - use longer timeout for CRDs
	timeout := impl.DefaultTimeout
	if workerParams.WaitTimeout != "" {
		if t, parseErr := time.ParseDuration(workerParams.WaitTimeout); parseErr == nil {
			timeout = t
		}
	}

	// CRDs often take longer to delete due to finalizers and dependent resources
	// Increase timeout for CRD-only resources
	if allResourcesAreCRDs(objects) {
		crdTimeout := timeout * 3 // 3x the normal timeout
		if crdTimeout < 15*time.Minute {
			crdTimeout = 15 * time.Minute // Minimum 15 minutes for CRDs
		}
		log.Log.Info("🔧 Using extended timeout for CRD deletion",
			"standardTimeout", timeout,
			"crdTimeout", crdTimeout,
			"crdCount", len(objects))
		timeout = crdTimeout
	}

	waitResult := applier.WaitForDestroy(wctx.Context(), objects, timeout)
	if waitResult.Error != nil {
		// Check if error is due to context cancellation (which might be normal)
		if err := wctx.Context().Err(); err != nil {
			log.Log.Info("⚠️ Destroy watcher context ended", "reason", err.Error())
		}
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyWaitFailed,
			waitResult.Error.Error(),
		), waitResult.Error)
	}

	// Build final result from waitResult (don't delegate to base - it would re-poll all objects including hooks)
	// Log changeset info
	changesetCount := 0
	if waitResult.ResourceSet != nil {
		changesetCount = len(waitResult.ResourceSet.GetEntries())
	}
	log.Log.Info("✅ Resources destroyed successfully - operation complete", "changeset_entries", changesetCount)

	status := newActionResult(
		api.ActionStatusCompleted,
		api.ActionResultDestroyCompleted,
		fmt.Sprintf("Destroyed %d resources successfully at %s", changesetCount, time.Now().Format(time.RFC3339)),
	)
	status.LiveState = nil // No live state after destroy

	wctx.SendStatus(status)
	return nil
}

// newActionResult creates an ActionResult with common fields
func newActionResult(status api.ActionStatusType, result api.ActionResultType, message string) *api.ActionResult {
	return &api.ActionResult{
		ActionResultBaseMeta: api.ActionResultMeta{
			Status:  status,
			Result:  result,
			Message: message,
		},
	}
}

// Ensure HelmBridge implements the Bridge interface
var _ api.Bridge = (*HelmBridge)(nil)
