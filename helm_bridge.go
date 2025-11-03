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

// selectApplier chooses between HelmApplier and CLIUtilsSSA
// Simple logic: Helm chart → HelmApplier, everything else → CLIUtilsSSA
func (hb *HelmBridge) selectApplier(payload api.BridgePayload) (impl.K8sApplier, error) {
	// Check if this is a Helm chart
	if !helmutils.IsHelmChart(payload.UnitLabels) {
		log.Log.Info("Using CLIUtilsSSA", "unitSlug", payload.UnitSlug)
		return hb.createCLIUtilsApplier(payload)
	}

	// Check if all resources are CRDs (exception case)
	objects, err := parseObjects(payload.Data)
	if err != nil {
		log.Log.Info("Failed to parse objects, using CLIUtilsSSA", "error", err)
		return hb.createCLIUtilsApplier(payload)
	}

	if allResourcesAreCRDs(objects) {
		log.Log.Info("Helm chart with only CRDs, using CLIUtilsSSA",
			"chart", payload.UnitLabels["HelmChart"],
			"crdCount", len(objects))
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

	// Delegate to base implementation for final status handling
	return hb.KubernetesBridgeWorker.WatchForApply(wctx, payload)
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

	// Parse timeout
	timeout := impl.DefaultTimeout
	if workerParams.WaitTimeout != "" {
		if t, parseErr := time.ParseDuration(workerParams.WaitTimeout); parseErr == nil {
			timeout = t
		}
	}

	waitResult := applier.WaitForDestroy(wctx.Context(), objects, timeout)
	if waitResult.Error != nil {
		return lib.SafeSendStatus(wctx, newActionResult(
			api.ActionStatusFailed,
			api.ActionResultDestroyWaitFailed,
			waitResult.Error.Error(),
		), waitResult.Error)
	}

	// Delegate to base implementation for final status handling
	return hb.KubernetesBridgeWorker.WatchForDestroy(wctx, payload)
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
