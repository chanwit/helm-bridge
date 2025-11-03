// Copyright (C) ConfigHub, Inc.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confighub/sdk/bridge-worker/impl"
	"github.com/confighub/sdk/helmutils"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/kube"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/releaseutil"
	helmtime "helm.sh/helm/v3/pkg/time"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

// HelmApplier implements impl.K8sApplier using Helm's native install/upgrade/uninstall mechanics
type HelmApplier struct {
	settings   *cli.EnvSettings
	spaceID    string
	unitSlug   string
	unitLabels map[string]string // Unit labels containing Helm metadata
	timeout    time.Duration
	liveState  []byte

	// Dynamic configuration - created just-in-time with proper namespace
	actionConfig *action.Configuration
	namespace    string // Set dynamically from first object

	// State for async operations (Apply → WaitForApply pattern)
	lastRelease             *release.Release
	lastResourceSet         impl.ResourceSet
	lastLiveObjects         []*unstructured.Unstructured
	pendingResources        kube.ResourceList // Resources waiting to be ready
	pendingDeletedResources kube.ResourceList // Non-namespace resources waiting to be fully deleted (Destroy → WaitForDestroy pattern)
	pendingNamespaces       kube.ResourceList // Namespace resources waiting to be deleted (Destroy → WaitForDestroy pattern)
	isInstallMode           bool              // True if last Apply was install
	isUpgradeMode           bool              // True if last Apply was upgrade
}

// HelmApplierConfig extends impl.ApplierConfig with Helm-specific fields
type HelmApplierConfig struct {
	impl.ApplierConfig
	UnitLabels map[string]string // Unit labels containing Helm metadata
}

// NewHelmApplier creates a new Helm applier
func NewHelmApplier(config HelmApplierConfig) (*HelmApplier, error) {
	// Parse timeout
	timeout := impl.DefaultTimeout
	if config.WaitTimeout != "" {
		if t, err := time.ParseDuration(config.WaitTimeout); err == nil {
			timeout = t
		}
	}

	// Initialize Helm settings
	settings := cli.New()

	// Action configuration will be initialized just-in-time in Apply/Destroy
	// when we have the actual namespace from the first object

	return &HelmApplier{
		settings:   settings,
		spaceID:    config.SpaceID,
		unitSlug:   config.UnitSlug,
		unitLabels: config.UnitLabels,
		timeout:    timeout,
		liveState:  config.LiveState,
		// actionConfig and namespace will be set dynamically
	}, nil
}

// extractNamespaceFromObjects detects the namespace from the first object
// Priority: 1. First Namespace object, 2. First object's namespace, 3. "default"
func (a *HelmApplier) extractNamespaceFromObjects(objects []*unstructured.Unstructured) string {
	if len(objects) == 0 {
		return "default"
	}

	// First priority: Check if the first object is a Namespace
	for _, obj := range objects {
		if obj.GetKind() == "Namespace" && obj.GetAPIVersion() == "v1" {
			ns := obj.GetName()
			if ns != "" {
				log.Log.V(1).Info("📍 Detected namespace from Namespace object", "namespace", ns)
				return ns
			}
		}
	}

	// Second priority: Use the namespace from the first namespaced resource
	for _, obj := range objects {
		ns := obj.GetNamespace()
		if ns != "" {
			log.Log.V(1).Info("📍 Detected namespace from object", "namespace", ns, "kind", obj.GetKind(), "name", obj.GetName())
			return ns
		}
	}

	// Default fallback
	log.Log.V(1).Info("📍 No namespace detected, using default")
	return "default"
}

// initializeActionConfig initializes the Helm action configuration with the given namespace
// This is called just-in-time when needed, ensuring we use the correct namespace
func (a *HelmApplier) initializeActionConfig(namespace string) error {
	// Only initialize if namespace changed or not initialized yet
	if a.actionConfig != nil && a.namespace == namespace {
		log.Log.V(1).Info("✅ Action configuration already initialized", "namespace", namespace)
		return nil // Already initialized with same namespace
	}

	log.Log.Info("🔧 Initializing Helm action configuration", "namespace", namespace, "unitSlug", a.unitSlug)

	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(
		a.settings.RESTClientGetter(),
		namespace,
		"secret", // Use Kubernetes Secrets as storage backend
		func(format string, v ...interface{}) {
			log.Log.Info(fmt.Sprintf(format, v...))
		},
	); err != nil {
		return fmt.Errorf("failed to initialize Helm action configuration: %w", err)
	}

	a.actionConfig = actionConfig
	a.namespace = namespace
	return nil
}

// Apply implements K8sApplier.Apply for Helm install/upgrade
func (a *HelmApplier) Apply(ctx context.Context, objects []*unstructured.Unstructured) impl.ApplyResult {
	log.Log.Info("🎯 Starting Helm apply operation", "unitSlug", a.unitSlug, "objectCount", len(objects))

	// Extract namespace from objects and initialize action configuration just-in-time
	namespace := a.extractNamespaceFromObjects(objects)
	if err := a.initializeActionConfig(namespace); err != nil {
		return impl.ApplyResult{Error: fmt.Errorf("failed to initialize Helm configuration: %w", err)}
	}

	// Extract Helm metadata from unit labels (set by cub helm install)
	helmMeta, err := a.extractHelmMetadata()
	if err != nil {
		return impl.ApplyResult{Error: fmt.Errorf("failed to extract Helm metadata: %w", err)}
	}

	// Convert objects to manifest string
	manifest, err := objectsToManifestString(objects)
	if err != nil {
		return impl.ApplyResult{Error: fmt.Errorf("failed to convert objects to manifest: %w", err)}
	}

	// Split manifests to separate hooks from regular resources
	manifestMap := releaseutil.SplitManifests(manifest)
	hooks, regularManifests, err := releaseutil.SortManifests(
		manifestMap,
		nil,
		releaseutil.InstallOrder,
	)
	if err != nil {
		return impl.ApplyResult{Error: fmt.Errorf("failed to sort manifests: %w", err)}
	}

	log.Log.Info("📋 Parsed manifests",
		"totalManifests", len(manifestMap),
		"hooks", len(hooks),
		"resources", len(regularManifests),
		"unitSlug", a.unitSlug)

	// Separate namespaces from other resources
	namespaceManifests, otherManifests := a.separateNamespaces(regularManifests)
	log.Log.V(1).Info("📋 Separated resources", "namespaces", len(namespaceManifests), "others", len(otherManifests))

	// Build full manifest including namespaces (for GC tracking)
	fullManifest := a.buildFullManifest(regularManifests)

	// Create chart metadata
	chrt := &chart.Chart{
		Metadata: &chart.Metadata{
			Name:       helmMeta.ChartName,
			APIVersion: helmMeta.ChartAPIVersion,
			Version:    helmMeta.ChartVersion,
			AppVersion: helmMeta.AppVersion,
		},
	}

	// Check if release exists (upgrade vs install mode)
	history, err := a.actionConfig.Releases.History(helmMeta.ReleaseName)
	if err == nil && len(history) > 0 {
		// UPGRADE MODE
		releaseutil.SortByRevision(history)
		currentRelease := history[len(history)-1]
		log.Log.Info("📦 Release exists, performing upgrade",
			"release", helmMeta.ReleaseName,
			"currentVersion", currentRelease.Version,
			"status", currentRelease.Info.Status,
			"unitSlug", a.unitSlug)
		return a.performUpgrade(ctx, helmMeta.ReleaseName, chrt, fullManifest, hooks, namespaceManifests, otherManifests, history)
	}

	// INSTALL MODE
	log.Log.Info("🆕 Release not found, performing install",
		"release", helmMeta.ReleaseName,
		"namespace", namespace,
		"unitSlug", a.unitSlug)
	return a.performInstall(ctx, helmMeta.ReleaseName, chrt, fullManifest, hooks, namespaceManifests, otherManifests)
}

// WaitForApply implements K8sApplier.WaitForApply
func (a *HelmApplier) WaitForApply(ctx context.Context, objects []*unstructured.Unstructured, timeout time.Duration) impl.WaitResult {
	log.Log.Info("⏳ Waiting for Helm apply to complete", "unitSlug", a.unitSlug, "timeout", timeout)

	// Ensure action configuration is initialized (should already be from Apply call)
	if a.actionConfig == nil {
		namespace := a.extractNamespaceFromObjects(objects)
		if err := a.initializeActionConfig(namespace); err != nil {
			return impl.WaitResult{Error: fmt.Errorf("failed to initialize Helm configuration: %w", err)}
		}
	}

	// If lastRelease is nil, try to retrieve it from Helm storage
	// This handles the case where WaitForApply is called on a fresh applier instance
	if a.lastRelease == nil {
		helmMeta, err := a.extractHelmMetadata()
		if err != nil {
			return impl.WaitResult{Error: fmt.Errorf("failed to extract Helm metadata: %w", err)}
		}

		// Try to get the release from Helm storage
		history, err := a.actionConfig.Releases.History(helmMeta.ReleaseName)
		if err != nil || len(history) == 0 {
			return impl.WaitResult{Error: fmt.Errorf("no release found in storage - Apply must be called first: %w", err)}
		}

		// Get the latest release
		releaseutil.SortByRevision(history)
		a.lastRelease = history[len(history)-1]

		// Determine mode based on release status
		if a.lastRelease.Info.Status == release.StatusPendingInstall {
			a.isInstallMode = true
			a.isUpgradeMode = false
		} else if a.lastRelease.Info.Status == release.StatusPendingUpgrade {
			a.isInstallMode = false
			a.isUpgradeMode = true
		} else {
			// Release already deployed/failed - just refresh and return current state
			log.Log.Info("⚠️ Release already in final state", "status", a.lastRelease.Info.Status)
			liveObjects, _ := a.Refresh(ctx, objects)
			return impl.WaitResult{
				LiveObjects: liveObjects,
				ResourceSet: impl.NewSimpleResourceSet(),
				Error:       nil,
			}
		}

		// Rebuild pending resources from manifest
		resources, err := a.actionConfig.KubeClient.Build(bytes.NewBufferString(a.lastRelease.Manifest), false)
		if err != nil {
			return impl.WaitResult{Error: fmt.Errorf("failed to rebuild resources from manifest: %w", err)}
		}
		a.pendingResources = resources

		// Rebuild resource set based on operation mode
		if a.isInstallMode {
			a.lastResourceSet = a.buildResourceSet(resources, "Creating")
		} else if a.isUpgradeMode {
			// For upgrade mode, we can't determine which resources were created vs updated
			// from just the manifest, so mark them all as "Updating" for now
			a.lastResourceSet = a.buildResourceSet(resources, "Updating")
		}
	}

	// Use provided timeout or fallback to applier timeout
	waitTimeout := a.timeout
	if timeout > 0 {
		waitTimeout = timeout
	}

	// Determine mode and call appropriate wait function
	if a.isInstallMode {
		return a.waitForInstall(ctx, waitTimeout)
	} else if a.isUpgradeMode {
		return a.waitForUpgrade(ctx, waitTimeout)
	}

	return impl.WaitResult{Error: fmt.Errorf("unknown operation mode")}
}

// waitForInstall waits for install to complete and executes post-install hooks
func (a *HelmApplier) waitForInstall(ctx context.Context, timeout time.Duration) impl.WaitResult {
	log.Log.Info("⏳ Waiting for install to complete",
		"release", a.lastRelease.Name,
		"version", a.lastRelease.Version,
		"timeout", timeout)

	rel := a.lastRelease

	// Wait for resources to be ready
	if len(a.pendingResources) > 0 {
		log.Log.Info("⏳ Waiting for resources to be ready", "count", len(a.pendingResources), "timeout", timeout)
		if err := a.actionConfig.KubeClient.Wait(a.pendingResources, timeout); err != nil {
			log.Log.Error(err, "❌ Resources failed to become ready", "count", len(a.pendingResources))
			rel.SetStatus(release.StatusFailed, fmt.Sprintf("timed out: %s", err))
			a.actionConfig.Releases.Update(rel)
			return impl.WaitResult{Error: fmt.Errorf("timeout waiting for resources: %w", err)}
		}
		log.Log.V(1).Info("✅ All resources ready", "count", len(a.pendingResources))
	}

	// Execute post-install hooks
	if len(rel.Hooks) > 0 {
		log.Log.Info("🪝 Executing post-install hooks", "count", len(rel.Hooks))
		if err := a.execHook(rel, release.HookPostInstall, timeout); err != nil {
			log.Log.Error(err, "❌ Post-install hooks failed")
			rel.SetStatus(release.StatusFailed, fmt.Sprintf("failed post-install: %s", err))
			a.actionConfig.Releases.Update(rel)
			return impl.WaitResult{Error: fmt.Errorf("post-install hooks failed: %w", err)}
		}
	}

	// Mark as deployed
	rel.SetStatus(release.StatusDeployed, "Install complete")
	a.actionConfig.Releases.Update(rel)

	// Get live objects
	liveObjects, _ := a.getLiveObjectsFromResources(ctx, a.pendingResources)
	a.lastLiveObjects = liveObjects

	// Update resource set with final status
	a.lastResourceSet = a.buildResourceSet(a.pendingResources, "Created")

	log.Log.Info("✅ Install complete", "version", rel.Version, "liveObjects", len(liveObjects))

	return impl.WaitResult{
		LiveObjects: liveObjects,
		ResourceSet: a.lastResourceSet,
		Error:       nil,
	}
}

// waitForUpgrade waits for upgrade to complete and executes post-upgrade hooks
func (a *HelmApplier) waitForUpgrade(ctx context.Context, timeout time.Duration) impl.WaitResult {
	log.Log.Info("⏳ Waiting for upgrade to complete",
		"release", a.lastRelease.Name,
		"version", a.lastRelease.Version,
		"timeout", timeout)

	rel := a.lastRelease

	// Wait for created/updated resources to be ready
	if len(a.pendingResources) > 0 {
		log.Log.Info("⏳ Waiting for resources to be ready", "count", len(a.pendingResources), "timeout", timeout)
		if err := a.actionConfig.KubeClient.Wait(a.pendingResources, timeout); err != nil {
			log.Log.Error(err, "❌ Resources failed to become ready", "count", len(a.pendingResources))
			rel.SetStatus(release.StatusFailed, fmt.Sprintf("timeout: %s", err))
			a.actionConfig.Releases.Update(rel)
			return impl.WaitResult{Error: fmt.Errorf("timeout waiting for resources: %w", err)}
		}
		log.Log.V(1).Info("✅ All resources ready", "count", len(a.pendingResources))
	}

	// Execute post-upgrade hooks
	if len(rel.Hooks) > 0 {
		log.Log.Info("🪝 Executing post-upgrade hooks", "count", len(rel.Hooks))
		if err := a.execHook(rel, release.HookPostUpgrade, timeout); err != nil {
			log.Log.Error(err, "❌ Post-upgrade hooks failed")
			rel.SetStatus(release.StatusFailed, fmt.Sprintf("failed post-upgrade: %s", err))
			a.actionConfig.Releases.Update(rel)
			return impl.WaitResult{Error: fmt.Errorf("post-upgrade hooks failed: %w", err)}
		}
	}

	// Mark as deployed
	rel.SetStatus(release.StatusDeployed, "Upgrade complete")
	a.actionConfig.Releases.Update(rel)

	// Get live objects
	liveObjects, _ := a.getLiveObjectsFromResources(ctx, a.pendingResources)
	a.lastLiveObjects = liveObjects

	// Update resource set with final status (Created → Created, Updating → Updated)
	finalChangeSet := impl.NewSimpleResourceSet()
	for _, entry := range a.lastResourceSet.GetEntries() {
		if simpleEntry, ok := entry.(impl.SimpleResourceSetEntry); ok {
			finalAction := simpleEntry.Action
			if finalAction == "Creating" {
				finalAction = "Created"
			} else if finalAction == "Updating" {
				finalAction = "Updated"
			}
			finalChangeSet.Add(impl.SimpleResourceSetEntry{
				Name:      simpleEntry.Name,
				Namespace: simpleEntry.Namespace,
				Kind:      simpleEntry.Kind,
				Action:    finalAction,
			})
		}
	}
	a.lastResourceSet = finalChangeSet

	log.Log.Info("✅ Upgrade complete", "version", rel.Version, "liveObjects", len(liveObjects))

	return impl.WaitResult{
		LiveObjects: liveObjects,
		ResourceSet: a.lastResourceSet,
		Error:       nil,
	}
}

// Refresh implements K8sApplier.Refresh
func (a *HelmApplier) Refresh(ctx context.Context, objects []*unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	log.Log.Info("🔄 Refreshing live objects from cluster", "objectCount", len(objects), "unitSlug", a.unitSlug)

	// Extract namespace from objects and initialize action configuration just-in-time
	namespace := a.extractNamespaceFromObjects(objects)
	if err := a.initializeActionConfig(namespace); err != nil {
		return nil, fmt.Errorf("failed to initialize Helm configuration: %w", err)
	}

	// Get live objects from cluster using Helm's KubeClient
	liveObjects := make([]*unstructured.Unstructured, 0, len(objects))

	for _, obj := range objects {
		// Convert object to YAML
		yamlStr, err := objectToYAML(obj)
		if err != nil {
			log.Log.Error(err, "⚠️ Failed to convert object to YAML, skipping", "kind", obj.GetKind(), "name", obj.GetName())
			continue
		}

		// Build a single resource to get it from cluster
		resources, err := a.actionConfig.KubeClient.Build(
			bytes.NewBufferString(yamlStr),
			false,
		)
		if err != nil {
			log.Log.V(1).Info("⚠️ Failed to build resource, skipping", "kind", obj.GetKind(), "name", obj.GetName(), "error", err)
			continue
		}

		if len(resources) > 0 {
			// Convert back to unstructured
			if liveObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resources[0].Object); err == nil {
				u := &unstructured.Unstructured{Object: liveObj}
				liveObjects = append(liveObjects, u)
			}
		}
	}

	log.Log.V(1).Info("✅ Refresh complete", "liveObjectCount", len(liveObjects))
	return liveObjects, nil
}

// Destroy implements K8sApplier.Destroy for Helm uninstall
func (a *HelmApplier) Destroy(ctx context.Context, objects []*unstructured.Unstructured) impl.DestroyResult {
	log.Log.Info("🗑️ Starting Helm destroy operation", "unitSlug", a.unitSlug, "objectCount", len(objects))

	// Extract namespace from objects and initialize action configuration just-in-time
	namespace := a.extractNamespaceFromObjects(objects)
	if err := a.initializeActionConfig(namespace); err != nil {
		return impl.DestroyResult{Error: fmt.Errorf("failed to initialize Helm configuration: %w", err)}
	}

	// Extract Helm metadata from unit labels
	helmMeta, err := a.extractHelmMetadata()
	if err != nil {
		return impl.DestroyResult{Error: fmt.Errorf("failed to extract Helm metadata: %w", err)}
	}

	// Get release history
	rels, err := a.actionConfig.Releases.History(helmMeta.ReleaseName)
	if err != nil {
		log.Log.Error(err, "❌ Release not found", "release", helmMeta.ReleaseName)
		return impl.DestroyResult{Error: fmt.Errorf("release not found: %s: %w", helmMeta.ReleaseName, err)}
	}
	if len(rels) < 1 {
		log.Log.Error(nil, "❌ Release history empty", "release", helmMeta.ReleaseName)
		return impl.DestroyResult{Error: fmt.Errorf("release %s not found", helmMeta.ReleaseName)}
	}

	// Get latest release
	releaseutil.SortByRevision(rels)
	rel := rels[len(rels)-1]

	// Check if already uninstalled
	if rel.Info.Status == release.StatusUninstalled {
		log.Log.Info("⚠️ Release already uninstalled", "release", helmMeta.ReleaseName)
		return impl.DestroyResult{Error: fmt.Errorf("release %s already uninstalled", helmMeta.ReleaseName)}
	}

	log.Log.Info("📦 Found release for deletion",
		"release", helmMeta.ReleaseName,
		"version", rel.Version,
		"status", rel.Info.Status,
		"namespace", rel.Namespace)

	// Mark as uninstalling
	rel.Info.Status = release.StatusUninstalling
	rel.Info.Deleted = helmtime.Now()
	rel.Info.Description = "Deletion in progress"

	// Execute pre-delete hooks
	if len(rel.Hooks) > 0 {
		log.Log.Info("🪝 Executing pre-delete hooks", "count", len(rel.Hooks))
		if err := a.execHook(rel, release.HookPreDelete, a.timeout); err != nil {
			log.Log.Error(err, "❌ Pre-delete hooks failed")
			return impl.DestroyResult{Error: fmt.Errorf("pre-delete hooks failed: %w", err)}
		}
	}

	// Delete non-namespace resources (using optimized pattern from helm-uninstall)
	log.Log.Info("🗑️ Deleting release resources")
	deletedResources, namespaceResources, kept, errs := a.deleteReleaseOptimized(rel, "background")
	if errs != nil {
		log.Log.Error(fmt.Errorf("%v", errs), "❌ Failed to delete resources")
		return impl.DestroyResult{Error: fmt.Errorf("failed to delete resources: %v", errs)}
	}

	if kept != "" {
		log.Log.Info("📌 Resources kept due to policy", "kept", kept)
	}
	log.Log.V(1).Info("✅ Resources deleted", "deletedCount", len(deletedResources), "namespacesDeferred", len(namespaceResources))

	// Build ResourceSet (include both deleted resources and pending namespaces)
	simpleResourceSet := impl.NewSimpleResourceSet()
	for _, info := range deletedResources {
		simpleResourceSet.Add(impl.SimpleResourceSetEntry{
			Name:      info.Name,
			Namespace: info.Namespace,
			Kind:      info.Mapping.GroupVersionKind.Kind,
			Action:    "Deleted",
		})
	}
	// Add namespace resources with "Deleting" status (will be deleted in WaitForDestroy)
	for _, info := range namespaceResources {
		simpleResourceSet.Add(impl.SimpleResourceSetEntry{
			Name:      info.Name,
			Namespace: info.Namespace,
			Kind:      info.Mapping.GroupVersionKind.Kind,
			Action:    "Deleting", // Will be updated to "Deleted" in WaitForDestroy
		})
	}

	// Store deleted resources and namespace resources for WaitForDestroy (store ResourceList directly to avoid conversion bugs)
	a.lastResourceSet = simpleResourceSet
	a.pendingDeletedResources = deletedResources // Non-namespace resources to wait for deletion
	a.pendingNamespaces = namespaceResources     // Store the built ResourceList directly - no conversion needed!

	log.Log.Info("✅ Destroy initiated (namespace deletion deferred to WaitForDestroy)",
		"release", helmMeta.ReleaseName,
		"deletedCount", len(deletedResources),
		"namespacesDeferred", len(namespaceResources))

	return impl.DestroyResult{
		ResourceSet: simpleResourceSet,
		LiveState:   nil,
		Error:       nil,
	}
}

// WaitForDestroy implements K8sApplier.WaitForDestroy
func (a *HelmApplier) WaitForDestroy(ctx context.Context, objects []*unstructured.Unstructured, timeout time.Duration) impl.WaitResult {
	log.Log.Info("⏳ Waiting for Helm destroy to complete", "unitSlug", a.unitSlug, "timeout", timeout)

	// Ensure action configuration is initialized (should already be from Destroy call)
	if a.actionConfig == nil {
		namespace := a.extractNamespaceFromObjects(objects)
		if err := a.initializeActionConfig(namespace); err != nil {
			return impl.WaitResult{Error: fmt.Errorf("failed to initialize Helm configuration: %w", err)}
		}
	}

	// Extract Helm metadata from unit labels
	helmMeta, err := a.extractHelmMetadata()
	if err != nil {
		return impl.WaitResult{Error: fmt.Errorf("failed to extract Helm metadata: %w", err)}
	}

	// Get release history (may have been purged already, that's OK)
	rels, err := a.actionConfig.Releases.History(helmMeta.ReleaseName)
	var rel *release.Release
	if err == nil && len(rels) > 0 {
		releaseutil.SortByRevision(rels)
		rel = rels[len(rels)-1]
	}

	// Execute post-delete hooks if release still exists
	if rel != nil {
		if len(rel.Hooks) > 0 {
			log.Log.Info("🪝 Executing post-delete hooks", "count", len(rel.Hooks))
			if err := a.execHook(rel, release.HookPostDelete, timeout); err != nil {
				log.Log.Error(err, "❌ Post-delete hooks failed")
			}
		}

		// Mark release as uninstalled
		rel.Info.Status = release.StatusUninstalled
		rel.Info.Description = "Uninstallation complete"

		// Purge release history (safe now - non-namespace resources deleted)
		log.Log.Info("🗑️ Purging release history", "release", rel.Name, "versions", len(rels))
		if err := a.purgeReleases(rels...); err != nil {
			log.Log.Error(err, "❌ Failed to purge release", "release", rel.Name)
		} else {
			log.Log.V(1).Info("✅ Release history purged", "release", rel.Name)
		}
	}

	// Wait for non-namespace resources to be fully deleted before deleting namespaces
	// This is critical: Kubernetes won't delete a namespace until ALL resources inside it are gone
	if len(a.pendingDeletedResources) > 0 {
		log.Log.Info("⏳ Waiting for non-namespace resources to be fully deleted", "count", len(a.pendingDeletedResources), "timeout", timeout)
		if kubeClient, ok := a.actionConfig.KubeClient.(kube.InterfaceExt); ok {
			if err := kubeClient.WaitForDelete(a.pendingDeletedResources, timeout); err != nil {
				log.Log.Error(err, "❌ Error waiting for non-namespace resource deletion")
				return impl.WaitResult{Error: fmt.Errorf("timeout waiting for non-namespace resource deletion: %w", err)}
			}
			log.Log.V(1).Info("✅ Non-namespace resources fully deleted")
		}
	}

	// Handle case where WaitForDestroy is called on a fresh applier instance
	// (state was lost between Destroy and WaitForDestroy calls)
	// In this case, pendingNamespaces will be empty, so we need to rebuild it from input objects
	if len(a.pendingNamespaces) == 0 && a.lastResourceSet == nil {
		log.Log.Info("⚠️ Fresh applier instance detected, extracting namespaces from input objects")
		// Extract namespace resources from input objects
		namespaceObjs := []*unstructured.Unstructured{}
		for _, obj := range objects {
			if obj.GetKind() == "Namespace" {
				namespaceObjs = append(namespaceObjs, obj)
			}
		}

		if len(namespaceObjs) > 0 {
			// Build ResourceList for namespace deletion
			yamlStr, yamlErr := objectsToManifestString(namespaceObjs)
			if yamlErr != nil {
				log.Log.Error(yamlErr, "❌ Failed to convert namespace objects to YAML")
				return impl.WaitResult{Error: fmt.Errorf("failed to convert namespace objects to YAML: %w", yamlErr)}
			}

			var buildErr error
			a.pendingNamespaces, buildErr = a.actionConfig.KubeClient.Build(
				strings.NewReader(yamlStr),
				false,
			)
			if buildErr != nil {
				log.Log.Error(buildErr, "❌ Failed to build namespace ResourceList from objects")
				return impl.WaitResult{Error: fmt.Errorf("failed to build namespace ResourceList: %w", buildErr)}
			}
			log.Log.Info("✅ Rebuilt namespace ResourceList from input objects", "count", len(a.pendingNamespaces))
		}
	}

	// Delete namespaces last (stored in pendingNamespaces from Destroy)
	if len(a.pendingNamespaces) > 0 {
		log.Log.Info("🗑️ Deleting namespaces", "count", len(a.pendingNamespaces))

		// Delete namespaces using FOREGROUND propagation to ensure all contained resources are deleted first
		// This prevents namespaces from getting stuck in "Terminating" state due to pending resources
		var deleteErrs []error
		if kubeClient, ok := a.actionConfig.KubeClient.(kube.InterfaceDeletionPropagation); ok {
			_, deleteErrs = kubeClient.DeleteWithPropagationPolicy(a.pendingNamespaces, metav1.DeletePropagationForeground)
		} else {
			_, deleteErrs = a.actionConfig.KubeClient.Delete(a.pendingNamespaces)
		}

		// Check for deletion errors
		if len(deleteErrs) > 0 {
			log.Log.Error(fmt.Errorf("%v", deleteErrs), "❌ Failed to delete namespaces", "errorCount", len(deleteErrs))
			return impl.WaitResult{Error: fmt.Errorf("failed to delete namespaces: %v", deleteErrs)}
		}

		// Wait for namespace deletion
		if kubeClient, ok := a.actionConfig.KubeClient.(kube.InterfaceExt); ok {
			log.Log.Info("⏳ Waiting for namespace deletion", "timeout", timeout)
			if err := kubeClient.WaitForDelete(a.pendingNamespaces, timeout); err != nil {
				log.Log.Error(err, "❌ Error waiting for namespace deletion")
				return impl.WaitResult{Error: fmt.Errorf("timeout waiting for namespace deletion: %w", err)}
			}
			log.Log.V(1).Info("✅ Namespaces deleted successfully")
		}
	}

	// Update ResourceSet to mark namespaces as "Deleted" (they were "Deleting" in Destroy)
	finalResourceSet := impl.NewSimpleResourceSet()

	// Handle case where WaitForDestroy is called on a fresh applier instance
	// (state was lost between Destroy and WaitForDestroy calls)
	if a.lastResourceSet == nil {
		log.Log.V(1).Info("⚠️ lastResourceSet is nil, building from pendingNamespaces")
		// Build ResourceSet from pendingNamespaces
		for _, info := range a.pendingNamespaces {
			finalResourceSet.Add(impl.SimpleResourceSetEntry{
				Name:      info.Name,
				Namespace: info.Namespace,
				Kind:      info.Mapping.GroupVersionKind.Kind,
				Action:    "Deleted", // Namespace deletion complete
			})
		}
	} else {
		// Normal path: update existing ResourceSet
		for _, entry := range a.lastResourceSet.GetEntries() {
			if simpleEntry, ok := entry.(impl.SimpleResourceSetEntry); ok {
				finalAction := simpleEntry.Action
				if finalAction == "Deleting" {
					finalAction = "Deleted" // Namespace deletion complete
				}
				finalResourceSet.Add(impl.SimpleResourceSetEntry{
					Name:      simpleEntry.Name,
					Namespace: simpleEntry.Namespace,
					Kind:      simpleEntry.Kind,
					Action:    finalAction,
				})
			}
		}
	}

	log.Log.Info("✅ Destroy complete", "unitSlug", a.unitSlug)
	return impl.WaitResult{
		LiveObjects: []*unstructured.Unstructured{}, // All destroyed
		ResourceSet: finalResourceSet,
		Error:       nil,
	}
}

// performInstall handles fresh installation (version 1)
func (a *HelmApplier) performInstall(
	_ context.Context,
	releaseName string,
	chrt *chart.Chart,
	fullManifest string,
	hooks []*release.Hook,
	namespaceManifests []releaseutil.Manifest,
	otherManifests []releaseutil.Manifest,
) impl.ApplyResult {
	log.Log.Info("📦 Performing Helm install",
		"release", releaseName,
		"namespace", a.namespace,
		"chartVersion", chrt.Metadata.Version,
		"hooks", len(hooks),
		"namespaces", len(namespaceManifests),
		"resources", len(otherManifests))

	now := helmtime.Now()
	rel := &release.Release{
		Name:      releaseName,
		Namespace: a.namespace,
		Chart:     chrt,
		Config:    map[string]interface{}{},
		Version:   1,
		Manifest:  fullManifest,
		Hooks:     hooks,
		Info: &release.Info{
			FirstDeployed: now,
			LastDeployed:  now,
			Status:        release.StatusPendingInstall,
			Description:   "Initial install underway",
		},
	}

	// 1. Create namespaces first
	if len(namespaceManifests) > 0 {
		if err := a.createNamespaces(namespaceManifests); err != nil {
			log.Log.Error(err, "❌ Failed to create namespaces")
			return impl.ApplyResult{Error: fmt.Errorf("failed to create namespaces: %w", err)}
		}
	}

	// 2. Create release record in Helm storage (always, not just when hooks exist)
	log.Log.V(1).Info("📋 Creating release record", "version", rel.Version)
	if err := a.actionConfig.Releases.Create(rel); err != nil {
		log.Log.Error(err, "❌ Failed to create release record")
		return impl.ApplyResult{Error: fmt.Errorf("failed to create release record: %w", err)}
	}

	// 3. Execute pre-install hooks if any
	if len(hooks) > 0 {
		log.Log.Info("🪝 Executing pre-install hooks", "count", len(hooks))
		if err := a.execHook(rel, release.HookPreInstall, a.timeout); err != nil {
			log.Log.Error(err, "❌ Pre-install hooks failed")
			rel.SetStatus(release.StatusFailed, fmt.Sprintf("failed pre-install: %s", err))
			a.actionConfig.Releases.Update(rel)
			return impl.ApplyResult{Error: fmt.Errorf("pre-install hooks failed: %w", err)}
		}
	}

	// 4. Build and create regular resources
	otherManifestBuf := a.buildManifestBuffer(otherManifests)
	resources, err := a.actionConfig.KubeClient.Build(bytes.NewBufferString(otherManifestBuf), true)
	if err != nil {
		log.Log.Error(err, "❌ Failed to build kubernetes resources")
		return impl.ApplyResult{Error: fmt.Errorf("failed to build kubernetes resources: %w", err)}
	}

	// Set ownership metadata
	log.Log.V(1).Info("📋 Setting ownership metadata", "resourceCount", len(resources))
	if err := resources.Visit(a.setMetadataVisitor(rel.Name, rel.Namespace, true)); err != nil {
		log.Log.Error(err, "❌ Failed to set metadata")
		return impl.ApplyResult{Error: fmt.Errorf("failed to set metadata: %w", err)}
	}

	// Create resources
	log.Log.Info("📦 Installing resources", "count", len(resources))
	if _, err := a.actionConfig.KubeClient.Create(resources); err != nil {
		log.Log.Error(err, "❌ Failed to create resources")
		rel.SetStatus(release.StatusFailed, fmt.Sprintf("failed to create resources: %s", err))
		a.actionConfig.Releases.Update(rel)
		return impl.ApplyResult{Error: fmt.Errorf("failed to create resources: %w", err)}
	}
	log.Log.V(1).Info("✅ Resources created successfully", "count", len(resources))

	// Store state for WaitForApply (non-blocking pattern)
	a.lastRelease = rel
	a.pendingResources = resources
	a.isInstallMode = true
	a.isUpgradeMode = false
	a.lastResourceSet = a.buildResourceSet(resources, "Creating")

	log.Log.Info("✅ Install initiated (waiting deferred to WaitForApply)", "version", rel.Version)

	return impl.ApplyResult{
		ResourceSet: a.lastResourceSet,
		LiveObjects: nil, // Not ready yet - will be populated in WaitForApply
		LiveState:   nil,
		Error:       nil,
	}
}

// performUpgrade handles upgrading existing release with garbage collection
func (a *HelmApplier) performUpgrade(
	ctx context.Context,
	releaseName string,
	chrt *chart.Chart,
	fullManifest string,
	hooks []*release.Hook,
	namespaceManifests []releaseutil.Manifest,
	otherManifests []releaseutil.Manifest,
	history []*release.Release,
) impl.ApplyResult {
	// Get current release
	releaseutil.SortByRevision(history)
	currentRelease := history[len(history)-1]

	log.Log.Info("📦 Performing Helm upgrade",
		"release", releaseName,
		"fromVersion", currentRelease.Version,
		"toVersion", currentRelease.Version+1)

	// Create new release
	now := helmtime.Now()
	newRelease := &release.Release{
		Name:      releaseName,
		Namespace: a.namespace,
		Chart:     chrt,
		Config:    map[string]interface{}{},
		Version:   currentRelease.Version + 1,
		Manifest:  fullManifest,
		Hooks:     hooks,
		Info: &release.Info{
			FirstDeployed: currentRelease.Info.FirstDeployed,
			LastDeployed:  now,
			Status:        release.StatusPendingUpgrade,
			Description:   "Upgrade in progress",
		},
	}

	// Build current and target resources
	log.Log.V(1).Info("📋 Building current manifest", "version", currentRelease.Version)
	current, err := a.actionConfig.KubeClient.Build(bytes.NewBufferString(currentRelease.Manifest), false)
	if err != nil {
		log.Log.Error(err, "❌ Unable to build current manifest")
		return impl.ApplyResult{Error: fmt.Errorf("unable to build current manifest: %w", err)}
	}

	log.Log.V(1).Info("📋 Building target manifest", "version", newRelease.Version)
	target, err := a.actionConfig.KubeClient.Build(bytes.NewBufferString(fullManifest), true)
	if err != nil {
		log.Log.Error(err, "❌ Unable to build new manifest")
		return impl.ApplyResult{Error: fmt.Errorf("unable to build new manifest: %w", err)}
	}

	// Set ownership metadata
	log.Log.V(1).Info("📋 Setting ownership metadata", "resourceCount", len(target))
	if err := target.Visit(a.setMetadataVisitor(newRelease.Name, newRelease.Namespace, true)); err != nil {
		log.Log.Error(err, "❌ Failed to set metadata")
		return impl.ApplyResult{Error: fmt.Errorf("failed to set metadata: %w", err)}
	}

	// Store new release
	log.Log.V(1).Info("📋 Creating upgrade record", "version", newRelease.Version)
	if err := a.actionConfig.Releases.Create(newRelease); err != nil {
		log.Log.Error(err, "❌ Failed to create upgrade record")
		return impl.ApplyResult{Error: fmt.Errorf("failed to create upgrade record: %w", err)}
	}

	// Execute pre-upgrade hooks
	if len(hooks) > 0 {
		log.Log.Info("🪝 Executing pre-upgrade hooks", "count", len(hooks))
		if err := a.execHook(newRelease, release.HookPreUpgrade, a.timeout); err != nil {
			log.Log.Error(err, "❌ Pre-upgrade hooks failed")
			newRelease.SetStatus(release.StatusFailed, fmt.Sprintf("failed pre-upgrade: %s", err))
			a.actionConfig.Releases.Update(newRelease)
			return impl.ApplyResult{Error: fmt.Errorf("pre-upgrade hooks failed: %w", err)}
		}
	}

	// Perform update with garbage collection
	log.Log.Info("🔄 Updating resources with GC", "current", len(current), "target", len(target))
	result, err := a.actionConfig.KubeClient.Update(current, target, false)
	if err != nil {
		log.Log.Error(err, "❌ Failed to update resources")
		newRelease.SetStatus(release.StatusFailed, fmt.Sprintf("failed to update: %s", err))
		a.actionConfig.Releases.Update(newRelease)
		return impl.ApplyResult{Error: fmt.Errorf("failed to update resources: %w", err)}
	}

	// Log GC results
	log.Log.Info("📊 Update summary",
		"created", len(result.Created),
		"updated", len(result.Updated),
		"deleted", len(result.Deleted))

	// Store state for WaitForApply (non-blocking pattern)
	a.lastRelease = newRelease
	a.pendingResources = append(result.Created, result.Updated...)
	a.isInstallMode = false
	a.isUpgradeMode = true

	// Build ResourceSet including GC info
	changeSet := impl.NewSimpleResourceSet()
	for _, info := range result.Created {
		changeSet.Add(impl.SimpleResourceSetEntry{
			Name:      info.Name,
			Namespace: info.Namespace,
			Kind:      info.Mapping.GroupVersionKind.Kind,
			Action:    "Creating",
		})
	}
	for _, info := range result.Updated {
		changeSet.Add(impl.SimpleResourceSetEntry{
			Name:      info.Name,
			Namespace: info.Namespace,
			Kind:      info.Mapping.GroupVersionKind.Kind,
			Action:    "Updating",
		})
	}
	for _, info := range result.Deleted {
		changeSet.Add(impl.SimpleResourceSetEntry{
			Name:      info.Name,
			Namespace: info.Namespace,
			Kind:      info.Mapping.GroupVersionKind.Kind,
			Action:    "Deleted (GC)",
		})
	}

	a.lastResourceSet = changeSet

	log.Log.Info("✅ Upgrade initiated (waiting deferred to WaitForApply)", "version", newRelease.Version)

	return impl.ApplyResult{
		ResourceSet: changeSet,
		LiveObjects: nil, // Not ready yet - will be populated in WaitForApply
		LiveState:   nil,
		Error:       nil,
	}
}

// Helper functions adapted from helm-install and helm-uninstall

// extractHelmMetadata extracts Helm chart metadata from unit labels
// Unit labels are set by 'cub helm install' and contain authoritative Helm metadata
func (a *HelmApplier) extractHelmMetadata() (*helmutils.HelmMetadata, error) {
	meta := helmutils.ExtractHelmMetadata(a.unitLabels, a.unitSlug)

	log.Log.Info("📋 Extracted Helm metadata from unit labels",
		"release", meta.ReleaseName,
		"chart", meta.ChartName,
		"version", meta.ChartVersion,
		"appVersion", meta.AppVersion)

	return meta, nil
}

func (a *HelmApplier) separateNamespaces(manifests []releaseutil.Manifest) (namespaces, others []releaseutil.Manifest) {
	for _, m := range manifests {
		if m.Head != nil && m.Head.Kind == "Namespace" {
			namespaces = append(namespaces, m)
		} else {
			others = append(others, m)
		}
	}
	return namespaces, others
}

func (a *HelmApplier) buildFullManifest(manifests []releaseutil.Manifest) string {
	var buf bytes.Buffer
	for i, m := range manifests {
		if i > 0 {
			buf.WriteString("\n---\n")
		}
		buf.WriteString(m.Content)
	}
	return buf.String()
}

func (a *HelmApplier) buildManifestBuffer(manifests []releaseutil.Manifest) string {
	var buf bytes.Buffer
	for i, m := range manifests {
		if i > 0 {
			buf.WriteString("\n---\n")
		}
		buf.WriteString(m.Content)
	}
	return buf.String()
}

func (a *HelmApplier) createNamespaces(namespaceManifests []releaseutil.Manifest) error {
	if len(namespaceManifests) == 0 {
		return nil
	}

	log.Log.Info("🏗️ Creating namespaces", "count", len(namespaceManifests))

	var nsBuf bytes.Buffer
	for i, m := range namespaceManifests {
		if i > 0 {
			nsBuf.WriteString("\n---\n")
		}
		nsBuf.WriteString(m.Content)
	}

	nsResources, err := a.actionConfig.KubeClient.Build(bytes.NewBufferString(nsBuf.String()), true)
	if err != nil {
		return fmt.Errorf("failed to build namespace resources: %w", err)
	}

	// Create namespaces (ignore if already exist)
	if _, err := a.actionConfig.KubeClient.Create(nsResources); err != nil {
		// Check if it's AlreadyExists error
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create namespaces: %w", err)
		}
	}

	return nil
}

// deleteReleaseOptimized deletes non-namespace resources and returns namespace resources
func (a *HelmApplier) deleteReleaseOptimized(
	rel *release.Release,
	deletionPropagation string,
) (deletedResources kube.ResourceList, namespaceResources kube.ResourceList, kept string, errs []error) {
	// Split manifests
	manifests := releaseutil.SplitManifests(rel.Manifest)
	_, sortedManifests, err := releaseutil.SortManifests(manifests, nil, releaseutil.UninstallOrder)
	if err != nil {
		return nil, nil, rel.Manifest, []error{fmt.Errorf("corrupted release record: %w", err)}
	}

	// Filter resources with helm.sh/resource-policy: keep
	manifestsToKeep, manifestsToDelete := a.filterManifestsToKeep(sortedManifests)

	// Build kept info
	var keptBuilder strings.Builder
	for _, f := range manifestsToKeep {
		keptBuilder.WriteString(fmt.Sprintf("[%s] %s\n", f.Head.Kind, f.Head.Metadata.Name))
	}
	kept = keptBuilder.String()

	// Separate namespaces from other resources
	var namespaceManifests []releaseutil.Manifest
	var nonNamespaceManifests []releaseutil.Manifest
	for _, manifest := range manifestsToDelete {
		if manifest.Head != nil && manifest.Head.Kind == "Namespace" {
			namespaceManifests = append(namespaceManifests, manifest)
		} else {
			nonNamespaceManifests = append(nonNamespaceManifests, manifest)
		}
	}

	if len(namespaceManifests) > 0 {
		log.Log.Info("📋 Found namespaces - deferring deletion", "count", len(namespaceManifests))
	}

	// Delete non-namespace resources first
	if len(nonNamespaceManifests) > 0 {
		log.Log.Info("🗑️ Preparing to delete resources (excluding namespaces)", "count", len(nonNamespaceManifests))
		var regularBuilder strings.Builder
		for i, manifest := range nonNamespaceManifests {
			if i > 0 {
				regularBuilder.WriteString("\n---\n")
			}
			regularBuilder.WriteString(manifest.Content)
		}

		resources, err := a.actionConfig.KubeClient.Build(strings.NewReader(regularBuilder.String()), false)
		if err != nil {
			log.Log.Error(err, "❌ Unable to build resources for deletion")
			return nil, nil, kept, []error{fmt.Errorf("unable to build resources: %w", err)}
		}

		log.Log.Info("🗑️ Deleting resources (excluding namespaces)", "count", len(resources), "policy", deletionPropagation)

		policy := a.parseDeletionPropagation(deletionPropagation)
		if kubeClient, ok := a.actionConfig.KubeClient.(kube.InterfaceDeletionPropagation); ok {
			_, errs = kubeClient.DeleteWithPropagationPolicy(resources, policy)
		} else {
			_, errs = a.actionConfig.KubeClient.Delete(resources)
		}

		if len(errs) > 0 {
			log.Log.Error(fmt.Errorf("%v", errs), "⚠️ Some resources failed to delete", "errorCount", len(errs))
		} else {
			log.Log.V(1).Info("✅ Resources deleted successfully", "count", len(resources))
		}

		deletedResources = resources
	}

	// Build namespace resources for later deletion
	if len(namespaceManifests) > 0 {
		var nsBuilder strings.Builder
		for i, manifest := range namespaceManifests {
			if i > 0 {
				nsBuilder.WriteString("\n---\n")
			}
			nsBuilder.WriteString(manifest.Content)
		}

		nsResources, err := a.actionConfig.KubeClient.Build(strings.NewReader(nsBuilder.String()), false)
		if err != nil {
			log.Log.Error(err, "❌ Unable to build namespace resources for deletion")
			return nil, nil, kept, []error{fmt.Errorf("unable to build namespace resources: %w", err)}
		}
		namespaceResources = nsResources
	}

	return deletedResources, namespaceResources, kept, errs
}

// filterManifestsToKeep filters manifests with helm.sh/resource-policy: keep
func (a *HelmApplier) filterManifestsToKeep(manifests []releaseutil.Manifest) (keep, remaining []releaseutil.Manifest) {
	for _, m := range manifests {
		if m.Head.Metadata == nil || m.Head.Metadata.Annotations == nil {
			remaining = append(remaining, m)
			continue
		}

		resourcePolicy, ok := m.Head.Metadata.Annotations[kube.ResourcePolicyAnno]
		if !ok {
			remaining = append(remaining, m)
			continue
		}

		if strings.ToLower(strings.TrimSpace(resourcePolicy)) == kube.KeepPolicy {
			keep = append(keep, m)
		} else {
			remaining = append(remaining, m)
		}
	}
	return keep, remaining
}

// execHook executes hooks for the given event
func (a *HelmApplier) execHook(rl *release.Release, hookEvent release.HookEvent, timeout time.Duration) error {
	executingHooks := []*release.Hook{}

	for _, h := range rl.Hooks {
		for _, e := range h.Events {
			if e == hookEvent {
				executingHooks = append(executingHooks, h)
				break
			}
		}
	}

	if len(executingHooks) == 0 {
		log.Log.V(1).Info("No hooks to execute", "event", hookEvent)
		return nil
	}

	log.Log.Info("🪝 Executing hooks", "event", hookEvent, "count", len(executingHooks), "timeout", timeout)

	// Sort by weight
	a.sortHooksByWeight(executingHooks)

	for _, h := range executingHooks {
		if len(h.DeletePolicies) == 0 {
			h.DeletePolicies = []release.HookDeletePolicy{release.HookBeforeHookCreation}
		}

		if err := a.deleteHookByPolicy(h, release.HookBeforeHookCreation, timeout); err != nil {
			return err
		}

		resources, err := a.actionConfig.KubeClient.Build(bytes.NewBufferString(h.Manifest), true)
		if err != nil {
			log.Log.Error(err, "❌ Unable to build hook", "name", h.Name, "path", h.Path)
			return fmt.Errorf("unable to build hook %s: %w", h.Path, err)
		}

		log.Log.Info("🪝 Running hook", "name", h.Name, "kind", h.Kind, "weight", h.Weight)

		if _, err := a.actionConfig.KubeClient.Create(resources); err != nil {
			log.Log.Error(err, "❌ Failed to create hook", "name", h.Name)
			return fmt.Errorf("failed to create hook %s: %w", h.Path, err)
		}

		log.Log.V(1).Info("⏳ Waiting for hook to be ready", "name", h.Name, "timeout", timeout)
		if err := a.actionConfig.KubeClient.WatchUntilReady(resources, timeout); err != nil {
			log.Log.Error(err, "❌ Hook failed", "name", h.Name)
			a.deleteHookByPolicy(h, release.HookFailed, timeout)
			return fmt.Errorf("hook %s failed: %w", h.Path, err)
		}
		log.Log.V(1).Info("✅ Hook completed successfully", "name", h.Name)
	}

	// Delete successful hooks
	for i := len(executingHooks) - 1; i >= 0; i-- {
		if err := a.deleteHookByPolicy(executingHooks[i], release.HookSucceeded, timeout); err != nil {
			return err
		}
	}

	log.Log.V(1).Info("✅ All hooks executed successfully", "event", hookEvent, "count", len(executingHooks))
	return nil
}

func (a *HelmApplier) sortHooksByWeight(hooks []*release.Hook) {
	for i := 0; i < len(hooks); i++ {
		for j := i + 1; j < len(hooks); j++ {
			if hooks[i].Weight > hooks[j].Weight {
				hooks[i], hooks[j] = hooks[j], hooks[i]
			} else if hooks[i].Weight == hooks[j].Weight && hooks[i].Name > hooks[j].Name {
				hooks[i], hooks[j] = hooks[j], hooks[i]
			}
		}
	}
}

func (a *HelmApplier) deleteHookByPolicy(h *release.Hook, policy release.HookDeletePolicy, timeout time.Duration) error {
	if h.Kind == "CustomResourceDefinition" {
		return nil
	}

	if !a.hookHasDeletePolicy(h, policy) {
		return nil
	}

	resources, err := a.actionConfig.KubeClient.Build(bytes.NewBufferString(h.Manifest), false)
	if err != nil {
		return fmt.Errorf("unable to build hook for deletion: %w", err)
	}

	_, errs := a.actionConfig.KubeClient.Delete(resources)
	if len(errs) > 0 {
		return fmt.Errorf("failed to delete hook: %v", errs)
	}

	if kubeClient, ok := a.actionConfig.KubeClient.(kube.InterfaceExt); ok {
		kubeClient.WaitForDelete(resources, timeout)
	}

	return nil
}

func (a *HelmApplier) hookHasDeletePolicy(h *release.Hook, policy release.HookDeletePolicy) bool {
	for _, v := range h.DeletePolicies {
		if policy == v {
			return true
		}
	}
	return false
}

func (a *HelmApplier) setMetadataVisitor(releaseName, releaseNamespace string, force bool) resource.VisitorFunc {
	accessor := meta.NewAccessor()

	return func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}

		if err := a.mergeLabels(accessor, info.Object, map[string]string{
			"app.kubernetes.io/managed-by": "Helm",
		}); err != nil {
			return fmt.Errorf("failed to set labels: %w", err)
		}

		if err := a.mergeAnnotations(accessor, info.Object, map[string]string{
			"meta.helm.sh/release-name":      releaseName,
			"meta.helm.sh/release-namespace": releaseNamespace,
		}); err != nil {
			return fmt.Errorf("failed to set annotations: %w", err)
		}

		return nil
	}
}

func (a *HelmApplier) mergeLabels(accessor meta.MetadataAccessor, obj runtime.Object, labels map[string]string) error {
	current, err := accessor.Labels(obj)
	if err != nil {
		return err
	}
	return accessor.SetLabels(obj, mergeStrStrMaps(current, labels))
}

func (a *HelmApplier) mergeAnnotations(accessor meta.MetadataAccessor, obj runtime.Object, annotations map[string]string) error {
	current, err := accessor.Annotations(obj)
	if err != nil {
		return err
	}
	return accessor.SetAnnotations(obj, mergeStrStrMaps(current, annotations))
}

func mergeStrStrMaps(current, desired map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range current {
		result[k] = v
	}
	for k, v := range desired {
		result[k] = v
	}
	return result
}

func (a *HelmApplier) parseDeletionPropagation(cascadingFlag string) metav1.DeletionPropagation {
	switch cascadingFlag {
	case "orphan":
		return metav1.DeletePropagationOrphan
	case "foreground":
		return metav1.DeletePropagationForeground
	default:
		return metav1.DeletePropagationBackground
	}
}

func (a *HelmApplier) purgeReleases(rels ...*release.Release) error {
	for _, rel := range rels {
		if _, err := a.actionConfig.Releases.Delete(rel.Name, rel.Version); err != nil {
			return err
		}
	}
	return nil
}

func (a *HelmApplier) buildResourceSet(resources kube.ResourceList, action string) impl.ResourceSet {
	simpleSet := impl.NewSimpleResourceSet()
	for _, info := range resources {
		simpleSet.Add(impl.SimpleResourceSetEntry{
			Name:      info.Name,
			Namespace: info.Namespace,
			Kind:      info.Mapping.GroupVersionKind.Kind,
			Action:    action,
		})
	}
	return simpleSet
}

func (a *HelmApplier) getLiveObjectsFromResources(ctx context.Context, resources kube.ResourceList) ([]*unstructured.Unstructured, error) {
	liveObjects := make([]*unstructured.Unstructured, 0, len(resources))

	for _, info := range resources {
		// Refresh to get latest state
		if err := info.Get(); err == nil {
			if freshU, ok := info.Object.(*unstructured.Unstructured); ok {
				liveObjects = append(liveObjects, freshU)
			}
		}
	}

	return liveObjects, nil
}

// objectToYAML converts an unstructured object to YAML string
func objectToYAML(obj *unstructured.Unstructured) (string, error) {
	data, err := yaml.Marshal(obj.Object)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object to YAML: %w", err)
	}
	return string(data), nil
}

// objectsToManifestString converts objects to manifest string
func objectsToManifestString(objects []*unstructured.Unstructured) (string, error) {
	var buf bytes.Buffer
	for i, obj := range objects {
		if i > 0 {
			buf.WriteString("\n---\n")
		}
		data, err := yaml.Marshal(obj.Object)
		if err != nil {
			return "", err
		}
		buf.Write(data)
	}
	return buf.String(), nil
}
