// Copyright (C) ConfigHub, Inc.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/confighub/sdk/bridge-worker/impl"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CRDsApplier implements a simplified applier for CRDs using direct apply
type CRDsApplier struct {
	k8sClient  dynamic.Interface
	restMapper meta.RESTMapper
	timeout    time.Duration
}

// NewCRDsApplier creates a new CRDs applier
func NewCRDsApplier(timeout time.Duration) (*CRDsApplier, error) {
	// Get Kubernetes config
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		// Fall back to kubeconfig
		restConfig, err = GetRESTConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get kubernetes config: %w", err)
		}
	}

	// Create dynamic client
	k8sClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create discovery client and REST mapper
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create discovery client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	return &CRDsApplier{
		k8sClient:  k8sClient,
		restMapper: mapper,
		timeout:    timeout,
	}, nil
}

// Apply applies CRDs and waits for them to be ready
func (a *CRDsApplier) Apply(ctx context.Context, objects []*unstructured.Unstructured) error {
	if len(objects) == 0 {
		log.Log.Info("No CRDs to apply")
		return nil
	}

	log.Log.Info("🎯 Applying CRDs", "count", len(objects))

	// Known GVR for CRDs
	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	// Apply each CRD
	appliedCRDs := make([]*unstructured.Unstructured, 0)

	for _, obj := range objects {
		// Only process CRDs
		if obj.GetKind() != "CustomResourceDefinition" {
			continue
		}

		// Apply using server-side apply
		appliedObj, err := a.k8sClient.Resource(crdGVR).Apply(
			ctx,
			obj.GetName(),
			obj,
			metav1.ApplyOptions{
				FieldManager: "helm-bridge",
				Force:        true,
			},
		)

		if err != nil {
			log.Log.Error(err, "❌ Failed to apply CRD", "name", obj.GetName())
			return fmt.Errorf("failed to apply CRD %s: %w", obj.GetName(), err)
		}

		appliedCRDs = append(appliedCRDs, appliedObj)
		log.Log.V(1).Info("✅ Applied CRD", "name", obj.GetName())
	}

	if len(appliedCRDs) == 0 {
		log.Log.Info("No CRDs were applied")
		return nil
	}

	// Wait for CRDs to be ready
	log.Log.Info("⏳ Waiting for CRDs to be ready", "count", len(appliedCRDs), "timeout", a.timeout)

	// Create context with timeout
	waitCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	// Poll until all CRDs are established
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("timeout waiting for CRDs to be ready")
			}
			return waitCtx.Err()
		case <-ticker.C:
			allReady := true
			for _, obj := range appliedCRDs {
				// Get the current state
				current, err := a.k8sClient.Resource(crdGVR).Get(
					ctx,
					obj.GetName(),
					metav1.GetOptions{},
				)

				if err != nil {
					log.Log.Error(err, "Failed to get CRD status", "name", obj.GetName())
					allReady = false
					continue
				}

				// Check if CRD is established
				conditions, found, err := unstructured.NestedSlice(current.Object, "status", "conditions")
				if err != nil || !found {
					log.Log.V(2).Info("CRD conditions not found yet", "name", obj.GetName())
					allReady = false
					continue
				}

				established := false
				for _, c := range conditions {
					condition, ok := c.(map[string]interface{})
					if !ok {
						continue
					}
					if condition["type"] == "Established" && condition["status"] == "True" {
						established = true
						break
					}
				}

				if !established {
					log.Log.V(2).Info("CRD not yet established", "name", obj.GetName())
					allReady = false
				}
			}

			if allReady {
				log.Log.Info("✅ All CRDs are ready", "count", len(appliedCRDs))
				return nil
			}
		}
	}
}

// Destroy deletes CRDs
func (a *CRDsApplier) Destroy(ctx context.Context, objects []*unstructured.Unstructured) error {
	if len(objects) == 0 {
		log.Log.Info("No CRDs to destroy")
		return nil
	}

	log.Log.Info("🗑️ Destroying CRDs", "count", len(objects))

	// Known GVR for CRDs
	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	deletedCount := 0
	for _, obj := range objects {
		// Only process CRDs
		if obj.GetKind() != "CustomResourceDefinition" {
			continue
		}

		// Delete the CRD
		deletePolicy := metav1.DeletePropagationForeground
		err := a.k8sClient.Resource(crdGVR).Delete(
			ctx,
			obj.GetName(),
			metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			},
		)

		if err != nil && !errors.IsNotFound(err) {
			log.Log.Error(err, "❌ Failed to delete CRD", "name", obj.GetName())
			return fmt.Errorf("failed to delete CRD %s: %w", obj.GetName(), err)
		}

		if errors.IsNotFound(err) {
			log.Log.V(1).Info("CRD already deleted", "name", obj.GetName())
		} else {
			log.Log.V(1).Info("✅ Deleted CRD", "name", obj.GetName())
			deletedCount++
		}
	}

	if deletedCount > 0 {
		// Wait for CRDs to be fully deleted
		log.Log.Info("⏳ Waiting for CRDs to be deleted", "count", deletedCount, "timeout", a.timeout)

		// Create context with timeout
		waitCtx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		// Poll until all CRDs are gone
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-waitCtx.Done():
				// Check if all CRDs are actually deleted before returning error
				allGone := true
				for _, obj := range objects {
					if obj.GetKind() != "CustomResourceDefinition" {
						continue
					}
					_, err := a.k8sClient.Resource(crdGVR).Get(
						context.Background(), // Use fresh context for final check
						obj.GetName(),
						metav1.GetOptions{},
					)
					if err == nil {
						allGone = false
						break
					}
				}

				if allGone {
					log.Log.Info("✅ All CRDs verified as deleted - operation complete")
					return nil
				}

				if waitCtx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("timeout waiting for CRDs to be deleted")
				}
				return waitCtx.Err()
			case <-ticker.C:
				allDeleted := true
				for _, obj := range objects {
					if obj.GetKind() != "CustomResourceDefinition" {
						continue
					}

					// Check if CRD still exists
					_, err := a.k8sClient.Resource(crdGVR).Get(
						ctx,
						obj.GetName(),
						metav1.GetOptions{},
					)

					if err == nil {
						// Still exists
						allDeleted = false
						log.Log.V(2).Info("CRD still exists", "name", obj.GetName())
					} else if !errors.IsNotFound(err) {
						// Unexpected error
						log.Log.Error(err, "Error checking CRD", "name", obj.GetName())
					}
				}

				if allDeleted {
					log.Log.Info("✅ All CRDs deleted successfully - cleanup complete")
					return nil
				}
			}
		}
	}

	log.Log.Info("✅ CRD destroy complete", "deletedCount", deletedCount)
	return nil
}

// GetRESTConfig gets the Kubernetes REST config
func GetRESTConfig() (*rest.Config, error) {
	// This should be implemented based on your existing configuration logic
	// For now, using a simple implementation
	return rest.InClusterConfig()
}

// CRDsApplierAdapter wraps CRDsApplier to implement impl.K8sApplier interface
type CRDsApplierAdapter struct {
	applier *CRDsApplier
}

// NewCRDsApplierAdapter creates a new adapter
func NewCRDsApplierAdapter(applier *CRDsApplier) *CRDsApplierAdapter {
	return &CRDsApplierAdapter{
		applier: applier,
	}
}

// Apply implements impl.K8sApplier.Apply
func (a *CRDsApplierAdapter) Apply(ctx context.Context, objects []*unstructured.Unstructured) impl.ApplyResult {
	err := a.applier.Apply(ctx, objects)
	if err != nil {
		return impl.ApplyResult{Error: err}
	}
	return impl.ApplyResult{
		LiveObjects: objects,
	}
}

// WaitForApply implements impl.K8sApplier.WaitForApply
func (a *CRDsApplierAdapter) WaitForApply(ctx context.Context, objects []*unstructured.Unstructured, timeout time.Duration) impl.WaitResult {
	// Apply already waits for CRDs to be ready, so we just need to call it
	// Create a new context with the specified timeout
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := a.applier.Apply(waitCtx, objects)
	if err != nil {
		return impl.WaitResult{Error: err}
	}

	return impl.WaitResult{
		LiveObjects: objects,
	}
}

// Refresh implements impl.K8sApplier.Refresh
func (a *CRDsApplierAdapter) Refresh(ctx context.Context, objects []*unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	// For CRDs, refresh means getting their current state
	// Since CRDsApplier doesn't have a refresh method, we'll fetch them directly
	refreshedObjects := make([]*unstructured.Unstructured, 0, len(objects))

	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	for _, obj := range objects {
		if obj.GetKind() != "CustomResourceDefinition" {
			continue
		}

		// Get the current state
		current, err := a.applier.k8sClient.Resource(crdGVR).Get(
			ctx,
			obj.GetName(),
			metav1.GetOptions{},
		)

		if err != nil {
			if errors.IsNotFound(err) {
				// CRD doesn't exist, skip it
				continue
			}
			return nil, fmt.Errorf("failed to refresh CRD %s: %w", obj.GetName(), err)
		}

		refreshedObjects = append(refreshedObjects, current)
	}

	return refreshedObjects, nil
}

// Destroy implements impl.K8sApplier.Destroy
func (a *CRDsApplierAdapter) Destroy(ctx context.Context, objects []*unstructured.Unstructured) impl.DestroyResult {
	err := a.applier.Destroy(ctx, objects)
	if err != nil {
		return impl.DestroyResult{Error: err}
	}
	return impl.DestroyResult{}
}

// WaitForDestroy implements impl.K8sApplier.WaitForDestroy
func (a *CRDsApplierAdapter) WaitForDestroy(ctx context.Context, objects []*unstructured.Unstructured, timeout time.Duration) impl.WaitResult {
	// Destroy already waits for CRDs to be deleted, so we just need to call it
	// Create a new context with the specified timeout
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := a.applier.Destroy(waitCtx, objects)
	if err != nil {
		return impl.WaitResult{Error: err}
	}

	return impl.WaitResult{}
}
