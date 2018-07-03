// Copyright 2018 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubernetesenv

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	k8s "k8s.io/client-go/kubernetes"

	"istio.io/istio/pkg/log"
)

const (
	mcLabel    = "istio/multiCluster"
	maxRetries = 5
)

var (
	serverStartTime time.Time
)

// Controller is the controller implementation for Secret resources
type Controller struct {
	b                 *builder
	kubeHandler       *handler
	kubeclientset     kubernetes.Interface
	namespace         string
	queue             workqueue.RateLimitingInterface
	informer          cache.SharedIndexInformer
	watchedNamespace  string
	domainSuffix      string
	resyncInterval    time.Duration
}

// Metadata defines a struct used as a key
type Metadata struct {
	Name, Namespace string
}

// newSecretController returns a new secret controller
func newSecretController(b *builder,
	kubeHandler *handler,
	kubeclientset kubernetes.Interface,
	namespace string,
	resyncInterval time.Duration,
	watchedNamespace string,
	domainSuffix string) *Controller {

	secretsInformer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListFunc: func(opts meta_v1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = mcLabel + "=true"
			return kubeclientset.CoreV1().Secrets(namespace).List(opts)
		},
		WatchFunc: func(opts meta_v1.ListOptions) (watch.Interface, error) {
			opts.LabelSelector = mcLabel + "=true"
			return kubeclientset.CoreV1().Secrets(namespace).Watch(opts)
		},
	},
		&corev1.Secret{},
		0,
		cache.Indexers{})

	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	controller := &Controller{
		b:                 b,
	    kubeHandler:       kubeHandler,
		kubeclientset:     kubeclientset,
		namespace:         namespace,
		informer:          secretsInformer,
		queue:             queue,
		watchedNamespace:  watchedNamespace,
		domainSuffix:      domainSuffix,
		resyncInterval:    resyncInterval,
	}

	log.Info("Setting up event handlers")
	secretsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			log.Infof("Processing add: %s", key)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			log.Infof("Processing delete: %s", key)
			if err == nil {
				queue.Add(key)
			}
		},
	})

	return controller
}

// Run starts the controller until it receves a message over stopCh
func (c *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	log.Info("Starting Secrets controller")
	serverStartTime = time.Now().Local()
	go c.informer.Run(stopCh)

	// Wait for the caches to be synced before starting workers
	log.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	wait.Until(c.runWorker, 5*time.Second, stopCh)
}

// StartSecretController start k8s controller which will be watching Secret object
// in a specified namesapce
func StartSecretController(b *builder,
	kubeHandler *handler,
	k8s kubernetes.Interface,
	namespace string,
	resyncInterval time.Duration,
	watchedNamespace,
	domainSuffix string) error {
	stopCh := make(chan struct{})
	controller := newSecretController(b, kubeHandler, k8s, namespace, resyncInterval,
		watchedNamespace, domainSuffix)

	go controller.Run(stopCh)

	return nil
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
		// continue looping
	}
}

func (c *Controller) processNextItem() bool {
	secretName, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(secretName)

	err := c.processItem(secretName.(string))
	if err == nil {
		// No error, reset the ratelimit counters
		c.queue.Forget(secretName)
	} else if c.queue.NumRequeues(secretName) < maxRetries {
		log.Errorf("Error processing %s (will retry): %v", secretName, err)
		c.queue.AddRateLimited(secretName)
	} else {
		log.Errorf("Error processing %s (giving up): %v", secretName, err)
		c.queue.Forget(secretName)
		utilruntime.HandleError(err)
	}

	return true
}

func (c *Controller) processItem(secretName string) error {
	obj, exists, err := c.informer.GetIndexer().GetByKey(secretName)
	if err != nil {
		return fmt.Errorf("error fetching object with secretName %s from store: %v", secretName, err)
	}

	if exists {
		c.createCacheController(secretName, obj.(*corev1.Secret))
	} else {
		c.deleteCacheController(secretName)
	}

	return nil
}

func (c *Controller ) createCacheController(secretName string, secret *corev1.Secret) {
	var k8sInterface k8s.Interface
	var err error
	var controller cacheController

	for clusterID, kubeconfig := range secret.Data {
		if _, ok := c.b.controllers[secretName]; !ok {
			c.b.controllers[secretName] = make(map[string]cacheController)
			c.kubeHandler.k8sCache[secretName] = make(map[string]cacheController)
		}
		if _, ok := c.b.controllers[secretName][clusterID]; !ok {
			k8sInterface, err = getK8sInterface(kubeconfig)
		    if err != nil {
				c.kubeHandler.env.Logger().Errorf("error on K8s interface access: %v", err)
			}

			controller, err = getNewCacheController(c.b, k8sInterface, c.kubeHandler.env)
			if err == nil {
				c.b.controllers[secretName][clusterID] = controller
				c.kubeHandler.k8sCache[secretName][clusterID] = controller
				c.kubeHandler.env.Logger().Infof("created remote controller %s %s", secretName, clusterID)
			} else {
				c.kubeHandler.env.Logger().Errorf("error on creating remote controller: %s", secretName)
			}		
		} else {
			c.kubeHandler.env.Logger().Infof("remote cluster %s %s has already been created, secret ignored",
				secretName, clusterID)
		}
	}
}

func (c *Controller) deleteCacheController(secretName string) {
	if _, ok := c.b.controllers[secretName]; ok {
		for clusterID := range c.b.controllers[secretName] {
			c.kubeHandler.k8sCache[secretName][clusterID].StopControlChannel()
			c.kubeHandler.env.Logger().Infof("deleted remote controller %s %s", secretName, clusterID)
		}
		delete(c.b.controllers, secretName)
		delete(c.kubeHandler.k8sCache, secretName)
	}
}
