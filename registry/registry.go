package registry

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/df010/router-lite/registry/container"
	"github.com/df010/router-lite/route"
	// "github.com/uber-go/zap"
	//
	// "code.cloudfoundry.org/gorouter/config"
	// "code.cloudfoundry.org/gorouter/logger"
	// "code.cloudfoundry.org/gorouter/metrics"
	// "code.cloudfoundry.org/gorouter/registry/container"
	// "code.cloudfoundry.org/gorouter/route"
)

//go:generate counterfeiter -o fakes/fake_registry.go . Registry
type Registry interface {
	Register(uri route.Uri, endpoint *route.Endpoint)
	Unregister(uri route.Uri, endpoint *route.Endpoint)
	Lookup(uri route.Uri) *route.Pool
	LookupWithInstance(uri route.Uri, appId, appIndex string) *route.Pool
	StartPruningCycle()
	StopPruningCycle()
	NumUris() int
	NumEndpoints() int
	MarshalJSON() ([]byte, error)
}

type PruneStatus int

const (
	CONNECTED = PruneStatus(iota)
	DISCONNECTED
)

type RouteRegistry struct {
	sync.RWMutex

	// Access to the Trie datastructure should be governed by the RWMutex of RouteRegistry
	byUri *container.Trie

	// used for ability to suspend pruning
	suspendPruning func() bool
	pruningStatus  PruneStatus

	pruneStaleDropletsInterval time.Duration
	dropletStaleThreshold      time.Duration

	ticker           *time.Ticker
	timeOfLastUpdate time.Time
}

func NewRouteRegistry() *RouteRegistry {
	r := &RouteRegistry{}
	r.byUri = container.NewTrie()
	r.suspendPruning = func() bool { return false }
	return r
}

func (r *RouteRegistry) Register(uri route.Uri, endpoint *route.Endpoint) {
	t := time.Now()

	r.Lock()

	routekey := uri.RouteKey()

	pool := r.byUri.Find(routekey)
	if pool == nil {
		contextPath := parseContextPath(uri)
		pool = route.NewPool(r.dropletStaleThreshold/4, contextPath)
		r.byUri.Insert(routekey, pool)
	}

	pool.Put(endpoint)

	r.timeOfLastUpdate = t
	r.Unlock()

	// r.reporter.CaptureRegistryMessage(endpoint)
	//
	// zapData := []zap.Field{
	// 	zap.Stringer("uri", uri),
	// 	zap.String("backend", endpoint.CanonicalAddr()),
	// 	zap.Object("modification_tag", endpoint.ModificationTag),
	// }
	//
	// if endpointAdded {
	// 	r.logger.Debug("endpoint-registered", zapData...)
	// } else {
	// 	r.logger.Debug("endpoint-not-registered", zapData...)
	// }
}

func (r *RouteRegistry) Unregister(uri route.Uri, endpoint *route.Endpoint) {
	r.Lock()

	uri = uri.RouteKey()

	pool := r.byUri.Find(uri)
	if pool != nil {
		pool.Remove(endpoint)
		if pool.IsEmpty() {
			r.byUri.Delete(uri)
		}
	}

	r.Unlock()
}

func (r *RouteRegistry) Lookup(uri route.Uri) *route.Pool {
	r.RLock()

	uri = uri.RouteKey()
	var err error
	pool := r.byUri.MatchUri(uri)
	for pool == nil && err == nil {
		uri, err = uri.NextWildcard()
		pool = r.byUri.MatchUri(uri)
	}

	r.RUnlock()
	return pool
}

func (r *RouteRegistry) LookupWithInstance(uri route.Uri, appId string, appIndex string) *route.Pool {
	uri = uri.RouteKey()
	p := r.Lookup(uri)

	var surgicalPool *route.Pool

	p.Each(func(e *route.Endpoint) {
		if (e.ApplicationId == appId) && (e.PrivateInstanceIndex == appIndex) {
			surgicalPool = route.NewPool(0, "")
			surgicalPool.Put(e)
		}
	})
	return surgicalPool
}

func (r *RouteRegistry) StartPruningCycle() {
	if r.pruneStaleDropletsInterval > 0 {
		r.Lock()
		r.ticker = time.NewTicker(r.pruneStaleDropletsInterval)
		r.Unlock()

		go func() {
			for {
				select {
				case <-r.ticker.C:
					r.pruneStaleDroplets()
				}
			}
		}()
	}
}

func (r *RouteRegistry) StopPruningCycle() {
	r.Lock()
	if r.ticker != nil {
		r.ticker.Stop()
	}
	r.Unlock()
}

func (registry *RouteRegistry) NumUris() int {
	registry.RLock()
	uriCount := registry.byUri.PoolCount()
	registry.RUnlock()

	return uriCount
}

func (r *RouteRegistry) TimeOfLastUpdate() time.Time {
	r.RLock()
	t := r.timeOfLastUpdate
	r.RUnlock()

	return t
}

func (r *RouteRegistry) NumEndpoints() int {
	r.RLock()
	count := r.byUri.EndpointCount()
	r.RUnlock()

	return count
}

func (r *RouteRegistry) MarshalJSON() ([]byte, error) {
	r.RLock()
	defer r.RUnlock()

	return json.Marshal(r.byUri.ToMap())
}

func (r *RouteRegistry) pruneStaleDroplets() {
	r.Lock()
	defer r.Unlock()

	// suspend pruning if option enabled and if NATS is unavailable
	if r.suspendPruning() {
		r.pruningStatus = DISCONNECTED
		return
	} else {
		if r.pruningStatus == DISCONNECTED {
			// if we are coming back from being disconnected from source,
			// bulk update routes / mark updated to avoid pruning right away
			r.freshenRoutes()
		}
		r.pruningStatus = CONNECTED
	}

	r.byUri.EachNodeWithPool(func(t *container.Trie) {
		endpoints := t.Pool.PruneEndpoints(r.dropletStaleThreshold)
		t.Snip()
		if len(endpoints) > 0 {
			addresses := []string{}
			for _, e := range endpoints {
				addresses = append(addresses, e.CanonicalAddr())
			}
		}
	})
}

func (r *RouteRegistry) SuspendPruning(f func() bool) {
	r.Lock()
	r.suspendPruning = f
	r.Unlock()
}

// bulk update to mark pool / endpoints as updated
func (r *RouteRegistry) freshenRoutes() {
	now := time.Now()
	r.byUri.EachNodeWithPool(func(t *container.Trie) {
		t.Pool.MarkUpdated(now)
	})
}

func parseContextPath(uri route.Uri) string {
	contextPath := "/"
	split := strings.SplitN(strings.TrimPrefix(uri.String(), "/"), "/", 2)

	if len(split) > 1 {
		contextPath += split[1]
	}

	if idx := strings.Index(string(contextPath), "?"); idx >= 0 {
		contextPath = contextPath[0:idx]
	}

	return contextPath
}
