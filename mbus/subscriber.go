package mbus

import (
	"encoding/json"
	"errors"
	"os"
	"strings"

	"code.cloudfoundry.org/routing-api/models"

	"github.com/df010/router-lite/registry"
	"github.com/df010/router-lite/route"
	"github.com/nats-io/nats"
)

// RegistryMessage defines the format of a route registration/unregistration
type RegistryMessage struct {
	Host                    string            `json:"host"`
	Port                    uint16            `json:"port"`
	Uris                    []route.Uri       `json:"uris"`
	Tags                    map[string]string `json:"tags"`
	App                     string            `json:"app"`
	StaleThresholdInSeconds int               `json:"stale_threshold_in_seconds"`
	RouteServiceURL         string            `json:"route_service_url"`
	PrivateInstanceID       string            `json:"private_instance_id"`
	PrivateInstanceIndex    string            `json:"private_instance_index"`
}

func (rm *RegistryMessage) makeEndpoint() *route.Endpoint {
	return route.NewEndpoint(
		rm.App,
		rm.Host,
		rm.Port,
		rm.PrivateInstanceID,
		rm.PrivateInstanceIndex,
		rm.Tags,
		rm.StaleThresholdInSeconds,
		rm.RouteServiceURL,
		models.ModificationTag{})
}

// ValidateMessage checks to ensure the registry message is valid
func (rm *RegistryMessage) ValidateMessage() bool {
	return rm.RouteServiceURL == "" || strings.HasPrefix(rm.RouteServiceURL, "https")
}

// Subscriber subscribes to NATS for all router.* messages and handles them
type Subscriber struct {
	natsClient    *nats.Conn
	routeRegistry registry.Registry
}

// NewSubscriber returns a new Subscriber
func NewSubscriber(
	natsClient *nats.Conn,
	routeRegistry registry.Registry,
) *Subscriber {
	return &Subscriber{
		natsClient:    natsClient,
		routeRegistry: routeRegistry,
	}
}

// Run manages the lifecycle of the subscriber process
func (s *Subscriber) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	err := s.subscribeRoutes()
	if err != nil {
		return err
	}

	close(ready)
	for {
		select {
		case <-signals:
			return nil
		}
	}
}

func (s *Subscriber) subscribeRoutes() error {
	natsSubscriber, err := s.natsClient.Subscribe("router.*", func(message *nats.Msg) {
		switch message.Subject {
		case "router.register":
			s.registerRoute(message)
		case "router.unregister":
			s.unregisterRoute(message)
		default:
		}
	})

	// Pending limits are set to twice the defaults
	natsSubscriber.SetPendingLimits(131072, 131072*1024)
	return err
}

func (s *Subscriber) unregisterRoute(message *nats.Msg) {
	msg, regErr := createRegistryMessage(message.Data)
	if regErr != nil {
		return
	}

	endpoint := msg.makeEndpoint()
	for _, uri := range msg.Uris {
		s.routeRegistry.Unregister(uri, endpoint)
	}
}

func (s *Subscriber) registerRoute(message *nats.Msg) {
	msg, regErr := createRegistryMessage(message.Data)
	if regErr != nil {
		return
	}

	endpoint := msg.makeEndpoint()
	for _, uri := range msg.Uris {
		s.routeRegistry.Register(uri, endpoint)
	}
}

func createRegistryMessage(data []byte) (*RegistryMessage, error) {
	var msg RegistryMessage

	jsonErr := json.Unmarshal(data, &msg)
	if jsonErr != nil {
		return nil, jsonErr
	}

	if !msg.ValidateMessage() {
		return nil, errors.New("Unable to validate message. route_service_url must be https")
	}

	return &msg, nil
}
