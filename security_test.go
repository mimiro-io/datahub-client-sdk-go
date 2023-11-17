package datahub

import (
	"github.com/google/uuid"
	"testing"
)

func TestGetClients(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	_, err := client.GetClients()
	if err != nil {
		t.Error(err)
	}
}

func TestAddClient(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	_, publicKey, err := client.GenerateKeypair()
	if err != nil {
		t.Error(err)
	}
	clientID := "client-" + uuid.New().String()
	err = client.AddClient(clientID, publicKey)
	if err != nil {
		t.Error(err)
	}

	clients, err := client.GetClients()
	if err != nil {
		t.Error(err)
	}

	if _, ok := clients[clientID]; !ok {
		t.Errorf("expected client '%s' to be present", clientID)
	}

	// check public key is the same
	keyOnServer, err := parseRsaPublicKeyFromPem(clients[clientID].PublicKey)
	if err != nil {
		t.Error(err)
	}

	if keyOnServer.N.Cmp(publicKey.N) != 0 {
		t.Errorf("expected public key to be '%s', got '%s'", publicKey.N, keyOnServer.N)
	}
}

func TestDeleteClient(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	_, publicKey, err := client.GenerateKeypair()
	if err != nil {
		t.Error(err)
	}
	clientID := "client-" + uuid.New().String()
	err = client.AddClient(clientID, publicKey)
	if err != nil {
		t.Error(err)
	}

	clients, err := client.GetClients()
	if err != nil {
		t.Error(err)
	}

	if _, ok := clients[clientID]; !ok {
		t.Errorf("expected client '%s' to be present", clientID)
	}

	err = client.DeleteClient(clientID)
	if err != nil {
		t.Error(err)
	}

	clients, err = client.GetClients()
	if err != nil {
		t.Error(err)
	}

	if _, ok := clients[clientID]; ok {
		t.Errorf("expected client '%s' to be deleted", clientID)
	}
}

func TestSetClientAcl(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	_, publicKey, err := client.GenerateKeypair()
	if err != nil {
		t.Error(err)
	}
	clientID := "client-" + uuid.New().String()
	err = client.AddClient(clientID, publicKey)
	if err != nil {
		t.Error(err)
	}

	clients, err := client.GetClients()
	if err != nil {
		t.Error(err)
	}

	if _, ok := clients[clientID]; !ok {
		t.Errorf("expected client '%s' to be present", clientID)
	}

	// check public key is the same
	keyOnServer, err := parseRsaPublicKeyFromPem(clients[clientID].PublicKey)
	if err != nil {
		t.Error(err)
	}

	if keyOnServer.N.Cmp(publicKey.N) != 0 {
		t.Errorf("expected public key to be '%s', got '%s'", publicKey.N, keyOnServer.N)
	}

	// add acl
	access := make([]AccessControl, 0)
	access = append(access, AccessControl{Action: "read", Resource: "/datasets/people/*"})
	err = client.SetClientAcl(clientID, access)
	if err != nil {
		t.Error(err)
	}

	accessOnServer, err := client.GetClientAcl(clientID)
	if err != nil {
		t.Error(err)
	}

	if len(accessOnServer) != 1 {
		t.Errorf("expected 1 acl, got '%d'", len(accessOnServer))
	}

	if accessOnServer[0].Action != "read" {
		t.Errorf("expected action to be 'read', got '%s'", accessOnServer[0].Action)
	}

	if accessOnServer[0].Resource != "/datasets/people/*" {
		t.Errorf("expected resource to be '/datasets/people/*', got '%s'", accessOnServer[0].Resource)
	}
}

func TestAddTokenProvider(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate name for provider from uuid
	providerName := "provider-" + uuid.New().String()

	tokenProvider := &ProviderConfig{
		Name: providerName,
		Type: "token",
		User: &ValueReader{
			Type:  "string",
			Value: "test-user",
		},
		Password: &ValueReader{
			Type:  "string",
			Value: "test-password",
		},
		ClientId: &ValueReader{
			Type:  "string",
			Value: "test-client-id",
		},
		ClientSecret: &ValueReader{
			Type:  "string",
			Value: "test-client-secret",
		},
		Audience: &ValueReader{
			Type:  "string",
			Value: "test-audience",
		},
		Endpoint: &ValueReader{
			Type:  "string",
			Value: "test-endpoint",
		},
	}

	err := client.AddTokenProvider(tokenProvider)
	if err != nil {
		t.Error(err)
	}

	providers, err := client.GetTokenProviders()
	if err != nil {
		t.Error(err)
	}

	var registeredProvider *ProviderConfig
	for _, provider := range providers {
		if provider.Name == providerName {
			registeredProvider = provider
			break
		}
	}

	if registeredProvider == nil {
		t.Errorf("expected provider '%s' to be present", providerName)
	}

	if registeredProvider.Name != tokenProvider.Name {
		t.Errorf("expected name to be '%s', got '%s'", tokenProvider.Name, registeredProvider.Name)
	}

	if registeredProvider.Type != tokenProvider.Type {
		t.Errorf("expected type to be '%s', got '%s'", tokenProvider.Type, registeredProvider.Type)
	}

	if registeredProvider.User.Value != tokenProvider.User.Value {
		t.Errorf("expected user value to be '%s', got '%s'", tokenProvider.User.Value, registeredProvider.User.Value)
	}

	if registeredProvider.Password.Value != "*****" {
		t.Errorf("expected password value to be '%s', got '%s'", tokenProvider.Password.Value, registeredProvider.Password.Value)
	}

	if registeredProvider.ClientId.Value != tokenProvider.ClientId.Value {
		t.Errorf("expected client id value to be '%s', got '%s'", tokenProvider.ClientId.Value, registeredProvider.ClientId.Value)
	}

	if registeredProvider.ClientSecret.Value != "*****" {
		t.Errorf("expected client secret value to be '%s', got '%s'", tokenProvider.ClientSecret.Value, registeredProvider.ClientSecret.Value)
	}

	if registeredProvider.Audience.Value != tokenProvider.Audience.Value {
		t.Errorf("expected audience value to be '%s', got '%s'", tokenProvider.Audience.Value, registeredProvider.Audience.Value)
	}

	if registeredProvider.Endpoint.Value != tokenProvider.Endpoint.Value {
		t.Errorf("expected endpoint value to be '%s', got '%s'", tokenProvider.Endpoint.Value, registeredProvider.Endpoint.Value)
	}
}

func TestGetTokenProvider(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate name for provider from uuid
	providerName := "provider-" + uuid.New().String()

	tokenProvider := &ProviderConfig{
		Name: providerName,
		Type: "token",
		User: &ValueReader{
			Type:  "string",
			Value: "test-user",
		},
		Password: &ValueReader{
			Type:  "string",
			Value: "test-password",
		},
		ClientId: &ValueReader{
			Type:  "string",
			Value: "test-client-id",
		},
		ClientSecret: &ValueReader{
			Type:  "string",
			Value: "test-client-secret",
		},
		Audience: &ValueReader{
			Type:  "string",
			Value: "test-audience",
		},
		Endpoint: &ValueReader{
			Type:  "string",
			Value: "test-endpoint",
		},
	}

	err := client.AddTokenProvider(tokenProvider)
	if err != nil {
		t.Error(err)
	}

	registeredProvider, err := client.GetTokenProvider(providerName)
	if err != nil {
		t.Error(err)
	}

	if registeredProvider == nil {
		t.Errorf("expected provider '%s' to be present", providerName)
	}
}

func TestDeleteTokenProvider(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate name for provider from uuid
	providerName := "provider-" + uuid.New().String()

	tokenProvider := &ProviderConfig{
		Name: providerName,
		Type: "token",
		User: &ValueReader{
			Type:  "string",
			Value: "test-user",
		},
		Password: &ValueReader{
			Type:  "string",
			Value: "test-password",
		},
		ClientId: &ValueReader{
			Type:  "string",
			Value: "test-client-id",
		},
		ClientSecret: &ValueReader{
			Type:  "string",
			Value: "test-client-secret",
		},
		Audience: &ValueReader{
			Type:  "string",
			Value: "test-audience",
		},
		Endpoint: &ValueReader{
			Type:  "string",
			Value: "test-endpoint",
		},
	}

	err := client.AddTokenProvider(tokenProvider)
	if err != nil {
		t.Error(err)
	}

	providers, err := client.GetTokenProviders()
	if err != nil {
		t.Error(err)
	}

	var registeredProvider *ProviderConfig
	for _, provider := range providers {
		if provider.Name == providerName {
			registeredProvider = provider
			break
		}
	}

	if registeredProvider == nil {
		t.Errorf("expected provider '%s' to be present", providerName)
	}

	err = client.DeleteTokenProvider(providerName)
	if err != nil {
		t.Error(err)
	}

	providers, err = client.GetTokenProviders()
	if err != nil {
		t.Error(err)
	}

	for _, provider := range providers {
		if provider.Name == providerName {
			t.Errorf("expected provider '%s' to be deleted", providerName)
		}
	}

}
