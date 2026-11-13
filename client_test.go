package datahub

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
)

type TestConfig struct {
	DataHubUrl              string
	AdminUser               string
	AdminKey                string
	ClientCredentialsKey    string
	ClientCredentialsSecret string
	AuthorizerUrl           string
	Audience                string
}

// You can use testing.T, if you want to test the code without benchmarking
func getTestConfig() *TestConfig {
	// load credentials from environment
	testConfig := &TestConfig{}
	testConfig.DataHubUrl = os.Getenv("DATAHUB_CLI_TEST_URL")
	testConfig.AdminUser = os.Getenv("DATAHUB_CLI_TEST_ADMIN_USER")
	testConfig.AdminKey = os.Getenv("DATAHUB_CLI_TEST_ADMIN_KEY")
	testConfig.ClientCredentialsKey = os.Getenv("DATAHUB_CLI_TEST_CLIENT_KEY")
	testConfig.ClientCredentialsSecret = os.Getenv("DATAHUB_CLI_TEST_CLIENT_SECRET")
	testConfig.AuthorizerUrl = os.Getenv("DATAHUB_CLI_TEST_AUTH_SERVICE_URL")
	testConfig.Audience = os.Getenv("DATAHUB_CLI_TEST_AUTH_SERVICE_AUDIENCE")
	return testConfig
}

func TestClientCredentialsAuthenticate(t *testing.T) {
	testConfig := getTestConfig()

	// get test credentials
	if testConfig.ClientCredentialsKey == "" || testConfig.ClientCredentialsSecret == "" {
		t.Skip("skipping test; no credentials provided")
	}

	if testConfig.AuthorizerUrl == "" || testConfig.Audience == "" {
		t.Skip("skipping test; no auth service config provided")
	}

	// test connect
	client, err := NewClient("http://localhost:8080")
	client.WithClientKeyAndSecretAuth(testConfig.AuthorizerUrl, testConfig.Audience, testConfig.ClientCredentialsKey, testConfig.ClientCredentialsSecret)
	err = client.Authenticate()
	if err != nil {
		t.Error(err)
	}
	if client.AuthToken.AccessToken == "" {
		t.Error("expected token to be populated")
	}
}

func TestAdminAuthenticate(t *testing.T) {
	testConfig := getTestConfig()
	if testConfig.DataHubUrl == "" || testConfig.AdminUser == "" || testConfig.AdminKey == "" {
		t.Skip("skipping test; no credentials provided")
	}

	// test connect
	client, err := NewClient(testConfig.DataHubUrl)
	client.WithAdminAuth(testConfig.AdminUser, testConfig.AdminKey)
	err = client.Authenticate()
	if err != nil {
		t.Error(err)
	}
	if client.AuthToken.AccessToken == "" {
		t.Error("expected token to be populated")
	}
}

func TestClientCertificateAuthenticate(t *testing.T) {
	testConfig := getTestConfig()
	if testConfig.DataHubUrl == "" {
		t.Skip("skipping test; no credentials provided")
	}

	client, err := NewClient(testConfig.DataHubUrl)
	client.WithAdminAuth(testConfig.AdminUser, testConfig.AdminKey)
	err = client.Authenticate()
	if err != nil {
		t.Error(err)
	}

	// use client to store a certificate
	privateKey, publicKey, err := client.GenerateKeypair()
	if err != nil {
		t.Error(err)
	}

	// generate client id from uuid
	clientId := "test-" + uuid.New().String()
	err = client.AddClient(clientId, publicKey)
	if err != nil {
		t.Error(err)
	}

	publicKeyClient, err := NewClient(testConfig.DataHubUrl)
	publicKeyClient.WithPublicKeyAuth(clientId, privateKey)

	err = publicKeyClient.Authenticate()
	if err != nil {
		t.Error(err)
	}
}

func TestClientCredentialsRequestsScopes(t *testing.T) {
	var gotScope string
	var auth *httptest.Server
	auth = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/.well-known/openid-configuration" {
			fmt.Fprintf(w, `{"issuer":%q,"token_endpoint":%q}`, auth.URL, auth.URL+"/token")
			return
		}
		_ = r.ParseForm()
		gotScope = r.PostForm.Get("scope")
		fmt.Fprint(w, `{"access_token":"token","token_type":"bearer","expires_in":3600}`)
	}))
	defer auth.Close()

	client, err := NewClient("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	client.WithClientKeyAndSecretAuth(auth.URL, "audience", "id", "secret", "datahub:r", "datahub:w")
	if err := client.Authenticate(); err != nil {
		t.Fatal(err)
	}
	if gotScope != "datahub:r datahub:w" {
		t.Errorf("expected scope %q, got %q", "datahub:r datahub:w", gotScope)
	}
}
