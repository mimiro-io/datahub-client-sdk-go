package datahub

import (
	"os"
	"testing"
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
