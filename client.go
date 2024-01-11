// package datahub provides a sdk for interacting with MIMIRO data hub instances.
package datahub

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/coreos/go-oidc/v3/oidc"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type EntityIterator interface {
	Context() *egdm.Context
	Next() (*egdm.Entity, error)
	Token() *egdm.Continuation
}

type AuthType int

const (
	// AuthTypeNone used for connecting to unsercured datahub instances
	AuthTypeNone AuthType = iota
	// AuthTypeBasic used for connecting as admin user with username and password
	AuthTypeBasic
	// AuthTypeClientKeyAndSecret used for OAuth flow with client key and secret
	AuthTypeClientKeyAndSecret
	// AuthTypePublicKey Used for OAuth flow with signed JWT authentication request
	AuthTypePublicKey
	// AuthTypeUser Used the OAuth User flow - Not yet supported
	AuthTypeUser
)

// authConfig contains the configuration for the different authentication types
type authConfig struct {
	AuthType     AuthType
	Authorizer   string
	ClientID     string
	ClientSecret string
	Audience     string
	PrivateKey   *rsa.PrivateKey
}

// Client is the main entry point for the data hub client sdk
type Client struct {
	AuthConfig *authConfig
	AuthToken  *oauth2.Token
	Server     string
}

// NewClient creates a new client instance.
// Specify the data hub server url as the parameter.
// Use the withXXX functions to configure options
// returns a ParameterError if the server url is empty or invalid URL
func NewClient(server string) (*Client, error) {
	if server == "" {
		return nil, &ParameterError{Err: nil, Msg: "server url is required"}
	}
	_, err := url.Parse(server)
	if err != nil {
		return nil, &ParameterError{Err: err, Msg: "server url is not valid"}
	}
	client := &Client{}
	client.Server = server
	client.AuthConfig = &authConfig{
		AuthType: AuthTypeNone,
	}
	return client, nil
}

// makeHttpClient creates a new http client with the specified access token
// and server configured
func (c *Client) makeHttpClient() *httpClient {
	accessToken := ""
	if c.AuthToken != nil {
		accessToken = c.AuthToken.AccessToken
	}

	client := newHttpClient(c.Server, accessToken)
	return client
}

// WithExistingToken sets the authentication token to use.
// This is useful if you have a reconstituted a stored token from a previous session
func (c *Client) WithExistingToken(token *oauth2.Token) *Client {
	c.AuthToken = token
	return c
}

// WithAdminAuth sets the authentication type to basic authentication.
// username and password are the credentials of the admin user
func (c *Client) WithAdminAuth(username string, password string) *Client {
	c.AuthConfig = &authConfig{
		AuthType:     AuthTypeBasic,
		ClientID:     username,
		ClientSecret: password,
		Authorizer:   c.Server,
	}
	return c
}

// WithClientKeyAndSecretAuth sets the authentication type to client key and secret OAuth2 authentication flow
// authorizer is the url of the authorizer service
// audience is the audience identifier
// clientKey is the client key
// clientSecret is the client secret
func (c *Client) WithClientKeyAndSecretAuth(authorizer string, audience string, clientKey string, clientSecret string) *Client {
	c.AuthConfig = &authConfig{
		AuthType:     AuthTypeClientKeyAndSecret,
		ClientID:     clientKey,
		ClientSecret: clientSecret,
		Authorizer:   authorizer,
		Audience:     audience,
	}
	return c
}

// WithPublicKeyAuth sets the authentication type to public key authentication.
// Sets the client id and private key
func (c *Client) WithPublicKeyAuth(clientID string, privateKey *rsa.PrivateKey) *Client {
	c.AuthConfig = &authConfig{
		AuthType:   AuthTypePublicKey,
		ClientID:   clientID,
		Audience:   "datahub-client-sdk",
		PrivateKey: privateKey,
		Authorizer: c.Server,
	}
	return c
}

// WithUserAuth sets the authentication type to user authentication
// and sets the authorizer url and audience
// NOT SUPPORTED YET
func (c *Client) WithUserAuth(authorizer string, audience string) *Client {
	c.AuthConfig = &authConfig{
		AuthType:   AuthTypeUser,
		Audience:   audience,
		Authorizer: authorizer,
	}
	return c
}

// checkToken checks if the current token is valid and if not, attempts to authenticate
func (c *Client) checkToken() error {
	if c.AuthToken == nil || !c.AuthToken.Valid() {
		err := c.Authenticate()
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}

// Authenticate attempts to authenticate the client with the configured authentication type
// returns an AuthenticationError if authentication fails
func (c *Client) Authenticate() error {
	if c.isTokenValid() {
		return nil
	}

	if c.AuthConfig.AuthType == AuthTypeClientKeyAndSecret {
		token, err := c.authenticateWithClientCredentials()
		if err != nil {
			return &AuthenticationError{Err: err, Msg: "Unable to authenticate using client credentials"}
		}
		c.AuthToken = token
	} else if c.AuthConfig.AuthType == AuthTypePublicKey {
		token, err := c.authenticateWithCertificate()
		if err != nil {
			return &AuthenticationError{Err: err, Msg: "Unable to authenticate using client certificate"}
		}
		c.AuthToken = token
	} else if c.AuthConfig.AuthType == AuthTypeUser {
		token, err := c.authenticateWithUserFlow()
		if err != nil {
			return &AuthenticationError{Err: err, Msg: "Unable to authenticate with user flow"}
		}
		c.AuthToken = token
	} else if c.AuthConfig.AuthType == AuthTypeBasic {
		token, err := c.authenticateWithBasicAuth()
		if err != nil {
			return &AuthenticationError{Err: err, Msg: "Unable to authenticate using basic authentication"}
		}
		c.AuthToken = token
	}

	return nil
}

func (c *Client) authenticateWithBasicAuth() (*oauth2.Token, error) {
	clientCredentialsConfig := &clientcredentials.Config{
		ClientID:     c.AuthConfig.ClientID,
		ClientSecret: c.AuthConfig.ClientSecret,
		TokenURL:     c.AuthConfig.Authorizer + "/security/token",
	}

	return clientCredentialsConfig.Token(context.Background())
}

func (c *Client) authenticateWithUserFlow() (*oauth2.Token, error) {
	return nil, nil
}

// GenerateKeypair generates a new RSA keypair
func (c *Client) GenerateKeypair() (*rsa.PrivateKey, *rsa.PublicKey, error) {
	private, public, err := generateRsaKeyPair()
	if err != nil {
		return nil, nil, err
	}
	return private, public, nil
}

// LoadKeypair loads an RSA keypair from the specified location. Names of the key files are node_key and node_key.pub
func (c *Client) LoadKeypair(location string) (*rsa.PrivateKey, *rsa.PublicKey, error) {
	if location == "" {
		return nil, nil, &ParameterError{Err: nil, Msg: fmt.Sprintf("location %s is not valid", location)}
	}
	var privateKey *rsa.PrivateKey
	privateKeyFilename := location + string(os.PathSeparator) + "node_key"
	_, err := os.Stat(privateKeyFilename)
	if err != nil {
		return nil, nil, &ParameterError{Err: nil, Msg: fmt.Sprintf("node_key at location %s is not valid", location)}
	} else {
		// load it
		privateKeyBytes, err := readFileContents(privateKeyFilename)
		if err != nil {
			return nil, nil, err
		}
		privateKey, err = parseRsaPrivateKeyFromPem(privateKeyBytes)
		if err != nil {
			return nil, nil, err
		}

	}

	var publicKey *rsa.PublicKey
	publicKeyFilename := location + string(os.PathSeparator) + "node_key.pub"
	_, err = os.Stat(publicKeyFilename)
	if err != nil {
		return nil, nil, &ParameterError{Err: nil, Msg: fmt.Sprintf("node_key.pub at location %s is not valid", location)}
	} else {
		// load it
		publicKeyBytes, err := readFileContents(publicKeyFilename)
		if err != nil {
			return nil, nil, err
		}
		publicKey, err = parseRsaPublicKeyFromPem(publicKeyBytes)
		if err != nil {
			return nil, nil, err
		}
	}

	return privateKey, publicKey, nil
}

// Utility function to read file contents and return bytes
func readFileContents(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close() // Ensure file is closed after reading

	// Read the contents
	contents, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return contents, nil
}

// SaveKeypair saves the specified RSA keypair to the specified location. Names of the key files are node_key and node_key.pub
func (c *Client) SaveKeypair(location string, privateKey *rsa.PrivateKey, publicKey *rsa.PublicKey) error {
	privateKeyPem, err := exportRsaPrivateKeyAsPem(privateKey)
	if err != nil {
		return err

	}
	publicKeyPem, err := exportRsaPublicKeyAsPem(publicKey)
	if err != nil {
		return err
	}

	// write keys to files
	err = os.WriteFile(location+string(os.PathSeparator)+"node_key", privateKeyPem, 0600)
	if err != nil {
		return err
	}
	err = os.WriteFile(location+string(os.PathSeparator)+"node_key.pub", publicKeyPem, 0600)
	if err != nil {
		return err
	}

	return nil
}

// authenticateWithCertificate used to authenticate using a signed JWT and the client assertion
// type urn:ietf:params:oauth:grant-type:jwt-bearer.
func (c *Client) authenticateWithCertificate() (*oauth2.Token, error) {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_assertion_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")

	pem, err := createJWTForTokenRequest(c.AuthConfig.ClientID, c.AuthConfig.Audience, c.AuthConfig.PrivateKey)
	data.Set("client_assertion", pem)

	reqUrl := c.AuthConfig.Authorizer + "/security/token"
	res, err := http.PostForm(reqUrl, data)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(res.Body)
	response := make(map[string]interface{})
	err = decoder.Decode(&response)
	if err != nil {
		return nil, err
	}
	accessToken := response["access_token"].(string)

	return &oauth2.Token{
		AccessToken: accessToken,
	}, nil
}

func (c *Client) authenticateWithClientCredentials() (*oauth2.Token, error) {
	// check we have the required config
	if c.AuthConfig.ClientID == "" {
		return nil, errors.New("missing client id")
	}

	if c.AuthConfig.ClientSecret == "" {
		return nil, errors.New("missing client secret")
	}

	if c.AuthConfig.Authorizer == "" {
		return nil, errors.New("missing authorizer url")
	}

	if c.AuthConfig.Audience == "" {
		return nil, errors.New("missing audience identifer")
	}

	ctx := oidc.InsecureIssuerURLContext(context.Background(), c.AuthConfig.Authorizer)
	provider, err := oidc.NewProvider(ctx, c.AuthConfig.Authorizer)
	if err != nil {
		return nil, err
	}

	params := url.Values{"audience": []string{c.AuthConfig.Audience}}
	cc := &clientcredentials.Config{
		ClientID:       c.AuthConfig.ClientID,
		ClientSecret:   c.AuthConfig.ClientSecret,
		TokenURL:       provider.Endpoint().TokenURL,
		EndpointParams: params,
	}

	return cc.Token(ctx)
}

func (c *Client) isTokenValid() bool {
	if c.AuthToken == nil {
		return false
	}

	return c.AuthToken.Valid()
}
