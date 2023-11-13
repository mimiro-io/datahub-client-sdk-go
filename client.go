package main

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/coreos/go-oidc/v3/oidc"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// Dataset structure

type Query struct {
}

type EntityIterator interface {
	// Next returns the next available entity or nil if no more entities are available
	Next() *egdm.Entity

	// Token returns a continuation token that can be used to resume the iteration.
	Token() string
}

type AuthType int

const (
	// used for connecting to unsercured datahub instances
	AuthTypeNone AuthType = iota
	// used for connecting as admin user with username and password
	AuthTypeBasic
	// used for OAuth flow with client key and secret
	AuthTypeClientKeyAndSecret
	// Used for OAuth flow with signed JWT authentication request
	AuthTypePublicKey
	// AuthType User uses the OAuth User flow
	AuthTypeUser
)

type AuthConfig struct {
	AuthType     AuthType
	Authorizer   string
	ClientID     string
	ClientSecret string
	Audience     string
	PrivateKey   *rsa.PrivateKey
}

type Client struct {
	AuthConfig *AuthConfig
	AuthToken  *oauth2.Token
	Server     string
}

func NewClient() *Client {
	client := &Client{}
	client.AuthConfig = &AuthConfig{
		AuthType: AuthTypeNone,
	}
	return client
}

func (c *Client) WithServer(server string) *Client {
	c.Server = server
	return c
}

func (c *Client) WithExistingToken(token *oauth2.Token) *Client {
	c.AuthToken = token
	return c
}

func (c *Client) WithAdminAuth(datahubEndpoint string, username string, password string) *Client {
	c.AuthConfig = &AuthConfig{
		AuthType:     AuthTypeBasic,
		ClientID:     username,
		ClientSecret: password,
		Authorizer:   datahubEndpoint,
	}
	return c
}

func (c *Client) WithClientKeyAndSecretAuth(authorizer string, audience string, clientKey string, clientSecret string) *Client {
	c.AuthConfig = &AuthConfig{
		AuthType:     AuthTypeClientKeyAndSecret,
		ClientID:     clientKey,
		ClientSecret: clientSecret,
		Authorizer:   authorizer,
		Audience:     audience,
	}
	return c
}

// WithPublicKeyAuth sets the authentication type to public key authentication
// and sets the client id, audience and private key
func (c *Client) WithPublicKeyAuth(clientID string, audience string, privateKey *rsa.PrivateKey) *Client {
	c.AuthConfig = &AuthConfig{
		AuthType:   AuthTypePublicKey,
		ClientID:   clientID,
		Audience:   audience,
		PrivateKey: privateKey,
	}
	return c
}

func (c *Client) WithUserAuth(authorizer string, audience string) *Client {
	c.AuthConfig = &AuthConfig{
		AuthType:   AuthTypeUser,
		Audience:   audience,
		Authorizer: authorizer,
	}
	return c
}

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

func (c *Client) Authenticate() error {
	if c.isTokenValid() {
		return nil
	}

	// if no token, get one
	if c.AuthConfig.AuthType == AuthTypeClientKeyAndSecret {
		token, err := c.authenticateWithClientCredentials()
		if err != nil {
			return err
		}
		c.AuthToken = token
	} else if c.AuthConfig.AuthType == AuthTypePublicKey {
		token, err := c.authenticateWithCertificate()
		if err != nil {
			return err
		}
		c.AuthToken = token
	} else if c.AuthConfig.AuthType == AuthTypeUser {
		token, err := c.authenticateWithUserFlow()
		if err != nil {
			return err
		}
		c.AuthToken = token
	} else if c.AuthConfig.AuthType == AuthTypeBasic {
		token, err := c.authenticateWithBasicAuth()
		if err != nil {
			return err
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

func (c *Client) GenerateKeypair() (*rsa.PrivateKey, *rsa.PublicKey, error) {
	private, public, err := generateRsaKeyPair()
	if err != nil {
		return nil, nil, err
	}
	return private, public, nil
}

func (c *Client) LoadKeypair(location string) (*rsa.PrivateKey, *rsa.PublicKey, error) {
	var privateKey *rsa.PrivateKey
	privateKeyFilename := location + string(os.PathSeparator) + "node_key"
	_, err := os.Stat(privateKeyFilename)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
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

func (c *Client) RunQuery(query *Query) map[string]any {
	return nil
}
