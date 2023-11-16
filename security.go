package datahub

import (
	"crypto/rsa"
	"encoding/json"
)

// AccessControl is a struct that represents a single access control rule for a single resource
type AccessControl struct {
	// Resource is a URL of the resource to which the access control rule applies
	Resource string
	// Action is the action that is allowed or denied. The value can be "read" or "write"
	Action string
	// Deny is a boolean value that indicates whether the action is allowed or denied
	Deny bool
}

// ClientInfo is a struct that represents a single client, including the client ID and public key
type ClientInfo struct {
	// ClientId is the unique ID of the client on the server
	ClientId string
	// PublicKey is the public key of the client
	PublicKey []byte
	// Deleted is a boolean value that indicates whether the client is deleted
	Deleted bool
}

// GetClients returns a map of client IDs to ClientInfo structs
// returns an AuthenticationError if the client is unable to authenticate.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetClients() (map[string]ClientInfo, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Err: err, Msg: "unable to authenticate"}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/security/clients", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get clients"}
	}

	clients := make(map[string]ClientInfo)
	err = json.Unmarshal(data, &clients)
	if err != nil {
		return nil, &ClientProcessingError{Err: err, Msg: "unable to process clients"}
	}

	return clients, nil
}

// AddClient stores the client ID and optional public key of a client.
// clientID is the unique id of the client to be added.
// publicKey is the client public key (optional).
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the clientID is empty
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) AddClient(clientID string, publicKey *rsa.PublicKey) error {
	if clientID == "" {
		return &ParameterError{Msg: "clientID cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Err: err, Msg: "unable to authenticate"}
	}

	clientInfo := &ClientInfo{}
	clientInfo.ClientId = clientID
	if publicKey == nil {
		publicKeyBytes, err := exportRsaPublicKeyAsPem(publicKey)
		if err != nil {
			return err
		}
		clientInfo.PublicKey = publicKeyBytes
	}

	jsonData, err := json.Marshal(clientInfo)
	if err != nil {
		return &ParameterError{Msg: "unable to marshal client info"}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/security/clients", jsonData, nil, nil)

	if err != nil {
		return &RequestError{Msg: "unable to add client"}
	}

	return nil
}

// DeleteClient deletes the specific client.
// clientID is the unique id of the client to be added.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the clientID is empty
// returns a RequestError if the request fails.
func (c *Client) DeleteClient(id string) error {
	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Err: err, Msg: "unable to authenticate"}
	}

	clientInfo := &ClientInfo{}
	clientInfo.ClientId = id
	clientInfo.Deleted = true
	jsonData, err := json.Marshal(clientInfo)
	if err != nil {
		return &ParameterError{Msg: "unable to marshal client info"}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/security/clients", jsonData, nil, nil)

	if err != nil {
		return &RequestError{Msg: "unable to delete client"}
	}

	return nil
}

// SetClientAcl sets the access control rules for the specified client.
// clientID is the unique id of the client to be added.
// acls is a slice of AccessControl structs that represent the access control rules to be set.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the clientID is empty
// returns a RequestError if the request fails.
func (c *Client) SetClientAcl(clientID string, acls []AccessControl) error {
	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Err: err, Msg: "unable to authenticate"}
	}

	jsonData, err := json.Marshal(acls)
	if err != nil {
		return &ParameterError{Msg: "unable to marshal access control list"}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/security/clients/"+clientID+"/acl", jsonData, nil, nil)

	if err != nil {
		return &RequestError{Msg: "unable to set client access control list"}
	}

	return nil
}

// GetClientAcl returns the access control rules for the specified client.
// clientID is the unique id of the client to be added.
// returns a slice of AccessControl structs that represent the access control rules.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the clientID is empty
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetClientAcl(clientID string) ([]AccessControl, error) {
	if clientID == "" {
		return nil, &ParameterError{Msg: "clientID cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Err: err, Msg: "unable to authenticate"}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/security/clients/"+clientID+"/acl", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get client access control list"}
	}

	acls := make([]AccessControl, 0)
	err = json.Unmarshal(data, &acls)
	if err != nil {
		return nil, err
	}

	return acls, nil
}

type ProviderConfig struct {
	Name         string       `json:"name"`
	Type         string       `json:"type"`
	User         *ValueReader `json:"user,omitempty"`
	Password     *ValueReader `json:"password,omitempty"`
	ClientId     *ValueReader `json:"key,omitempty"`
	ClientSecret *ValueReader `json:"secret,omitempty"`
	Audience     *ValueReader `json:"audience,omitempty"`
	GrantType    *ValueReader `json:"grantType,omitempty"`
	Endpoint     *ValueReader `json:"endpoint,omitempty"`
}

type ValueReader struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// AddTokenProvider returns the access control rules for the specified client.
// tokenProviderConfig is a single token provider configuration to be added.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the tokenProviderConfig is nil
// returns a RequestError if the request fails.
func (c *Client) AddTokenProvider(tokenProviderConfig *ProviderConfig) error {
	if tokenProviderConfig == nil {
		return &ParameterError{Msg: "tokenProviderConfig cannot be nil"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Err: err, Msg: "unable to authenticate"}
	}

	jsonData, err := json.Marshal(tokenProviderConfig)
	if err != nil {
		return &ParameterError{Msg: "unable to marshal token provider config"}

	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/provider/logins", jsonData, nil, nil)

	if err != nil {
		return &RequestError{Msg: "unable to add token provider"}
	}

	return nil
}

// GetTokenProviders returns a slice of ProviderConfig structs.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the tokenProviderConfig is nil
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetTokenProviders() ([]*ProviderConfig, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Err: err, Msg: "unable to authenticate"}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/provider/logins", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get token providers"}
	}

	providers := make([]*ProviderConfig, 0)
	err = json.Unmarshal(data, &providers)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to process token providers"}
	}

	return providers, nil
}
