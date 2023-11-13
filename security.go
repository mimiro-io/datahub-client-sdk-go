package main

import (
	"crypto/rsa"
	"encoding/json"
)

type AccessControl struct {
	Resource string
	Action   string
	Deny     bool
}

type ClientInfo struct {
	ClientId  string
	PublicKey []byte
	Deleted   bool
}

func (c *Client) ListClients() (map[string]ClientInfo, error) {
	err := c.checkToken()
	if err != nil {
		return nil, err
	}

	client := NewHttpClient()
	data, err := client.makeRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/security/clients", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	clients := make(map[string]ClientInfo)
	err = json.Unmarshal(data, &clients)
	if err != nil {
		return nil, err
	}

	return clients, nil
}

func (c *Client) AddClient(clientID string, publicKey *rsa.PublicKey) error {
	err := c.checkToken()
	if err != nil {
		return err
	}

	clientInfo := &ClientInfo{}
	clientInfo.ClientId = clientID
	publicKeyBytes, err := exportRsaPublicKeyAsPem(publicKey)
	if err != nil {
		return err
	}
	clientInfo.PublicKey = publicKeyBytes
	jsonData, err := json.Marshal(clientInfo)
	if err != nil {
		return err
	}

	client := NewHttpClient()
	_, err = client.makeRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, "/security/clients", jsonData, nil, nil)

	return err
}

func (c *Client) DeleteClient(id string) error {
	err := c.checkToken()
	if err != nil {
		return err
	}

	clientInfo := &ClientInfo{}
	clientInfo.ClientId = id
	clientInfo.Deleted = true
	jsonData, err := json.Marshal(clientInfo)
	if err != nil {
		return err
	}

	client := NewHttpClient()
	_, err = client.makeRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, "/security/clients", jsonData, nil, nil)

	return err
}

func (c *Client) SetClientAcl(id string, acls []AccessControl) error {
	err := c.checkToken()
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(acls)
	if err != nil {
		return err
	}

	client := NewHttpClient()
	_, err = client.makeRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, "/security/clients/"+id+"/acl", jsonData, nil, nil)

	return err
}

func (c *Client) GetClientAcl(id string) ([]AccessControl, error) {
	err := c.checkToken()
	if err != nil {
		return nil, err
	}

	client := NewHttpClient()
	data, err := client.makeRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/security/clients/"+id+"/acl", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	acls := make([]AccessControl, 0)
	err = json.Unmarshal(data, &acls)
	if err != nil {
		return nil, err
	}

	return acls, nil
}

type ValueReader struct {
	Type  string `json:"type"`
	Value string `json:"value"`
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

func (c *Client) AddTokenProvider(tokenProviderConfig ProviderConfig) error {
	err := c.checkToken()
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(tokenProviderConfig)
	if err != nil {
		return err

	}

	client := NewHttpClient()
	_, err = client.makeRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, "/provider/logins", jsonData, nil, nil)

	return err
}

func (c *Client) ListTokenProviders() ([]ProviderConfig, error) {
	err := c.checkToken()
	if err != nil {
		return nil, err
	}

	client := NewHttpClient()
	data, err := client.makeRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/provider/logins", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	providers := make([]ProviderConfig, 0)
	err = json.Unmarshal(data, &providers)
	if err != nil {
		return nil, err
	}

	return providers, nil
}
