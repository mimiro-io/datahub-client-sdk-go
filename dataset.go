package main

import (
	"encoding/json"
	"errors"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io"
	"strconv"
)

func (c *Client) GetDataset(name string) (*Dataset, error) {
	if name == "" {
		return nil, errors.New("dataset name is required")
	}

	err := c.CheckToken()
	if err != nil {
		return nil, err
	}

	client := NewHttpClient()
	data, err := client.makeRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/datasets/"+name, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	datasetEntity := &egdm.Entity{}
	if err := json.Unmarshal(data, datasetEntity); err != nil {
		return nil, err
	}

	dataset := &Dataset{}
	dataset.Name = datasetEntity.Properties["ns0:name"].(string)

	return dataset, nil
}

func (c *Client) GetDatasetEntity(name string) (*egdm.Entity, error) {
	if name == "" {
		return nil, errors.New("dataset name is required")
	}

	err := c.CheckToken()
	if err != nil {
		return nil, err
	}

	client := NewHttpClient()
	data, err := client.makeRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/datasets/"+name, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	datasetEntity := &egdm.Entity{}
	if err := json.Unmarshal(data, datasetEntity); err != nil {
		return nil, err
	}

	return datasetEntity, nil
}

func (c *Client) PutDatasetEntity(dataset string, datasetEntity *egdm.Entity) error {
	if dataset == "" {
		return errors.New("dataset name is required")
	}

	if datasetEntity == nil {
		return errors.New("dataset entity is required")
	}

	err := c.CheckToken()
	if err != nil {
		return err
	}

	client := NewHttpClient()
	data, err := json.Marshal(datasetEntity)
	if err != nil {
		return err
	}

	_, err = client.makeRequest(HTTP_PUT, c.AuthToken.AccessToken, c.Server, "/datasets/"+dataset, data, nil, nil)
	return err
}

type ProxyDatasetConfig struct {
	RemoteUrl        string `json:"remoteUrl"`
	AuthProviderName string `json:"authProviderName"`
}
type CreateDatasetConfig struct {
	ProxyDatasetConfig *ProxyDatasetConfig `json:"ProxyDatasetConfig"`
	PublicNamespaces   []string            `json:"publicNamespaces"`
}

// CreateDataset creates a dataset if it does not exist, or updates the namespaces if it does.
// returns an error if the dataset could not be created or updated.
func (c *Client) CreateDataset(name string, namespaces []string) error {
	if name == "" {
		return errors.New("dataset name is required")
	}

	err := c.CheckToken()
	if err != nil {
		return err
	}

	conf := &CreateDatasetConfig{}
	conf.PublicNamespaces = namespaces

	var b []byte
	if len(conf.PublicNamespaces) > 0 {
		b, err = json.Marshal(conf)
		if err != nil {
			return err
		}
	}

	path := "/datasets/" + name

	client := NewHttpClient()
	_, err = client.makeRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, path, b, nil, nil)
	return err
}

// AssertProxyDataset creates a proxy dataset if it does not exist, or updates the namespaces, remoteDatasetURL and
// authProviderName if it does. returns an error if the dataset could not be created or updated.
func (c *Client) AssertProxyDataset(name string, namespaces []string, remoteDatasetURL string, authProviderName string) error {
	err := c.CheckToken()
	if err != nil {
		return err
	}

	conf := &CreateDatasetConfig{}
	conf.PublicNamespaces = namespaces
	conf.ProxyDatasetConfig = &ProxyDatasetConfig{
		RemoteUrl:        remoteDatasetURL,
		AuthProviderName: authProviderName,
	}

	var b []byte
	b, err = json.Marshal(conf)
	if err != nil {
		return err
	}

	path := "/datasets/" + name
	queryParams := map[string]string{"proxy": "true"}

	client := NewHttpClient()
	_, err = client.makeRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, path, b, nil, queryParams)
	return err
}

func (c *Client) DeleteDataset(dataset string) error {
	if dataset == "" {
		return errors.New("dataset name is required")
	}

	err := c.CheckToken()
	if err != nil {
		return err
	}

	client := NewHttpClient()
	_, err = client.makeRequest(HTTP_DELETE, c.AuthToken.AccessToken, c.Server, "/datasets/"+dataset, nil, nil, nil)

	return err
}

func (c *Client) GetChanges(dataset string, since string, take int, latestOnly bool, reverse bool, expandURIs bool) (*egdm.EntityCollection, error) {
	err := c.CheckToken()
	if err != nil {
		return nil, err
	}

	params := map[string]string{}
	if since != "" {
		params["since"] = since
	}

	if take > 0 {
		params["limit"] = strconv.Itoa(take)
	}

	if latestOnly {
		params["latestOnly"] = "true"
	}

	if reverse {
		params["reverse"] = "true"
	}

	client := NewHttpClient()
	data, err := client.makeStreamingRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/datasets/"+dataset+"/changes", nil, nil, params)
	if err != nil {
		return nil, err
	}
	defer data.Close()

	nsManager := egdm.NewNamespaceContext()
	parser := egdm.NewEntityParser(nsManager)
	if expandURIs {
		parser = parser.WithExpandURIs()
	}
	entityCollection, err := parser.LoadEntityCollection(data)
	if err != nil {
		return nil, err
	}

	return entityCollection, nil
}

func (c *Client) GetEntities(dataset string, from string, take int, reverse bool, expandURIs bool) (*egdm.EntityCollection, error) {
	err := c.CheckToken()
	if err != nil {
		return nil, err
	}

	params := map[string]string{}
	if from != "" {
		params["from"] = from
	}

	if take > 0 {
		params["limit"] = strconv.Itoa(take)
	}

	if reverse {
		params["reverse"] = "true"
	}

	client := NewHttpClient()
	data, err := client.makeStreamingRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/datasets/"+dataset+"/entities", nil, nil, params)
	if err != nil {
		return nil, err
	}
	defer data.Close()

	nsManager := egdm.NewNamespaceContext()
	parser := egdm.NewEntityParser(nsManager)
	if expandURIs {
		parser = parser.WithExpandURIs()
	}
	entityCollection, err := parser.LoadEntityCollection(data)
	if err != nil {
		return nil, err
	}

	return entityCollection, nil
}

func (c *Client) GetDatasets() ([]*Dataset, error) {
	err := c.CheckToken()
	if err != nil {
		return nil, err
	}

	client := NewHttpClient()
	data, err := client.makeRequest(HTTP_GET, c.AuthToken.AccessToken, c.Server, "/datasets", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	datasets := make([]*Dataset, 0)
	if err := json.Unmarshal(data, &datasets); err != nil {
		return nil, err
	}

	return datasets, nil
}

func (c *Client) StoreEntities(dataset string, entityCollection *egdm.EntityCollection) error {
	err := c.CheckToken()
	if err != nil {
		return err
	}

	client := NewHttpClient()
	reader, err := client.makeStreamingWriterRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, "/datasets/"+dataset+"/entities", entityCollection.WriteEntityGraphJSON, nil, nil)
	if err != nil {
		return err
	}

	return reader.Close()
}

func (c *Client) StoreEntityStream(dataset string, data io.Reader) error {
	err := c.CheckToken()
	if err != nil {
		return err
	}

	writerFunc := func(writer io.Writer) error {
		// write the empty context as we expand all URIs
		ctx := egdm.NewContext()
		contextJson, _ := json.Marshal(ctx)
		_, err = writer.Write(contextJson)
		if err != nil {
			return err
		}

		// create entity parser and read from data stream
		entityParser := egdm.NewEntityParser(nil).WithExpandURIs()
		err := entityParser.Parse(data,
			func(entity *egdm.Entity) error {
				entityJson, _ := json.Marshal(entity)
				_, err = writer.Write(entityJson)
				if err != nil {
					return err
				}
				return nil
			},
			nil)
		return err
	}

	client := NewHttpClient()
	reader, err := client.makeStreamingWriterRequest(HTTP_POST, c.AuthToken.AccessToken, c.Server, "/datasets/"+dataset+"/entities", writerFunc, nil, nil)
	if err != nil {
		return err
	}

	return reader.Close()
}
