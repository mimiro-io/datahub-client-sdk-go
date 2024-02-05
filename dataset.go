package datahub

import (
	"encoding/json"
	"errors"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io"
	"strconv"
)

// Dataset represents a dataset in the data hub.
// Name is a unique identifier for the dataset for a given data hub instance.
// Metadata is a map of metadata properties for the dataset.
type Dataset struct {
	Name     string
	Metadata map[string]any
}

// proxyDatasetConfig represents the configuration for a proxy dataset.
// Sent as a data structure when creating a proxy dataset.
type proxyDatasetConfig struct {
	RemoteUrl        string `json:"remoteUrl"`
	AuthProviderName string `json:"authProviderName"`
}

// createDatasetConfig represents the configuration for a dataset.
// Sent as a data structure when creating a dataset.
type createDatasetConfig struct {
	ProxyDatasetConfig *proxyDatasetConfig `json:"proxyDatasetConfig"`
	PublicNamespaces   []string            `json:"publicNamespaces"`
}

// GetDataset gets a dataset by name.
// returns a dataset if it exists, or an error if it does not.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetDataset(name string) (*Dataset, error) {
	if name == "" {
		return nil, &ParameterError{Msg: "dataset name is required"}
	}

	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "invalid token or unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/datasets/"+name, nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get dataset", Err: err}
	}

	datasetEntity := &egdm.Entity{}
	if err := json.Unmarshal(data, datasetEntity); err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshall dataset entity", Err: err}
	}

	dataset := &Dataset{}

	// fixme: this is a fragile way to get the name of the dataset
	dataset.Name = datasetEntity.Properties["ns0:name"].(string)

	return dataset, nil
}

// GetDatasetEntity gets a dataset entity by name.
// returns an Entity if it exists, or an error if it does not.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetDatasetEntity(name string) (*egdm.Entity, error) {
	if name == "" {
		return nil, &ParameterError{Msg: "dataset name is required"}
	}

	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "invalid token or unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/datasets/"+name, nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get dataset entity", Err: err}
	}

	datasetEntity := &egdm.Entity{}
	if err := json.Unmarshal(data, datasetEntity); err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshall dataset entity", Err: err}
	}

	return datasetEntity, nil
}

// UpdateDatasetEntity updates the dataset entity for a named dataset.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty or the dataset entity is nil.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) UpdateDatasetEntity(dataset string, datasetEntity *egdm.Entity) error {
	if dataset == "" {
		return &ParameterError{Msg: "dataset name is required"}
	}

	if datasetEntity == nil {
		return &ParameterError{Msg: "dataset entity cannot be nil"}
	}

	data, err := json.Marshal(datasetEntity)
	if err != nil {
		return &ParameterError{Msg: "unable to serialise dataset entity", Err: err}
	}

	err = c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "invalid token or unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPut, "/datasets/"+dataset, data, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to update dataset entity", Err: err}
	}

	return err
}

// AddDataset creates a dataset if it does not exist.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) AddDataset(name string, namespaces []string) error {
	if name == "" {
		return &ParameterError{Msg: "dataset name is required"}
	}

	// default to
	if namespaces == nil {
		namespaces = make([]string, 0)
	}

	var err error

	conf := &createDatasetConfig{}
	conf.PublicNamespaces = namespaces
	var b []byte
	if len(conf.PublicNamespaces) > 0 {
		b, err = json.Marshal(conf)
		if err != nil {
			return &ParameterError{Msg: "unable to serialise create dataset config"}
		}
	}

	err = c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "invalid token or unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/datasets/"+name, b, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to create dataset", Err: err}
	}

	return nil
}

// AddProxyDataset creates a proxy dataset if it does not exist, or updates the namespaces, remoteDatasetURL and
// authProviderName if it does. returns an error if the dataset could not be created or updated.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) AddProxyDataset(name string, namespaces []string, remoteDatasetURL string, authProviderName string) error {
	var err error

	if name == "" {
		return &ParameterError{Msg: "dataset name is required"}
	}

	if remoteDatasetURL == "" {
		return &ParameterError{Msg: "remote dataset URL is required"}
	}

	conf := &createDatasetConfig{}
	conf.PublicNamespaces = namespaces
	conf.ProxyDatasetConfig = &proxyDatasetConfig{
		RemoteUrl:        remoteDatasetURL,
		AuthProviderName: authProviderName,
	}

	var b []byte
	b, err = json.Marshal(conf)
	if err != nil {
		return &ParameterError{Msg: "unable to serialise create dataset config"}
	}

	err = c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "invalid token or unable to authenticate", Err: err}
	}

	queryParams := map[string]string{"proxy": "true"}
	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/datasets/"+name, b, nil, queryParams)
	if err != nil {
		return &RequestError{Msg: "unable to create proxy dataset", Err: err}
	}

	return nil
}

// DeleteDataset deletes a named dataset.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) DeleteDataset(dataset string) error {
	if dataset == "" {
		return &ParameterError{Msg: "dataset name is required"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpDelete, "/datasets/"+dataset, nil, nil, nil)

	if err != nil {
		return &RequestError{Msg: "unable to delete dataset", Err: err}
	}

	return nil
}

// GetChanges gets changes for a dataset.
// returns an EntityCollection for the named dataset.
// since parameter is an optional token to get changes since.
// take parameter is an optional limit on the number of changes to return.
// latestOnly parameter is an optional flag to only return the latest version of each entity.
// reverse parameter is an optional flag to reverse the order of the changes.
// expandURIs parameter is an optional flag to expand Entity URIs in the response.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetChanges(dataset string, since string, take int, latestOnly bool, reverse bool, expandURIs bool) (*egdm.EntityCollection, error) {
	if dataset == "" {
		return nil, &ParameterError{Msg: "dataset name is required"}
	}

	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
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

	client := c.makeHttpClient()
	data, err := client.makeStreamingRequest(httpGet, "/datasets/"+dataset+"/changes", nil, nil, params)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get changes", Err: err}
	}
	defer data.Close()

	nsManager := egdm.NewNamespaceContext()
	parser := egdm.NewEntityParser(nsManager)
	if expandURIs {
		parser = parser.WithExpandURIs()
	}
	entityCollection, err := parser.LoadEntityCollection(data)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to parse changes", Err: err}
	}

	return entityCollection, nil
}

// GetChangesStream gets entities for a dataset as a stream from the since position defined.
// returns an EntityIterator over the changes for the named dataset.
// since parameter is an optional token to get changes since.
// take parameter is an optional limit on the number of changes to return in each batch.
// reverse parameter is an optional flag to reverse the order of the changes.
// latestOnly parameter is an optional flag to only return the latest version of each entity.
// expandURIs parameter is an optional flag to expand Entity URIs in the response.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetChangesStream(dataset string, since string, latestOnly bool, take int, reverse bool, expandURIs bool) (EntityIterator, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	stream, err := c.newChangesStream(dataset, since, latestOnly, take, reverse, expandURIs)
	return stream, err
}

// GetEntities gets entities for a dataset.
// returns an EntityCollection for the named dataset.
// from parameter is an optional token to get changes since.
// take parameter is an optional limit on the number of changes to return.
// reverse parameter is an optional flag to reverse the order of the changes.
// expandURIs parameter is an optional flag to expand Entity URIs in the response.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetEntities(dataset string, from string, take int, reverse bool, expandURIs bool) (*egdm.EntityCollection, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
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

	client := c.makeHttpClient()
	data, err := client.makeStreamingRequest(httpGet, "/datasets/"+dataset+"/entities", nil, nil, params)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get entities", Err: err}
	}
	defer data.Close()

	nsManager := egdm.NewNamespaceContext()
	parser := egdm.NewEntityParser(nsManager)
	if expandURIs {
		parser = parser.WithExpandURIs()
	}
	entityCollection, err := parser.LoadEntityCollection(data)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to parse entities", Err: err}
	}

	return entityCollection, nil
}

// GetEntitiesStream gets entities for a dataset as a stream from the start position defined.
// returns an EntityIterator over the entities in the named dataset.
// from parameter is an optional token to get changes since.
// take parameter is an optional limit on the number of changes to return.
// reverse parameter is an optional flag to reverse the order of the changes.
// expandURIs parameter is an optional flag to expand Entity URIs in the response.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetEntitiesStream(dataset string, from string, take int, reverse bool, expandURIs bool) (EntityIterator, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	stream, err := c.newEntitiesStream(dataset, from, take, reverse, expandURIs)
	return stream, err
}

type EntitiesStream struct {
	client            *Client
	currentCollection *egdm.EntityCollection
	startFrom         string
	take              int
	reverse           bool
	expandURIs        bool
	dataset           string
	currentPos        int
	nextBatch         func() (*egdm.EntityCollection, error)
}

func (c *Client) newChangesStream(dataset string, since string, latestOnly bool, take int, reverse bool, expandURIs bool) (EntityIterator, error) {
	es := &EntitiesStream{
		client:     c,
		startFrom:  since,
		take:       take,
		reverse:    reverse,
		expandURIs: expandURIs,
		dataset:    dataset,
	}

	// load initial collection so that context is there
	var err error
	es.currentCollection, err = es.client.GetChanges(es.dataset, es.startFrom, es.take, latestOnly, es.reverse, es.expandURIs)
	if err != nil {
		return nil, err
	}

	es.nextBatch = func() (*egdm.EntityCollection, error) {
		return es.client.GetChanges(es.dataset, es.currentCollection.Continuation.Token, es.take, latestOnly, es.reverse, es.expandURIs)
	}

	return es, nil
}

func (c *Client) newEntitiesStream(dataset string, from string, take int, reverse bool, expandURIs bool) (EntityIterator, error) {
	es := &EntitiesStream{
		client:     c,
		startFrom:  from,
		take:       take,
		reverse:    reverse,
		expandURIs: expandURIs,
		dataset:    dataset,
	}

	// load initial collection so that context is there
	var err error
	es.currentCollection, err = es.client.GetEntities(es.dataset, es.startFrom, es.take, es.reverse, es.expandURIs)
	if err != nil {
		return nil, err
	}

	es.nextBatch = func() (*egdm.EntityCollection, error) {
		return es.client.GetEntities(es.dataset, es.currentCollection.Continuation.Token, es.take, es.reverse, es.expandURIs)
	}

	return es, nil
}

func (e *EntitiesStream) Next() (*egdm.Entity, error) {
	var err error
	if e.currentPos == len(e.currentCollection.Entities) {
		// query for next page with client
		e.currentCollection, err = e.nextBatch() // e.client.GetEntities(e.dataset, e.currentCollection.Continuation.Token, e.take, e.reverse, e.expandURIs)
		if err != nil {
			return nil, err
		}
		e.currentPos = 0
	}

	// no more entities
	if len(e.currentCollection.Entities) == 0 {
		return nil, nil
	}

	entity := e.currentCollection.Entities[e.currentPos]
	e.currentPos++

	return entity, nil
}

func (e *EntitiesStream) Context() *egdm.Context {
	if e.currentCollection == nil {
		return nil
	}

	return e.currentCollection.NamespaceManager.AsContext()
}

func (e *EntitiesStream) Token() *egdm.Continuation {
	if e.currentCollection == nil {
		return nil
	}

	return e.currentCollection.Continuation
}

// GetDatasets gets list of datasets.
// returns []*Dataset for the named dataset.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetDatasets() ([]*Dataset, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/datasets", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get datasets", Err: err}
	}

	datasets := make([]*Dataset, 0)
	if err := json.Unmarshal(data, &datasets); err != nil {
		return nil, &ClientProcessingError{Msg: "unable to parse datasets", Err: err}
	}

	return datasets, nil
}

// StoreEntities stores the entities in a named dataset.
// dataset is the name of the dataset to be updated.
// entityCollection is the set of entities to store.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty or entityCollection is nil.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) StoreEntities(dataset string, entityCollection *egdm.EntityCollection) error {
	if dataset == "" {
		return &ParameterError{Msg: "dataset name is required"}
	}

	if entityCollection == nil {
		return &ParameterError{Msg: "entity collection cannot be nil"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	reader, err := client.makeStreamingWriterRequest(httpPost, "/datasets/"+dataset+"/entities", entityCollection.WriteEntityGraphJSON, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to store entities", Err: err}
	}

	return reader.Close()
}

// StoreEntityStream stores the entities in a named dataset.
// dataset is the name of the dataset to be updated.
// data is the stream of entities to store.
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the dataset name is empty or entityCollection is nil.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) StoreEntityStream(dataset string, data io.Reader) error {
	if dataset == "" {
		return &ParameterError{Msg: "dataset name is required"}
	}

	if data == nil {
		return &ParameterError{Msg: "data cannot be nil"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	writerFunc := func(writer io.Writer) error {
		// write the empty context as we expand all URIs
		ctx := egdm.NewContext()
		contextJson, _ := json.Marshal(ctx)
		_, err = writer.Write(contextJson)
		if err != nil {
			return errors.New("unable to write context")
		}

		// create entity parser and read from data stream
		entityParser := egdm.NewEntityParser(nil).WithExpandURIs()
		err := entityParser.Parse(data,
			func(entity *egdm.Entity) error {
				entityJson, _ := json.Marshal(entity)
				_, err = writer.Write(entityJson)
				if err != nil {
					return errors.New("unable to write entity")
				}
				return nil
			},
			nil)
		return err
	}

	client := c.makeHttpClient()
	reader, err := client.makeStreamingWriterRequest(httpPost, "/datasets/"+dataset+"/entities", writerFunc, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to store entities", Err: err}
	}

	return reader.Close()
}
