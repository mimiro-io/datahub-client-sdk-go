package datahub

import (
	"encoding/json"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io"
)

// QueryResultIterator is used to iterate over the results of a javascript query.
type QueryResultIterator struct {
	dataStream io.ReadCloser
	decoder    *json.Decoder
	readStart  bool
}

func newQueryResultIterator(dataStream io.ReadCloser) *QueryResultIterator {
	qri := &QueryResultIterator{dataStream: dataStream}
	qri.decoder = json.NewDecoder(dataStream)
	return qri
}

// Next returns the next object in the query result iterator.
// returns a ClientProcessingError if there is an issue decoding the data stream.
// returns nil if there are no more objects.
// returns the object if there are no errors.
func (qri *QueryResultIterator) Next() (map[string]interface{}, error) {
	var err error
	if !qri.readStart {
		token, err := qri.decoder.Token()
		if err != nil {
			return nil, &ClientProcessingError{Msg: "unable to decode start of data stream", Err: err}
		}
		if token != json.Delim('[') {
			return nil, &ClientProcessingError{Msg: "expected [ at start of data stream", Err: nil}
		}
		qri.readStart = true
	}

	if qri.decoder.More() {
		var obj map[string]interface{}
		err = qri.decoder.Decode(&obj)
		if err != nil {
			return nil, &ClientProcessingError{Msg: "unable to decode data stream", Err: err}
		}
		return obj, nil
	}

	// No error, return the object
	return nil, nil
}

// Close closes the query result iterator. This must be called when the iterator is no longer needed.
// returns a ClientProcessingError if there is an issue closing the data stream.
func (qri *QueryResultIterator) Close() error {
	err := qri.dataStream.Close()
	if err != nil {
		return &ClientProcessingError{Msg: "unable to close data stream", Err: err}
	}
	return nil
}

// RunJavascriptQuery executes a javascript query on the server.
// The query is a base64 encoded string of the javascript code to execute.
// returns a QueryResultIterator that can be used to iterate over the results.
// returns an AuthenticationError if the client is not authenticated.
// returns a ParameterError if the query is empty.
// returns a RequestError if there is an issue executing the query.
func (c *Client) RunJavascriptQuery(query string) (*QueryResultIterator, error) {
	if query == "" {
		return nil, &ParameterError{Msg: "query cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	queryObject := map[string]string{"query": query}
	queryBytes, err := json.Marshal(queryObject)

	client := c.makeHttpClient()
	headers := make(map[string]string)
	headers["Content-Type"] = "application/x-javascript-query"
	data, err := client.makeStreamingRequest(httpPost, "/query", queryBytes, headers, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to execute query", Err: err}
	}

	return newQueryResultIterator(data), nil
}

type Query struct {
	EntityID         string   `json:"entityId"`
	StartingEntities []string `json:"startingEntities"`
	Predicate        string   `json:"predicate"`
	Inverse          bool     `json:"inverse"`
	Datasets         []string `json:"datasets"`
	Details          bool     `json:"details"`
	Limit            int      `json:"limit"`
	Continuations    []string `json:"continuations"`
	NoPartialMerging bool     `json:"noPartialMerging"`
}

type QueryBuilder struct {
	query *Query
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{query: &Query{}}
}

func (qb *QueryBuilder) WithEntityId(entityId string) *QueryBuilder {
	qb.query.EntityID = entityId
	return qb
}

func (qb *QueryBuilder) WithStartingEntities(startingEntities []string) *QueryBuilder {
	qb.query.StartingEntities = startingEntities
	return qb
}

func (qb *QueryBuilder) WithPredicate(predicate string) *QueryBuilder {
	qb.query.Predicate = predicate
	return qb
}

func (qb *QueryBuilder) WithInverse(inverse bool) *QueryBuilder {
	qb.query.Inverse = inverse
	return qb
}

func (qb *QueryBuilder) WithDatasets(datasets []string) *QueryBuilder {
	qb.query.Datasets = datasets
	return qb
}

func (qb *QueryBuilder) WithDetails(details bool) *QueryBuilder {
	qb.query.Details = details
	return qb
}

func (qb *QueryBuilder) WithLimit(limit int) *QueryBuilder {
	qb.query.Limit = limit
	return qb
}

func (qb *QueryBuilder) WithContinuations(continuations []string) *QueryBuilder {
	qb.query.Continuations = continuations
	return qb
}

func (qb *QueryBuilder) WithNoPartialMerging(noPartialMerging bool) *QueryBuilder {
	qb.query.NoPartialMerging = noPartialMerging
	return qb
}

func (qb *QueryBuilder) Build() *Query {
	return qb.query
}

type QueryResultEntitiesStream struct {
	client            *Client
	currentCollection *egdm.EntityCollection
	currentPos        int
}

func (c *Client) RunHopQuery(entityId string, predicate string, datasets []string, inverse bool, limit int) (EntityIterator, error) {
	qb := NewQueryBuilder()
	qb.query.StartingEntities = make([]string, 0)
	qb.query.StartingEntities = append(qb.query.StartingEntities, entityId)
	qb.WithInverse(inverse)
	qb.WithLimit(limit)
	qb.WithPredicate(predicate)
	if datasets != nil {
		qb.WithDatasets(datasets)
	}
	return c.newQueryResultEntitiesStream(qb.Build())
}

func (c *Client) newQueryResultEntitiesStream(query *Query) (EntityIterator, error) {
	es := &QueryResultEntitiesStream{
		client:     c,
		currentPos: 0,
	}

	// load initial collection so that context is there
	var err error
	if err != nil {
		return nil, err
	}
	result, err := c.RunQuery(query)
	if err != nil {
		return nil, err
	}

	es.currentCollection, err = es.makeEntityCollectionFromQueryResult(result)
	if err != nil {
		return nil, err
	}

	return es, nil
}

func (e *QueryResultEntitiesStream) makeEntityCollectionFromQueryResult(data []any) (*egdm.EntityCollection, error) {
	context := data[0].(map[string]any)
	resultRows := data[1].([]any)
	continuation := data[2].([]any)

	ctx := egdm.NewNamespaceContext()

	namespacePrefixes := context["namespaces"].(map[string]any)
	for key, value := range namespacePrefixes {
		ctx.StorePrefixExpansionMapping(key, value.(string))
	}

	ec := egdm.NewEntityCollection(ctx)
	for _, row := range resultRows {
		ec.AddEntityFromMap(row.([]any)[2].(map[string]any))
	}
	err := ec.ExpandNamespacePrefixes()
	if err != nil {
		return nil, err
	}

	if len(continuation) == 1 {
		cont := egdm.NewContinuation()
		cont.Token = continuation[0].(string)
		ec.SetContinuationToken(cont)
	} else {
		ec.SetContinuationToken(nil)
	}

	return ec, nil
}

func (e *QueryResultEntitiesStream) Next() (*egdm.Entity, error) {
	if e.currentPos == len(e.currentCollection.Entities) {
		if e.currentCollection.Continuation == nil {
			return nil, nil
		}

		// query for next page with client
		token := e.currentCollection.Continuation.Token
		query := NewQueryBuilder().WithContinuations([]string{token}).Build()
		result, err := e.client.RunQuery(query)
		if err != nil {
			return nil, err
		}

		e.currentCollection, err = e.makeEntityCollectionFromQueryResult(result)
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

func (e *QueryResultEntitiesStream) Context() *egdm.Context {
	if e.currentCollection == nil {
		return nil
	}

	return e.currentCollection.NamespaceManager.AsContext()
}

func (e *QueryResultEntitiesStream) Token() *egdm.Continuation {
	if e.currentCollection == nil {
		return nil
	}

	return e.currentCollection.Continuation
}

func (c *Client) RunStreamingQuery(query *Query) (EntityIterator, error) {
	if len(query.StartingEntities) != 1 {
		return nil, &ParameterError{Msg: "query must have exactly one starting entity"}
	}

	if query.Predicate == "" {
		return nil, &ParameterError{Msg: "query must have a predicate"}
	}

	return c.newQueryResultEntitiesStream(query)
}

func (c *Client) RunQuery(query *Query) ([]any, error) {
	if query == nil {
		return nil, &ParameterError{Msg: "query cannot be nil"}
	}

	data, err := json.Marshal(query)
	if err != nil {
		return nil, &ParameterError{Msg: "unable to marshal query", Err: err}
	}

	err = c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	response, err := client.makeRequest(httpPost, "/query", data, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to execute query", Err: err}
	}

	result := make([]any, 0)
	err = json.Unmarshal(response, &result)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal query", Err: err}
	}

	return result, nil
}
