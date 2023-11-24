package datahub

import (
	"encoding/json"
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

func (c *Client) RunQuery(query *Query) ([]map[string]any, error) {
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

	result := make([]map[string]any, 0)
	err = json.Unmarshal(response, &result)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal job", Err: err}
	}

	return result, nil
}
