package datahub

import (
	"encoding/json"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type Transaction struct {
	NamespaceManager *egdm.NamespaceContext
	DatasetEntities  map[string][]*egdm.Entity
}

func (t *Transaction) toGenericStructure() map[string]any {
	representation := make(map[string]any)
	representation["@context"] = map[string]any{"namespaces": t.NamespaceManager.AsContext().Namespaces}

	for k, v := range t.DatasetEntities {
		representation[k] = v
	}

	return representation
}

// NewTransaction creates a new transaction
// initialize the transaction with a namespace manage that will be used to generate prefixed URIs
func NewTransaction() *Transaction {
	return &Transaction{
		NamespaceManager: egdm.NewNamespaceContext(),
		DatasetEntities:  make(map[string][]*egdm.Entity),
	}
}

// ProcessTransaction sends a transaction to the datahub
// returns a ParameterError if the transaction is nil or cannot be serialiased
// returns an AuthenticationError if the client is not authenticated
// returns a RequestError if the transaction could not be processed
// Example usage: (error handling omitted for brevity)
//
//		txn := NewTransaction()
//		entityId, err := txn.NamespaceManager.AssertPrefixFromURI("http://data.example.io/entity1")
//		entity := egdm.NewEntity().SetID(entityId)
//		txn.DatasetEntities[datasetId1] = append(txn.DatasetEntities[datasetId1], entity)
//		err = client.ProcessTransaction(txn)
//	 	create another entity
//	 	entityId2, err := txn.NamespaceManager.AssertPrefixFromURI("http://data.example.io/entity2")
//	 	entity2 := egdm.NewEntity().SetID(entityId2)
//	 	txn.DatasetEntities[datasetId2] = append(txn.DatasetEntities[datasetId2], entity2)
//	 	err = client.ProcessTransaction(txn)
func (c *Client) ProcessTransaction(transaction *Transaction) error {
	if transaction == nil {
		return &ParameterError{Msg: "transaction cannot be nil"}
	}

	if len(transaction.DatasetEntities) == 0 {
		return &ParameterError{Msg: "transaction must contain at least one dataset"}
	}

	data, err := json.Marshal(transaction.toGenericStructure())
	if err != nil {
		return &ParameterError{Msg: "transaction could not be serialized"}
	}

	err = c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/transactions", data, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to process transaction", Err: err}
	}

	return nil
}
