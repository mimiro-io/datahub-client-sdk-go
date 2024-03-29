package datahub

import (
	"encoding/base64"
	"github.com/google/uuid"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"testing"
)

func TestJavascriptQuery(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	javascriptQuery := `function do_query() {
							WriteQueryResult({key1: "value1"});
							WriteQueryResult({key1: "value2"});
							WriteQueryResult({key1: "value3"});
						}`

	// base64 encode the query
	javascriptQuery = base64.StdEncoding.EncodeToString([]byte(javascriptQuery))

	results, err := client.RunJavascriptQuery(javascriptQuery)
	if err != nil {
		t.Error(err)
	}

	result, err := results.Next()
	if err != nil {
		t.Error(err)
	}

	if result["key1"] != "value1" {
		t.Errorf("expected result to be 'value1', got '%s'", result["key1"])
	}

	result, err = results.Next()
	if err != nil {
		t.Error(err)
	}

	if result["key1"] != "value2" {
		t.Errorf("expected result to be 'value2', got '%s'", result["key1"])
	}

	result, err = results.Next()
	if err != nil {
		t.Error(err)
	}

	if result["key1"] != "value3" {
		t.Errorf("expected result to be 'value3', got '%s'", result["key1"])
	}

	// check no more
	result, err = results.Next()
	if err != nil {
		t.Error(err)
	}

	if result != nil {
		t.Errorf("expected no more results")
	}

	result, err = results.Next()
	if err != nil {
		t.Error(err)
	}

	if result != nil {
		t.Errorf("expected no more results")
	}

	err = results.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestQueryForEntityById(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	datasetName := "test-" + uuid.New().String()

	err := client.AddDataset(datasetName, nil)
	if err != nil {
		t.Error(err)
	}

	// make entity collection
	namespaceManager := egdm.NewNamespaceContext()
	prefixedId, err := namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/entity1")
	ec := egdm.NewEntityCollection(namespaceManager)
	entity := egdm.NewEntity().SetID(prefixedId)
	err = ec.AddEntity(entity)
	if err != nil {
		t.Error(err)
	}

	// store entities
	err = client.StoreEntities(datasetName, ec)
	if err != nil {
		t.Error(err)
	}

	qb := NewQueryBuilder()
	qb.WithEntityId("http://data.example.com/things/entity1")
	query := qb.Build()

	results, err := client.RunQuery(query)
	if err != nil {
		t.Error(err)
	}

	if results == nil {
		t.Error("expected results")
	}
}

func TestForRelatedEntities(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	datasetName := "test-" + uuid.New().String()

	err := client.AddDataset(datasetName, nil)
	if err != nil {
		t.Error(err)
	}

	// make entity collection
	namespaceManager := egdm.NewNamespaceContext()
	prefixedId, err := namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/entity1")
	ec := egdm.NewEntityCollection(namespaceManager)
	entity := egdm.NewEntity().SetID(prefixedId)
	entity.SetReference("http://data.example.com/things/related", "http://data.example.com/things/entity2")
	err = ec.AddEntity(entity)
	if err != nil {
		t.Error(err)
	}

	// store entities
	err = client.StoreEntities(datasetName, ec)
	if err != nil {
		t.Error(err)
	}

	qb := NewQueryBuilder()
	qb.WithEntityId("http://data.example.com/things/entity2")
	qb.WithInverse(true)
	qb.WithDatasets([]string{datasetName})
	query := qb.Build()

	results, err := client.RunQuery(query)
	if err != nil {
		t.Error(err)
	}

	if results == nil {
		t.Error("expected results")
	}
}

func TestStreamResultForHop(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	datasetName := "test-" + uuid.New().String()

	err := client.AddDataset(datasetName, nil)
	if err != nil {
		t.Error(err)
	}

	// make entity collection
	namespaceManager := egdm.NewNamespaceContext()
	ec := egdm.NewEntityCollection(namespaceManager)

	prefixedId, err := namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/entity1")
	entity := egdm.NewEntity().SetID(prefixedId)
	entity.SetReference("http://data.example.com/things/related", "http://data.example.com/things/entity3")
	err = ec.AddEntity(entity)
	if err != nil {
		t.Error(err)
	}

	prefixedId, err = namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/entity2")
	entity = egdm.NewEntity().SetID(prefixedId)
	entity.SetReference("http://data.example.com/things/related", "http://data.example.com/things/entity3")
	err = ec.AddEntity(entity)
	if err != nil {
		t.Error(err)
	}

	// store entities
	err = client.StoreEntities(datasetName, ec)
	if err != nil {
		t.Error(err)
	}

	stream, err := client.RunHopQuery("http://data.example.com/things/entity3", "http://data.example.com/things/related", []string{datasetName}, true, 1)
	if err != nil {
		t.Error(err)
	}

	e1, err := stream.Next()
	if err != nil {
		t.Error(err)
	}

	if e1.ID != "http://data.example.com/things/entity1" {
		t.Errorf("expected entity id to be 'ns0:entity1', got '%s'", e1.ID)
	}

	e2, err := stream.Next()
	if err != nil {
		t.Error(err)
	}
	if e2.ID != "http://data.example.com/things/entity2" {
		t.Errorf("expected entity id to be 'ns0:entity2', got '%s'", e2.ID)
	}

	e3, err := stream.Next()
	if err != nil {
		t.Error(err)
	}
	if e3 != nil {
		t.Errorf("expected entity to be nil, got '%s'", e3.ID)
	}
}
