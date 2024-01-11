package datahub

import (
	"github.com/google/uuid"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"strings"
	"testing"
)

func NewAdminUserConfiguredClient() *Client {
	testConfig := getTestConfig()
	client, _ := NewClient(testConfig.DataHubUrl)
	client.WithAdminAuth(testConfig.AdminUser, testConfig.AdminKey)
	return client
}

func TestGetDatasets(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	datasets, err := client.GetDatasets()
	if err != nil {
		t.Error(err)
	}
	if len(datasets) == 0 {
		t.Error("expected datasets to be populated")
	}
}

func TestGetDatasetEntity(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	entity, err := client.GetDatasetEntity("core.Dataset")
	if err != nil {
		t.Error(err)
	}
	if entity.ID != "ns0:core.Dataset" {
		t.Errorf("expected dataset entity id to be 'ns0:core.Dataset', got '%s'", entity.ID)
	}
}

func TestGetEntities(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	ec, err := client.GetEntities("core.Dataset", "", -1, false, false)
	if err != nil {
		t.Error(err)
	}
	if len(ec.Entities) == 0 {
		t.Error("expected entities to be populated")
	}
}

func TestGetDataset(t *testing.T) {
	client := NewAdminUserConfiguredClient()
	dataset, err := client.GetDataset("core.Dataset")
	if err != nil {
		t.Error(err)
	}
	if dataset.Name != "core.Dataset" {
		t.Errorf("expected dataset entity id to be 'core.Dataset', got '%s'", dataset.Name)
	}
}

func TestAssertDataset(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// make dateset name from test+ a guid
	datasetName := "test-" + uuid.New().String()

	err := client.AddDataset(datasetName, nil)
	if err != nil {
		t.Error(err)
	}

	// get dataset by name
	entity, err := client.GetDatasetEntity(datasetName)
	if err != nil {
		t.Error(err)
	}

	if !strings.Contains(entity.ID, datasetName) {
		t.Errorf("expected dataset entity id to be '%s', got '%s'", datasetName, entity.ID)
	}
}

func TestStoreEntities(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// make dateset name from test+ a guid
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

	// get entities
	ec2, err := client.GetEntities(datasetName, "", -1, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(ec2.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(ec2.Entities))
	}

	if ec2.Entities[0].ID != "http://data.example.com/things/entity1" {
		t.Errorf("expected entity id to be 'ns0:entity1', got '%s'", ec2.Entities[0].ID)
	}
}

func TestGetEntitiesStream(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// make dateset name from test+ a guid
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

	// get entities
	stream, err := client.GetEntitiesStream(datasetName, "", 1, false, true)
	if err != nil {
		t.Error(err)
	}

	// check context isnt nil
	if stream.Context() == nil {
		t.Error("expected context to be populated")
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

	if e2 != nil {
		t.Errorf("expected entity to be nil, got '%s'", e2.ID)
	}
}

func TestGetChanges(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// make dateset name from test+ a guid
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

	// get the changes
	changes, err := client.GetChanges(datasetName, "", -1, false, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(changes.Entities))
	}

	// use continuation token and check no more changes
	changes, err = client.GetChanges(datasetName, changes.Continuation.Token, -1, false, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 0 {
		t.Errorf("expected 0 entities, got %d", len(changes.Entities))
	}

	// add some more entities and try and get changes again
	prefixedId, err = namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/entity2")
	ec = egdm.NewEntityCollection(namespaceManager)
	entity = egdm.NewEntity().SetID(prefixedId)
	err = ec.AddEntity(entity)
	if err != nil {
		t.Error(err)
	}

	// store entities
	err = client.StoreEntities(datasetName, ec)

	// get the changes
	changes, err = client.GetChanges(datasetName, changes.Continuation.Token, -1, false, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(changes.Entities))
	}

}

func TestChangesWithLatestOnly(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// make dateset name from test+ a guid
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

	// get the changes
	changes, err := client.GetChanges(datasetName, "", -1, true, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(changes.Entities))
	}

	// add some more entities and try and get changes again
	prefixedId, err = namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/entity2")
	ec = egdm.NewEntityCollection(namespaceManager)
	entity = egdm.NewEntity().SetID(prefixedId)
	err = ec.AddEntity(entity)
	if err != nil {
		t.Error(err)
	}
	entity2 := egdm.NewEntity().SetID(prefixedId)
	namePredicate, err := namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/name")
	entity2.SetProperty(namePredicate, "bob")
	err = ec.AddEntity(entity2)
	if err != nil {
		t.Error(err)
	}

	// store entities
	err = client.StoreEntities(datasetName, ec)

	// get the changes
	changes, err = client.GetChanges(datasetName, changes.Continuation.Token, -1, true, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(changes.Entities))
	}

	// check name is bob
	if changes.Entities[0].Properties["http://data.example.com/things/name"] != "bob" {
		t.Errorf("expected name to be bob, got %s", changes.Entities[0].Properties["http://data.example.com/things/name"])
	}
}

func TestGetChangesUsingTake(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// make dateset name from test+ a guid
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
	prefixedId, err = namespaceManager.AssertPrefixedIdentifierFromURI("http://data.example.com/things/entity2")
	entity2 := egdm.NewEntity().SetID(prefixedId)
	err = ec.AddEntity(entity2)
	if err != nil {
		t.Error(err)
	}

	// store entities
	err = client.StoreEntities(datasetName, ec)

	// get the changes
	changes, err := client.GetChanges(datasetName, "", 1, false, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(changes.Entities))
	}

	// get the changes
	changes, err = client.GetChanges(datasetName, changes.Continuation.Token, 1, false, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(changes.Entities))
	}

	// and one more time and expect 0
	changes, err = client.GetChanges(datasetName, changes.Continuation.Token, 1, false, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(changes.Entities) != 0 {
		t.Errorf("expected 0 entities, got %d", len(changes.Entities))
	}
}
