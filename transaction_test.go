package datahub

import (
	"github.com/google/uuid"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"testing"
)

func TestProcessTransaction(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// create two test datasets
	datasetId1 := "dataset-" + uuid.New().String()
	datasetId2 := "dataset-" + uuid.New().String()

	// use the client to create the datasets
	err := client.AddDataset(datasetId1, nil)
	if err != nil {
		t.Error(err)
	}

	err = client.AddDataset(datasetId2, nil)
	if err != nil {
		t.Error(err)
	}

	// create a transaction
	txn := NewTransaction()

	// create an entity
	entityId, err := txn.NamespaceManager.AssertPrefixFromURI("http://data.example.io/entity1")
	entity := egdm.NewEntity().SetID(entityId)
	txn.DatasetEntities[datasetId1] = append(txn.DatasetEntities[datasetId1], entity)

	// create another entity
	entityId2, err := txn.NamespaceManager.AssertPrefixFromURI("http://data.example.io/entity2")
	entity2 := egdm.NewEntity().SetID(entityId2)
	txn.DatasetEntities[datasetId2] = append(txn.DatasetEntities[datasetId2], entity2)

	// run the transaction
	err = client.ProcessTransaction(txn)
	if err != nil {
		t.Error(err)
	}

	// check the entities in the datasets
	dataset1, err := client.GetEntities(datasetId1, "", -1, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(dataset1.Entities) != 1 {
		t.Errorf("expected dataset to have 1 entity, got %d", len(dataset1.Entities))
	}

	dataset2, err := client.GetEntities(datasetId2, "", -1, false, true)
	if err != nil {
		t.Error(err)
	}

	if len(dataset2.Entities) != 1 {
		t.Errorf("expected dataset to have 1 entity, got %d", len(dataset2.Entities))
	}

}
