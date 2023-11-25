
# Data Hub Client SDK for Go
This is a Client SDK in Go for interacting with MIMIRO data hub instances. Documentation on MIMIRO data hub, set up and configuration can be found [here](https://github.com/mimiro-io/datahub/blob/master/DOCUMENTATION.md).

The full go package documentation can be seen [online]().

## Installation
To use the SDK in your Go project, you can use the `go get` command:

```bash
go get github.com/mimiro-io/datahub-client-sdk-go
```

## Usage

To use the SDK in a Go project import the following:

```go
import (
    datahub "github.com/mimiro-io/datahub-client-sdk-go"
)
```

The following sections highlight the core patterns and ways of using the SDK for common tasks.

### Creating a client

To create a client, you need to provide the base URL of the data hub instance you want to connect to. 

For example:

```go   
client := datahub.NewClient("http://localhost:8080")
```

### Authenticating

There are several supported authentication mechanisms; admin authentication using key and secret, client credentials towards an external identity provider using key and secret, and lastly public key authentication using a client certificate to sign the authentication request.

To authenticate and setup the authentication approach for the duration of the client lifecycle the client can be configured differently depending on the desired approach.

To authenticate with client credentials against an external identity provider that supports the OAuth2 flow configure the client in the following way:

```go
client, err := NewClient("http://localhost:8080")
client.WithClientKeyAndSecretAuth("authorizer URL", "audience", "key", "secret")
err = client.Authenticate()
```

To authenticate with admin authentication configure the client in the following way:

```go
client, err := NewClient(testConfig.DataHubUrl)
client.WithAdminAuth("admin user key", "admin user secret")
err = client.Authenticate()
```

To authenticate with client certificate it is assumed that the server has been configured with this client id and corresponding public key. The client should be configured in the following way:

```go
client, err := NewClient(testConfig.DataHubUrl)
client.WithPublicKeyAuth(clientId, privateKey)
err = client.Authenticate()
```

### Add Dataset

To Add a dataset to the datahub.

```go
err := client.AddDataset("datasetName", nil)
```

### Get Datasets

The list of datasets that a client has access to can be retrieved with the `GetDatasets` function.

```go
datasets := client.GetDatasets()
```

### Store Entities

Stores entities into a named dataset. Makes use of the Entity Graph Data Model package. Build a EntityCollection either directly, or by parsing JSON, then call StoreEntities with the collection and the name of the dataset.

```go
namespaceManager := egdm.NewNamespaceContext()
prefixedId, err := namespaceManager.AssertPrefixFromURI("http://data.example.com/things/entity1")
ec := egdm.NewEntityCollection(namespaceManager)
entity := egdm.NewEntity().SetID(prefixedId)
err = ec.AddEntity(entity)
err = client.StoreEntities(datasetName, ec)
```

The StoreEntityStream function can be used to deliver a stream of entities to the server. This is useful when streaming many entities from a file or some other source.

### Get Changes

```go
GetChanges(dataset string, since string, take int, latestOnly bool, reverse bool, expandURIs bool)
```

Since should be the empty string unless this is a subsequent call. Take should be -1 to get all (up to the server limit). LatestOnly indicates if only the latest version of a changed entity should be returned (recommended). Reverse will return the changes with the most recent first. ExpandURIs will returned entities with all namespace prefixes resolved.

```go
changes, err := client.GetChanges("people", "", -1, true, false, true)
```

The entities that have been changed are in the entities property. The Continuation property has a token that can be used as the since value in a subsequent call.

### Get Jobs

To return a list of all the job configurations use the GetJobs function.

```go
jobs, err := client.GetJobs()
```

### Add Job

To add a job use the JobBuilder to construct a Job definition, then call AddJob with that as a parameter. There are many job options that can be set using the JobBuilder.

```go
// build the job definition
jb := NewJobBuilder("myjob", "job1")
jb.WithDescription("my description")
jb.WithTags([]string{"tag1", "tag2"})
jb.WithDatasetSource("my-source-dataset", true)
jb.WithDatasetSink("my-sink-dataset")

js := base64.StdEncoding.EncodeToString([]byte("function transform(record) { return record; }"))
jb.WithJavascriptTransform(js, 0)

triggerBuilder := NewJobTriggerBuilder()
triggerBuilder.WithCron("0 0 0 * *")
triggerBuilder.WithIncremental()
triggerBuilder.AddLogErrorHandler(10)

jb.AddTrigger(triggerBuilder.Build())

// build and add the job
err := client.AddJob(jb.Build())
```

Use `UpdateJob` and `DeleteJob` to manage the set of jobs being executed.

### Run Job

As well as jobs running according to their schedule they can be controlled on demand. There are several job control functions, Run, Pause, Resume, Kill, Reset Token. They are invoked using the Job id.

When running a job it can be run as full sync or incremental.

```go
err = client.RunJobAsFullSync(jobId)
```

### Get Job Statuses

To get the current execution status of running jobs.

```go
statuses, err := client.GetJobStatuses()
```

### Query for Entity

To query for an entity use the QueryBuilder then call RunQuery.

```go
qb := NewQueryBuilder()
qb.WithEntityId("http://data.example.com/things/entity1")
query := qb.Build()

results, err := client.RunQuery(query)
```

### Query for Related Entities

To query for related entities (to traverse the graph) again use the query builder and then call RunQuery.

```go
qb := NewQueryBuilder()
qb.WithEntityId("http://data.example.com/things/entity2")
qb.WithInverse(true)
query := qb.Build()
```

### Javascript Query

To execute a Javascript function it must be encoded as base64. The use the `RunJavascriptQuery` function.

```go
javascriptQuery := `function do_query() {
							WriteQueryResult({key1: "value1"});
							WriteQueryResult({key1: "value2"});
							WriteQueryResult({key1: "value3"});
						}`

// base64 encode the query
javascriptQuery = base64.StdEncoding.EncodeToString([]byte(javascriptQuery))

results, err := client.RunJavascriptQuery(javascriptQuery)

// iterate results
obj, err := results.Next()
```
