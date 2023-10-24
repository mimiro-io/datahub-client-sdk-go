package main

import (
	"context"
	"net/url"

	"github.com/coreos/go-oidc/v3/oidc"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// Dataset structure
type Dataset struct {
	name     string
	metadata map[string]any
}

type Job struct {
	id        string
	title     string
	tags      []string
	paused    bool
	source    string
	sink      string
	transform string
	error     string
	duration  string
	lastRun   string
	triggers  string
}

type Query struct {
}

type EntityIterator interface {
	// Next returns the next available entity or nil if no more entities are available
	Next() *egdm.Entity

	// Token returns a continuation token that can be used to resume the iteration.
	Token() string
}

type AuthType int

const (
	// used for connecting to unsercured datahub instances
	AuthTypeNone AuthType = iota
	// used for connecting as admin user with username and password
	AuthTypeBasic
	// used for OAuth flow with client key and secret
	AuthTypeClientKeyAndSecret
	// Used for OAuth flow with signed JWT authentication request
	AuthTypePublicKey
	// AuthType User uses the OAuth User flow
	AuthTypeUser
)

type AuthConfig struct {
	AuthType     AuthType
	Authorizer   string
	ClientID     string
	ClientSecret string
	Audience     string
	PrivateKey   []byte
}

type Client struct {
	AuthConfig *AuthConfig
	AuthToken  *oauth2.Token
}

func NewClient() *Client {
	client := &Client{}
	client.AuthConfig = &AuthConfig{
		AuthType: AuthTypeNone,
	}
	return client
}

func (c *Client) WithBasicAuth(username string, password string) {
	c.AuthConfig = &AuthConfig{
		AuthType:     AuthTypeBasic,
		ClientID:     username,
		ClientSecret: password,
	}
}

func (c *Client) WithClientKeyAndSecretAuth(clientKey string, clientSecret string) {
	c.AuthConfig = &AuthConfig{
		AuthType:     AuthTypeClientKeyAndSecret,
		ClientID:     clientKey,
		ClientSecret: clientSecret,
	}
}

func (c *Client) WithPublicKeyAuth(privateKey []byte) {
	c.AuthConfig = &AuthConfig{
		AuthType:   AuthTypePublicKey,
		PrivateKey: privateKey,
	}
}

func (c *Client) WithUserAuth(authorizer string, audience string) {
	c.AuthConfig = &AuthConfig{
		AuthType:   AuthTypeUser,
		Audience:   audience,
		Authorizer: authorizer,
	}
}

func (c *Client) GenerateKeyPair(location string) error {
	return nil
}

func (c *Client) Authenticate() error {
	// check if current token should be refreshed

	// if no token, get one

	return nil
}

func (c *Client) DoClientCredentialsLogin() (*oauth2.Token, error) {
	ctx := oidc.InsecureIssuerURLContext(context.Background(), c.AuthConfig.Authorizer)
	provider, err := oidc.NewProvider(ctx, c.AuthConfig.Authorizer)
	if err != nil {
		return nil, err
	}

	params := url.Values{"audience": []string{c.AuthConfig.Audience}}
	cc := &clientcredentials.Config{
		ClientID:       c.AuthConfig.ClientKey,
		ClientSecret:   c.AuthConfig.ClientSecret,
		TokenURL:       provider.Endpoint().TokenURL,
		EndpointParams: params,
	}
	cfg.ClientCredentialsConfig = cc

	return cc.Token(ctx)
	return nil
}

func (c *Client) IsJWTValid() (bool, error) {
	if c.AuthToken == nil {
		return false, nil
	}

	return false, nil
}

func (c *Client) GetDataset(dataset string) (*Dataset, error) {
	return nil, nil
}

func (c *Client) CreateDataset(dataset string) (*Dataset, error) {
	return nil, nil
}

func (c *Client) DeleteDataset(dataset string) (*Dataset, error) {
	return nil, nil
}

func (c *Client) GetChanges(dataset string, since string, take int, latestOnly bool) (EntityIterator, error) {
	return nil, nil
}

func (c *Client) GetEntities(dataset string, since string, take int) (EntityIterator, error) {
	return nil, nil
}

func (c *Client) ListDatasets() ([]*Dataset, error) {
	return nil, nil
}

func (c *Client) GetJob(job string) (*Job, error) {
	return nil, nil
}

func (c *Client) OperateJob(job string) (*Job, error) {
	return nil, nil
}

func (c *Client) ListJobs(filter *JobsFilter) []*Job {
	return nil
}

func (c *Client) CreateJob(job string) *Job {
	return nil
}

func (c *Client) RunQuery(query *Query) map[string]any {
	return nil
}

// this is the set of features offered by the cli so makes a good candidate list for the sdk
// also add this to the server directly
// add with functions to JobFilters for the following
// title=mystringhere
// tags=mytag
// id=myidstring
// paused=true
// source=dataset
// sink=http
// transform=javascript
// error=my error message
// duration>10s or duration<30ms
// lastrun<2020-11-19T14:56:17+01:00 or lastrun>2020-11-19T14:56:17+01:00
// triggers=@every 60 or triggers=fullsync or triggers=person.Crm

func NewJobsFilter() *JobsFilter {
	jf := &JobsFilter{}
	jf.hasTags = make([]string, 0)
	return jf
}

// JobsFilter structure used for filtering jobs when using the ListJobs function
type JobsFilter struct {
	isPaused               bool
	hasTitle               string
	hasTags                []string
	hasId                  string
	hasSource              string
	hasSink                string
	hasTransform           string
	hasError               string
	hasDurationGreaterThan string
	hasDurationLessThan    string
	hasLastRunAfter        string
	hasLastRunBefore       string
	hasTrigger             string
}

// HasTitle adds a title filter to the JobsFilter
func (jf *JobsFilter) HasTitle(title string) *JobsFilter {
	jf.hasTitle = title
	return jf
}

// HasTags adds a tags filter to the JobsFilter
func (jf *JobsFilter) HasTags(tags string) *JobsFilter {
	jf.hasTags = append(jf.hasTags, tags)
	return jf
}

// HasId adds an id filter to the JobsFilter
func (jf *JobsFilter) HasId(id string) *JobsFilter {
	jf.hasId = id
	return jf
}

// IsPaused adds a paused filter to the JobsFilter
func (jf *JobsFilter) IsPaused(paused bool) *JobsFilter {
	jf.isPaused = paused
	return jf
}

// HasSource adds a source filter to the JobsFilter
func (jf *JobsFilter) HasSource(source string) *JobsFilter {
	jf.hasSource = source
	return jf
}

// HasSink adds a sink filter to the JobsFilter
func (jf *JobsFilter) HasSink(sink string) *JobsFilter {
	jf.hasSink = sink
	return jf
}

// HasTransform adds a transform filter to the JobsFilter
func (jf *JobsFilter) HasTransform(transform string) *JobsFilter {
	jf.hasTransform = transform
	return jf
}

// HasError adds an error filter to the JobsFilter
func (jf *JobsFilter) HasError(err string) *JobsFilter {
	jf.hasError = err
	return jf
}

// HasDurationGreaterThan adds a duration filter to the JobsFilter
func (jf *JobsFilter) HasDurationGreaterThan(duration string) *JobsFilter {
	jf.hasDurationGreaterThan = duration
	return jf
}

// HasDurationLessThan adds a duration filter to the JobsFilter
func (jf *JobsFilter) HasDurationLessThan(duration string) *JobsFilter {
	jf.hasDurationLessThan = duration
	return jf
}

// HasLastRunAfter adds a last run after filter to the JobsFilter
func (jf *JobsFilter) HasLastRunAfter(lastRun string) *JobsFilter {
	jf.hasLastRunAfter = lastRun
	return jf
}

// HasLastRunBefore adds a last run before filter to the JobsFilter
func (jf *JobsFilter) HasLastRunBefore(lastRun string) *JobsFilter {
	jf.hasLastRunBefore = lastRun
	return jf
}

// HasTrigger adds a triggers filter to the JobsFilter
func (jf *JobsFilter) HasTrigger(triggers string) *JobsFilter {
	jf.hasTrigger = triggers
	return jf
}
