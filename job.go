package datahub

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

type Transform struct {
	Type        string `json:"Type"`
	Code        string `json:"Code"`
	Parallelism int    `json:"Parallelism"`
}

// NewJavascriptTransform creates a new JavascriptTransform
// code is the javascript to be executed encoded as a base64 string
func NewJavascriptTransform(code string, parallelism int) *Transform {
	transform := &Transform{}
	transform.Type = "JavascriptTransform"
	transform.Code = code
	transform.Parallelism = parallelism
	return transform
}

type JobTrigger struct {
	TriggerType      string                   `json:"triggerType"`
	JobType          string                   `json:"jobType"`
	Schedule         string                   `json:"schedule"`
	MonitoredDataset string                   `json:"monitoredDataset,omitempty"`
	OnError          []map[string]interface{} `json:"onError,omitempty"`
}

type JobTriggerBuilder struct {
	trigger *JobTrigger
}

func NewJobTriggerBuilder() *JobTriggerBuilder {
	jtb := &JobTriggerBuilder{}
	jtb.trigger = &JobTrigger{}
	jtb.trigger.OnError = make([]map[string]interface{}, 0)
	return jtb
}

func (jtb *JobTriggerBuilder) JobTrigger() *JobTrigger {
	return jtb.trigger
}

func (jtb *JobTriggerBuilder) AsCron(schedule string) *JobTriggerBuilder {
	jtb.trigger.TriggerType = "cron"
	jtb.trigger.Schedule = schedule
	return jtb
}

func (jtb *JobTriggerBuilder) AsOnChange(dataset string) *JobTriggerBuilder {
	jtb.trigger.TriggerType = "onchange"
	jtb.trigger.MonitoredDataset = dataset
	return jtb
}

func (jtb *JobTriggerBuilder) AsIncremental() *JobTriggerBuilder {
	jtb.trigger.JobType = "incremental"
	return jtb
}

func (jtb *JobTriggerBuilder) AsFullSync() *JobTriggerBuilder {
	jtb.trigger.JobType = "fullsync"
	return jtb
}

func (jtb *JobTriggerBuilder) AddLogErrorHandler(maxItems int) *JobTrigger {
	errHandler := map[string]interface{}{}
	errHandler["errorHandler"] = "log"
	errHandler["maxItems"] = maxItems
	jtb.trigger.OnError = append(jtb.trigger.OnError, errHandler)
	return jtb.trigger
}

func (jtb *JobTriggerBuilder) AddRerunErrorHandler(retryDelay int, maxRetries int) *JobTrigger {
	errHandler := map[string]interface{}{}
	errHandler["errorHandler"] = "reRun"
	errHandler["retryDelay"] = retryDelay
	errHandler["maxRetries"] = maxRetries
	jtb.trigger.OnError = append(jtb.trigger.OnError, errHandler)
	return jtb.trigger
}

type Job struct {
	Title       string                 `json:"title"`
	Id          string                 `json:"id"`
	Description string                 `json:"description"`
	Tags        []string               `json:"tags,omitempty"`
	Source      map[string]interface{} `json:"source,omitempty"`
	Sink        map[string]interface{} `json:"sink,omitempty"`
	Transform   *Transform             `json:"transform,omitempty"`
	Triggers    []*JobTrigger          `json:"triggers,omitempty"`
	Paused      bool                   `json:"paused"`
	BatchSize   int                    `json:"batchSize"`
}

type JobBuilder struct {
	job *Job
}

func NewJobBuilder(title string, id string) *JobBuilder {
	jb := &JobBuilder{}
	jb.job = &Job{}
	jb.job.Title = title
	jb.job.Id = id
	return jb
}

func (jb *JobBuilder) WithDescription(description string) *JobBuilder {
	jb.job.Description = description
	return jb
}

func (jb *JobBuilder) WithTags(tags []string) *JobBuilder {
	jb.job.Tags = tags
	return jb
}

func (jb *JobBuilder) WithSource(source map[string]interface{}) *JobBuilder {
	jb.job.Source = source
	return jb
}

func (jb *JobBuilder) WithSink(sink map[string]interface{}) *JobBuilder {
	jb.job.Sink = sink
	return jb
}

func (jb *JobBuilder) WithTransform(transform *Transform) *JobBuilder {
	jb.job.Transform = transform
	return jb
}

// WithJavascriptTransform adds a JavascriptTransform to the job.
// Code is the javascript to be executed encoded as a base64 string.
// Parallelism is the number of parallel workers to use
func (jb *JobBuilder) WithJavascriptTransform(code string, parallelism int) *JobBuilder {
	jb.job.Transform = NewJavascriptTransform(code, parallelism)
	return jb
}

func (jb *JobBuilder) WithTriggers(triggers []*JobTrigger) *JobBuilder {
	jb.job.Triggers = triggers
	return jb
}

func (jb *JobBuilder) AddTrigger(trigger *JobTrigger) *JobBuilder {
	jb.job.Triggers = append(jb.job.Triggers, trigger)
	return jb
}

func (jb *JobBuilder) WithPaused(paused bool) *JobBuilder {
	jb.job.Paused = paused
	return jb
}

func (jb *JobBuilder) WithBatchSize(batchSize int) *JobBuilder {
	jb.job.BatchSize = batchSize
	return jb
}

func (jb *JobBuilder) WithDatasetSource(name string, latestOnly bool) *JobBuilder {
	jb.job.Source = map[string]interface{}{
		"Type":       "DatasetSource",
		"Name":       name,
		"LatestOnly": latestOnly,
	}
	return jb
}

func (jb *JobBuilder) WithHttpSource(url string, latestOnly bool) *JobBuilder {
	jb.job.Source = map[string]interface{}{
		"Type":       "HttpDatasetSource",
		"Url":        url,
		"LatestOnly": latestOnly,
	}
	return jb
}

func (jb *JobBuilder) WithSecureHttpSource(url string, latestOnly bool, tokenProvider string) *Job {
	jb.job.Source = map[string]interface{}{
		"Type":          "HttpDatasetSource",
		"Url":           url,
		"LatestOnly":    latestOnly,
		"TokenProvider": tokenProvider,
	}
	return jb.job
}

func (jb *JobBuilder) WithDatasetSink(name string) *Job {
	jb.job.Sink = map[string]interface{}{
		"Type": "DatasetSink",
		"Name": name,
	}
	return jb.job
}

func (jb *JobBuilder) WithHttpSink(url string) *Job {
	jb.job.Sink = map[string]interface{}{
		"Type": "HttpDatasetSink",
		"Url":  url,
	}
	return jb.job
}

func (jb *JobBuilder) WithSecureHttpSink(url string, tokenProvider string) *Job {
	jb.job.Sink = map[string]interface{}{
		"Type":          "HttpDatasetSink",
		"Url":           url,
		"TokenProvider": tokenProvider,
	}
	return jb.job
}

func (jb *JobBuilder) Job() *Job {
	return jb.job
}

func (c *Client) AddJob(job *Job) error {
	if job == nil {
		return &ParameterError{Msg: "job cannot be nil"}
	}

	if job.Id == "" {
		return &ParameterError{Msg: "job id cannot be empty"}
	}

	jobData, err := json.Marshal(job)
	if err != nil {
		return &ParameterError{Msg: "unable to serialise job"}
	}

	err = c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/jobs", jobData, nil, nil)
	if err != nil {
		return &RequestError{Msg: fmt.Sprintf("unable to add job %s", job.Id), Err: err}
	}

	return nil
}

func (c *Client) GetJobs(filter *JobsFilter) ([]*Job, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/jobs", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get jobs", Err: err}
	}

	var jobs []*Job
	err = json.Unmarshal(data, &jobs)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal jobs", Err: err}
	}

	return jobs, nil
}

func (c *Client) DeleteJob(id string) error {
	if id == "" {
		return &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpDelete, "/jobs/"+id, nil, nil, nil)
	if err != nil {
		return &RequestError{Msg: fmt.Sprintf("unable to delete job with id %s", id), Err: err}
	}

	return nil
}

func (c *Client) GetJob(id string) (*Job, error) {
	if id == "" {
		return nil, &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/jobs/"+id, nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: fmt.Sprintf("unable to get job with id %s", id), Err: err}
	}

	var job *Job
	err = json.Unmarshal(data, &job)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal job", Err: err}
	}

	return job, nil
}

func (c *Client) UpdateJob(job *Job) error {
	if job == nil {
		return &ParameterError{Msg: "job cannot be nil"}
	}

	data, err := json.Marshal(job)
	if err != nil {
		return &ParameterError{Msg: "unable to serialise job"}
	}

	err = c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPost, "/jobs", data, nil, nil)
	if err != nil {
		return &RequestError{Msg: fmt.Sprintf("unable to update job with id %s", job.Id), Err: err}
	}

	return nil
}

type JobStatus struct {
	JobId    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Started  time.Time `json:"started"`
}

func (c *Client) GetJobStatuses() ([]*JobStatus, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/jobs/_/status", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get job statuses ", Err: err}
	}

	var job []*JobStatus
	err = json.Unmarshal(data, &job)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal job statuses", Err: err}
	}

	return job, nil
}

type ScheduleEntries struct {
	Entries []ScheduleEntry `json:"entries"`
}

type ScheduleEntry struct {
	ID       int       `json:"id"`
	JobID    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Next     time.Time `json:"next"`
	Prev     time.Time `json:"prev"`
}

func (c *Client) GetJobsSchedule() (*ScheduleEntries, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/jobs/_/schedules", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get scheduled jobs", Err: err}
	}

	var entries *ScheduleEntries
	err = json.Unmarshal(data, &entries)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal schedule", Err: err}
	}

	return entries, nil
}

type JobResult struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	LastError string    `json:"lastError"`
	Processed int       `json:"processed"`
}

func (c *Client) GetJobsHistory() ([]*JobResult, error) {
	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/jobs/_/history", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get job results", Err: err}
	}

	var jobResults []*JobResult
	err = json.Unmarshal(data, &jobResults)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal job results", Err: err}
	}

	return jobResults, nil
}

func (c *Client) PauseJob(id string) error {
	if id == "" {
		return &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPut, "/job/"+id+"/pause", nil, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to pause job", Err: err}
	}

	return nil
}

func (c *Client) ResumeJob(id string) error {
	if id == "" {
		return &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPut, "/job/"+id+"/resume", nil, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to resume job", Err: err}
	}

	return nil
}

func (c *Client) RunJobAsIncremental(id string) error {
	if id == "" {
		return &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPut, "/job/"+id+"/run?type=incremental", nil, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to kill job", Err: err}
	}

	return nil
}

func (c *Client) RunJobAsFullSync(id string) error {
	if id == "" {
		return &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPut, "/job/"+id+"/run?type=fullsync", nil, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to kill job", Err: err}
	}

	return nil
}

func (c *Client) KillJob(id string) error {
	if id == "" {
		return &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPut, "/job/"+id+"/resume", nil, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to resume job", Err: err}
	}

	return nil
}

func (c *Client) ResetJobSinceToken(id string, token string) error {
	if id == "" {
		return &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	path := "/job/" + id + "/reset"
	if token != "" {
		path += "?since=" + url.QueryEscape(token)
	}

	client := c.makeHttpClient()
	_, err = client.makeRequest(httpPut, path, nil, nil, nil)
	if err != nil {
		return &RequestError{Msg: "unable to reset job since token", Err: err}
	}

	return nil
}

func (c *Client) GetJobStatus(id string) (*JobStatus, error) {
	if id == "" {
		return nil, &ParameterError{Msg: "id cannot be empty"}
	}

	err := c.checkToken()
	if err != nil {
		return nil, &AuthenticationError{Msg: "unable to authenticate", Err: err}
	}

	client := c.makeHttpClient()
	data, err := client.makeRequest(httpGet, "/job/"+id+"/status", nil, nil, nil)
	if err != nil {
		return nil, &RequestError{Msg: "unable to get job status", Err: err}
	}

	var jobStatuses []*JobStatus
	err = json.Unmarshal(data, &jobStatuses)
	if err != nil {
		return nil, &ClientProcessingError{Msg: "unable to unmarshal job status", Err: err}
	}

	if len(jobStatuses) == 0 {
		return nil, nil
	}

	return jobStatuses[0], nil
}

// Jobs Filtering
func NewJobsFilter() *JobsFilter {
	jf := &JobsFilter{}
	jf.hasTags = make([]string, 0)
	return jf
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
