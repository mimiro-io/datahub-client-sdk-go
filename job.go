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

// JobTrigger represents a trigger for a job
// TriggerType can be cron or onchange
// JobType can be incremental or fullsync
// Schedule is the cron schedule
// MonitoredDataset is the dataset to monitor for changes
// OnError is a list of error handlers
type JobTrigger struct {
	TriggerType      string                   `json:"triggerType"`
	JobType          string                   `json:"jobType"`
	Schedule         string                   `json:"schedule"`
	MonitoredDataset string                   `json:"monitoredDataset,omitempty"`
	OnError          []map[string]interface{} `json:"onError,omitempty"`
}

// JobTriggerBuilder is a builder for JobTrigger
type JobTriggerBuilder struct {
	trigger *JobTrigger
}

// NewJobTriggerBuilder creates a new JobTriggerBuilder.
// Use the build functions to build the JobTrigger after
// calling the configuration functions.
func NewJobTriggerBuilder() *JobTriggerBuilder {
	jtb := &JobTriggerBuilder{}
	jtb.trigger = &JobTrigger{}
	jtb.trigger.OnError = make([]map[string]interface{}, 0)
	return jtb
}

// Build builds the JobTrigger
func (jtb *JobTriggerBuilder) Build() *JobTrigger {
	return jtb.trigger
}

// WithCron configures the JobTrigger as a cron trigger
// schedule is the cron schedule
func (jtb *JobTriggerBuilder) WithCron(schedule string) *JobTriggerBuilder {
	jtb.trigger.TriggerType = "cron"
	jtb.trigger.Schedule = schedule
	return jtb
}

// WithOnChange configures the JobTrigger as an onchange trigger
// dataset is the dataset to monitor for changes
func (jtb *JobTriggerBuilder) WithOnChange(dataset string) *JobTriggerBuilder {
	jtb.trigger.TriggerType = "onchange"
	jtb.trigger.MonitoredDataset = dataset
	return jtb
}

// WithIncremental configures the JobTrigger as an incremental job
func (jtb *JobTriggerBuilder) WithIncremental() *JobTriggerBuilder {
	jtb.trigger.JobType = "incremental"
	return jtb
}

// WithFullSync configures the JobTrigger as a full sync job
func (jtb *JobTriggerBuilder) WithFullSync() *JobTriggerBuilder {
	jtb.trigger.JobType = "fullsync"
	return jtb
}

// AddLogErrorHandler adds a log error handler to the JobTrigger
// maxItems is the maximum number of items to log
func (jtb *JobTriggerBuilder) AddLogErrorHandler(maxItems int) *JobTrigger {
	errHandler := map[string]interface{}{}
	errHandler["errorHandler"] = "log"
	errHandler["maxItems"] = maxItems
	jtb.trigger.OnError = append(jtb.trigger.OnError, errHandler)
	return jtb.trigger
}

// AddRerunErrorHandler adds a kill error handler to the JobTrigger
// retryDelay is the delay in seconds before retrying
// maxRetries is the maximum number of retries that should be attempted
func (jtb *JobTriggerBuilder) AddRerunErrorHandler(retryDelay int, maxRetries int) *JobTrigger {
	errHandler := map[string]interface{}{}
	errHandler["errorHandler"] = "reRun"
	errHandler["retryDelay"] = retryDelay
	errHandler["maxRetries"] = maxRetries
	jtb.trigger.OnError = append(jtb.trigger.OnError, errHandler)
	return jtb.trigger
}

// Job is a datahub job
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

// JobBuilder is a builder for Job
type JobBuilder struct {
	job *Job
}

// NewJobBuilder creates a new JobBuilder.
// Use the build functions to build the Job after
// title and id must be provided, by non-empty and be unique
func NewJobBuilder(title string, id string) *JobBuilder {
	jb := &JobBuilder{}
	jb.job = &Job{}
	jb.job.Title = title
	jb.job.Id = id
	return jb
}

// WithDescription adds a description to the job
func (jb *JobBuilder) WithDescription(description string) *JobBuilder {
	jb.job.Description = description
	return jb
}

// WithTags adds tags to the job
func (jb *JobBuilder) WithTags(tags []string) *JobBuilder {
	jb.job.Tags = tags
	return jb
}

// WithSource adds a source to the job. See data hub documentation on valid sources
// Use of the WithXXXSource simplifies most use cases
func (jb *JobBuilder) WithSource(source map[string]interface{}) *JobBuilder {
	jb.job.Source = source
	return jb
}

// WithSink adds a sink to the job. See data hub documentation on valid sinks
// Use of the WithXXXSink simplifies most use cases
func (jb *JobBuilder) WithSink(sink map[string]interface{}) *JobBuilder {
	jb.job.Sink = sink
	return jb
}

// WithTransform adds a transform to the job. See data hub documentation on valid transforms
// Use of the WithXXXTransform simplifies most use cases
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

// WithTriggers adds triggers to the job. See data hub documentation on valid triggers
func (jb *JobBuilder) WithTriggers(triggers []*JobTrigger) *JobBuilder {
	jb.job.Triggers = triggers
	return jb
}

// AddTrigger adds a trigger to the job. Use the JobTriggerBuilder to construct valid triggers
func (jb *JobBuilder) AddTrigger(trigger *JobTrigger) *JobBuilder {
	jb.job.Triggers = append(jb.job.Triggers, trigger)
	return jb
}

// WithPaused adds a paused flag to the job
func (jb *JobBuilder) WithPaused(paused bool) *JobBuilder {
	jb.job.Paused = paused
	return jb
}

// WithBatchSize adds a batch size to the job
func (jb *JobBuilder) WithBatchSize(batchSize int) *JobBuilder {
	jb.job.BatchSize = batchSize
	return jb
}

// WithDatasetSource adds a dataset source to the job
// name is the name of the dataset
// latestOnly is a flag to indicate whether only the latest version of the entities should be used
func (jb *JobBuilder) WithDatasetSource(name string, latestOnly bool) *JobBuilder {
	jb.job.Source = map[string]interface{}{
		"Type":       "DatasetSource",
		"Name":       name,
		"LatestOnly": latestOnly,
	}
	return jb
}

// WithHttpSource adds an http source to the job
// url is the url to the source
// latestOnly is a flag to indicate whether only the latest version of the entities should be used
func (jb *JobBuilder) WithHttpSource(url string, latestOnly bool) *JobBuilder {
	jb.job.Source = map[string]interface{}{
		"Type":       "HttpDatasetSource",
		"Url":        url,
		"LatestOnly": latestOnly,
	}
	return jb
}

// WithSecureHttpSource adds a secure http source to the job
// url is the url to the source
// latestOnly is a flag to indicate whether only the latest version of the entities should be used
// tokenProvider is the name of the token provider to use
func (jb *JobBuilder) WithSecureHttpSource(url string, latestOnly bool, tokenProvider string) *JobBuilder {
	jb.job.Source = map[string]interface{}{
		"Type":          "HttpDatasetSource",
		"Url":           url,
		"LatestOnly":    latestOnly,
		"TokenProvider": tokenProvider,
	}
	return jb
}

// WithDatasetSink adds a dataset sink to the job
// name is the name of the dataset
func (jb *JobBuilder) WithDatasetSink(name string) *JobBuilder {
	jb.job.Sink = map[string]interface{}{
		"Type": "DatasetSink",
		"Name": name,
	}
	return jb
}

// WithHttpSink adds an http sink to the job
// url is the url to the sink
func (jb *JobBuilder) WithHttpSink(url string) *JobBuilder {
	jb.job.Sink = map[string]interface{}{
		"Type": "HttpDatasetSink",
		"Url":  url,
	}
	return jb
}

// WithSecureHttpSink adds a secure http sink to the job
// url is the url to the sink
// tokenProvider is the name of the token provider to use
func (jb *JobBuilder) WithSecureHttpSink(url string, tokenProvider string) *JobBuilder {
	jb.job.Sink = map[string]interface{}{
		"Type":          "HttpDatasetSink",
		"Url":           url,
		"TokenProvider": tokenProvider,
	}
	return jb
}

// WithUnionDatasetSource adds a UnionDatasetSource to the job.
// name is the name of the union dataset.
// contributingDatasets is a list of dataset names that contribute to the union.
// latestOnly indicates whether the union should only contain the latest version of an entity from each source.
func (jb *JobBuilder) WithUnionDatasetSource(contributingDatasets []string, latestOnly bool) *JobBuilder {
	datasetSources := make([]map[string]interface{}, 0)
	for _, dataset := range contributingDatasets {
		datasetSources = append(datasetSources, map[string]interface{}{
			"Type":       "DatasetSource",
			"Name":       dataset,
			"LatestOnly": latestOnly,
		})
	}

	jb.job.Source = map[string]interface{}{
		"Type":           "UnionDatasetSource",
		"DatasetSources": datasetSources,
	}
	return jb
}

// Build builds the Job
func (jb *JobBuilder) Build() *Job {
	return jb.job
}

// AddJob adds a job to the data hub
// Use the JobBuilder to create valid jobs
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job is nil, the job id is empty or the job title is empty.
// returns a RequestError if the request fails.
func (c *Client) AddJob(job *Job) error {
	if job == nil {
		return &ParameterError{Msg: "job cannot be nil"}
	}

	if job.Id == "" {
		return &ParameterError{Msg: "job id cannot be empty"}
	}

	if job.Title == "" {
		return &ParameterError{Msg: "job title cannot be empty"}
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

// GetJobs gets a list of jobs from the data hub
// returns an AuthenticationError if the client is unable to authenticate.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
func (c *Client) GetJobs() ([]*Job, error) {
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

// DeleteJob deletes a job from the data hub
// id is the id of the job to delete
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
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

// GetJob gets a job from the data hub
// id is the id of the job to get
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
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

// UpdateJob updates a job in the data hub
// Use the JobBuilder to create valid jobs
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job is nil, the job id is empty or the job title is empty.
// returns a RequestError if the request fails.
func (c *Client) UpdateJob(job *Job) error {
	if job == nil {
		return &ParameterError{Msg: "job cannot be nil"}
	}

	if job.Id == "" {
		return &ParameterError{Msg: "job id cannot be empty"}
	}

	if job.Title == "" {
		return &ParameterError{Msg: "job title cannot be empty"}
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

// JobStatus represents the status of a running job
type JobStatus struct {
	JobId    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Started  time.Time `json:"started"`
}

// GetJobStatuses gets the status of all running jobs from the data hub
// returns an AuthenticationError if the client is unable to authenticate.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
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

// ScheduleEntries is a container for all scheduled jobs
type ScheduleEntries struct {
	Entries []ScheduleEntry `json:"entries"`
}

// ScheduleEntry is information about a scheduled job
type ScheduleEntry struct {
	ID       int       `json:"id"`
	JobID    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Next     time.Time `json:"next"`
	Prev     time.Time `json:"prev"`
}

// GetJobsSchedule gets the schedule for all scheduled jobs from the data hub
// returns an AuthenticationError if the client is unable to authenticate.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
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

// JobResult represents the history of job runs
type JobResult struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	LastError string    `json:"lastError"`
	Processed int       `json:"processed"`
}

// GetJobsHistory gets the history of all jobs from the data hub
// returns an AuthenticationError if the client is unable to authenticate.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
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

// PauseJob pauses a job in the data hub
// id is the id of the job to pause
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
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

// ResumeJob resumes a job in the data hub
// id is the id of the job to resume
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
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

// RunJobAsIncremental runs a job as an incremental job
// id is the id of the job to run
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
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

// RunJobAsFullSync runs a job as a full sync job
// id is the id of the job to run
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
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

// KillJob kills a job in the data hub
// id is the id of the job to kill
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
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

// ResetJobSinceToken resets the job since token
// id is the id of the job to reset
// token is the since token to reset to
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
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

// GetJobStatus gets the status of a job from the data hub
// id is the id of the job to get the status for
// returns an AuthenticationError if the client is unable to authenticate.
// returns a ParameterError if the job id is empty.
// returns a RequestError if the request fails.
// returns a ClientProcessingError if the response cannot be processed.
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
func newJobsFilter() *jobsFilter {
	jf := &jobsFilter{}
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

// jobsFilter structure used for filtering jobs when using the ListJobs function
type jobsFilter struct {
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

// HasTitle adds a title filter to the jobsFilter
func (jf *jobsFilter) HasTitle(title string) *jobsFilter {
	jf.hasTitle = title
	return jf
}

// HasTags adds a tags filter to the jobsFilter
func (jf *jobsFilter) HasTags(tags string) *jobsFilter {
	jf.hasTags = append(jf.hasTags, tags)
	return jf
}

// HasId adds an id filter to the jobsFilter
func (jf *jobsFilter) HasId(id string) *jobsFilter {
	jf.hasId = id
	return jf
}

// IsPaused adds a paused filter to the jobsFilter
func (jf *jobsFilter) IsPaused(paused bool) *jobsFilter {
	jf.isPaused = paused
	return jf
}

// HasSource adds a source filter to the jobsFilter
func (jf *jobsFilter) HasSource(source string) *jobsFilter {
	jf.hasSource = source
	return jf
}

// HasSink adds a sink filter to the jobsFilter
func (jf *jobsFilter) HasSink(sink string) *jobsFilter {
	jf.hasSink = sink
	return jf
}

// HasTransform adds a transform filter to the jobsFilter
func (jf *jobsFilter) HasTransform(transform string) *jobsFilter {
	jf.hasTransform = transform
	return jf
}

// HasError adds an error filter to the jobsFilter
func (jf *jobsFilter) HasError(err string) *jobsFilter {
	jf.hasError = err
	return jf
}

// HasDurationGreaterThan adds a duration filter to the jobsFilter
func (jf *jobsFilter) HasDurationGreaterThan(duration string) *jobsFilter {
	jf.hasDurationGreaterThan = duration
	return jf
}

// HasDurationLessThan adds a duration filter to the jobsFilter
func (jf *jobsFilter) HasDurationLessThan(duration string) *jobsFilter {
	jf.hasDurationLessThan = duration
	return jf
}

// HasLastRunAfter adds a last run after filter to the jobsFilter
func (jf *jobsFilter) HasLastRunAfter(lastRun string) *jobsFilter {
	jf.hasLastRunAfter = lastRun
	return jf
}

// HasLastRunBefore adds a last run before filter to the jobsFilter
func (jf *jobsFilter) HasLastRunBefore(lastRun string) *jobsFilter {
	jf.hasLastRunBefore = lastRun
	return jf
}

// HasTrigger adds a triggers filter to the jobsFilter
func (jf *jobsFilter) HasTrigger(triggers string) *jobsFilter {
	jf.hasTrigger = triggers
	return jf
}
