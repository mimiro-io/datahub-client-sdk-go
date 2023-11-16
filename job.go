package datahubclient

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
