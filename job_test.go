package datahub

import (
	"encoding/base64"
	"encoding/json"
	"github.com/google/uuid"
	"testing"
)

func TestJobBuilder(t *testing.T) {
	jb := NewJobBuilder("myjob", "job1")
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")

	js := base64.StdEncoding.EncodeToString([]byte("function transform(record) { return record; }"))
	jb.WithJavascriptTransform(js, 0)

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 0 * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	// serialise to json
	jobJson, err := json.Marshal(jb.Job())
	if err != nil {
		t.Error(err)
	}

	if jobJson == nil {
		t.Error("jobJson is nil")
	}

	// bring it back to a map and check the values
	var jobMap map[string]interface{}
	err = json.Unmarshal(jobJson, &jobMap)
	if err != nil {
		t.Error(err)
	}

	if jobMap["id"] != "job1" {
		t.Errorf("expected id to be 'job1', got '%s'", jobMap["id"])
	}

	if jobMap["title"] != "myjob" {
		t.Errorf("expected title to be 'myjob', got '%s'", jobMap["title"])
	}

	if jobMap["description"] != "my description" {
		t.Errorf("expected description to be 'my description', got '%s'", jobMap["description"])
	}

	if jobMap["tags"] == nil {
		t.Error("expected tags to be present")
	}

	if jobMap["tags"].([]interface{})[0] != "tag1" {
		t.Errorf("expected tag1 to be present, got '%s'", jobMap["tags"].([]interface{})[0])
	}

	if jobMap["tags"].([]interface{})[1] != "tag2" {
		t.Errorf("expected tag2 to be present, got '%s'", jobMap["tags"].([]interface{})[1])
	}

	if jobMap["source"] == nil {
		t.Error("expected source to be present")
	}

	if jobMap["source"].(map[string]interface{})["Name"] != "my-source-dataset" {
		t.Errorf("expected source dataset to be 'my-source-dataset', got '%s'", jobMap["source"].(map[string]interface{})["Name"])
	}

	if jobMap["source"].(map[string]interface{})["Type"] != "DatasetSource" {
		t.Errorf("expected source type to be 'DatasetSource', got '%s'", jobMap["source"].(map[string]interface{})["Type"])
	}

	if jobMap["sink"] == nil {
		t.Error("expected sink to be present")
	}

	if jobMap["sink"].(map[string]interface{})["Type"] != "DatasetSink" {
		t.Errorf("expected soursce dataset to be 'my-source-dataset', got '%s'", jobMap["source"].(map[string]interface{})["Type"])
	}

	if jobMap["sink"].(map[string]interface{})["Name"] != "my-sink-dataset" {
		t.Errorf("expected sink dataset to be 'my-sink-dataset', got '%s'", jobMap["sink"].(map[string]interface{})["Name"])
	}

	// check trigger
	if jobMap["triggers"] == nil {
		t.Error("expected trigger to be present")
	}

	triggers := jobMap["triggers"].([]interface{})
	if len(triggers) != 1 {
		t.Errorf("expected 1 trigger, got %d", len(triggers))
	}

	trigger := triggers[0].(map[string]interface{})
	if trigger["triggerType"] != "cron" {
		t.Errorf("expected trigger type to be 'cron', got '%s'", trigger["triggerType"])
	}

	if trigger["schedule"] != "0 0 0 * *" {
		t.Errorf("expected schedule to be '0 0 0 * *', got '%s'", trigger["schedule"])
	}

	if trigger["jobType"] != "incremental" {
		t.Errorf("expected job type to be 'incremental', got '%s'", trigger["jobType"])
	}

	if trigger["onError"] == nil {
		t.Error("expected on error to be present")
	}

	onError := trigger["onError"].([]interface{})
	if len(onError) != 1 {
		t.Errorf("expected 1 on error, got %d", len(onError))
	}

	onErrorMap := onError[0].(map[string]interface{})
	if onErrorMap["errorHandler"] != "log" {
		t.Errorf("expected error handler type to be 'log', got '%s'", onErrorMap["errorHandler"])
	}

	if int(onErrorMap["maxItems"].(float64)) != 10 {
		t.Errorf("expected max items to be 10, got '%d'", onErrorMap["maxItems"])
	}
}

func TestAddJob(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate job id with guid
	jobId := "job-" + uuid.New().String()

	jb := NewJobBuilder("title-"+jobId, jobId)
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")
	jb.WithPaused(true)

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 * * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	err := client.AddJob(jb.Job())
	if err != nil {
		t.Error(err)
	}

	// check job is there
	jobs, err := client.GetJobs(nil)
	if err != nil {
		t.Error(err)
	}

	// get job with id from returned jobs
	var job *Job
	for _, j := range jobs {
		if j.Id == jobId {
			job = j
			break
		}
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	if job.Title != "title-"+jobId {
		t.Errorf("expected job title to be 'title-%s', got '%s'", jobId, job.Title)
	}

	if job.Description != "my description" {
		t.Errorf("expected job description to be 'my description', got '%s'", job.Description)
	}

	if job.Tags == nil {
		t.Error("expected tags to be present")
	}

	if job.Tags[0] != "tag1" {
		t.Errorf("expected tag1 to be present, got '%s'", job.Tags[0])
	}

	if job.Tags[1] != "tag2" {
		t.Errorf("expected tag2 to be present, got '%s'", job.Tags[1])
	}

	if job.Source == nil {
		t.Error("expected source to be present")

	}

	if job.Source["Name"] != "my-source-dataset" {
		t.Errorf("expected source dataset to be 'my-source-dataset', got '%s'", job.Source["Name"])
	}

	if job.Source["Type"] != "DatasetSource" {
		t.Errorf("expected source type to be 'DatasetSource', got '%s'", job.Source["Type"])
	}

	if job.Sink == nil {
		t.Error("expected sink to be present")
	}

	if job.Sink["Type"] != "DatasetSink" {
		t.Errorf("expected soursce dataset to be 'my-source-dataset', got '%s'", job.Source["Type"])
	}

	if job.Sink["Name"] != "my-sink-dataset" {
		t.Errorf("expected sink dataset to be 'my-sink-dataset', got '%s'", job.Sink["Name"])
	}

	// check trigger
	if job.Triggers == nil {
		t.Error("expected trigger to be present")
	}

	triggers := job.Triggers
	if len(triggers) != 1 {
		t.Errorf("expected 1 trigger, got %d", len(triggers))
	}

	trigger := triggers[0]
	if trigger.TriggerType != "cron" {
		t.Errorf("expected trigger type to be 'cron', got '%s'", trigger.TriggerType)
	}

	if trigger.Schedule != "0 0 * * *" {
		t.Errorf("expected schedule to be '0 0 * * *', got '%s'", trigger.Schedule)
	}

	if trigger.JobType != "incremental" {
		t.Errorf("expected job type to be 'incremental', got '%s'", trigger.JobType)
	}

	if trigger.OnError == nil {
		t.Error("expected on error to be present")
	}

	onError := trigger.OnError
	if len(onError) != 1 {
		t.Errorf("expected 1 on error, got %d", len(onError))
	}

	onErrorMap := onError[0]
	if onErrorMap["errorHandler"] != "log" {
		t.Errorf("expected error handler type to be 'log', got '%s'", onErrorMap["errorHandler"])
	}

	if int(onErrorMap["maxItems"].(float64)) != 10 {
		t.Errorf("expected max items to be 10, got '%d'", onErrorMap["maxItems"])
	}

	// check paused
	if job.Paused != true {
		t.Errorf("expected job to be paused")
	}
}

func TestDeleteJob(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate job id with guid
	jobId := "job-" + uuid.New().String()

	jb := NewJobBuilder("title-"+jobId, jobId)
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 * * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	err := client.AddJob(jb.Job())
	if err != nil {
		t.Error(err)
	}

	// check job is there
	jobs, err := client.GetJobs(nil)
	if err != nil {
		t.Error(err)
	}

	// get job with id from returned jobs
	var job *Job
	for _, j := range jobs {
		if j.Id == jobId {
			job = j
			break
		}
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	// delete job
	err = client.DeleteJob(jobId)
	if err != nil {
		t.Error(err)
	}

	// check job is gone
	jobs, err = client.GetJobs(nil)
	if err != nil {
		t.Error(err)
	}

	// get job with id from returned jobs
	job = nil
	for _, j := range jobs {
		if j.Id == jobId {
			job = j
			break
		}
	}

	if job != nil {
		t.Errorf("expected job with id '%s' to be deleted", jobId)
	}
}

func TestGetJob(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate job id with guid
	jobId := "job-" + uuid.New().String()

	jb := NewJobBuilder("title-"+jobId, jobId)
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 * * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	err := client.AddJob(jb.Job())
	if err != nil {
		t.Error(err)
	}

	// check job is there
	job, err := client.GetJob(jobId)
	if err != nil {
		t.Error(err)
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	if job.Id != jobId {
		t.Errorf("expected job id to be '%s', got '%s'", jobId, job.Id)
	}

}

func TestUpdateJob(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate job id with guid
	jobId := "job-" + uuid.New().String()

	jb := NewJobBuilder("title-"+jobId, jobId)
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 * * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	err := client.AddJob(jb.Job())
	if err != nil {
		t.Error(err)
	}

	// check job is there
	job, err := client.GetJob(jobId)
	if err != nil {
		t.Error(err)
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	if job.Id != jobId {
		t.Errorf("expected job id to be '%s', got '%s'", jobId, job.Id)
	}

	// modify job tags and update
	job.Tags = []string{"tag3", "tag4"}
	err = client.UpdateJob(job)
	if err != nil {
		t.Error(err)
	}

	// check job is there
	job, err = client.GetJob(jobId)
	if err != nil {
		t.Error(err)
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	if job.Id != jobId {
		t.Errorf("expected job id to be '%s', got '%s'", jobId, job.Id)
	}

	if job.Tags[0] != "tag3" {
		t.Errorf("expected tag3 to be present, got '%s'", job.Tags[0])
	}

	if job.Tags[1] != "tag4" {
		t.Errorf("expected tag4 to be present, got '%s'", job.Tags[1])
	}

}

func TestGetJobStatuses(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate job id with guid
	jobId := "job-" + uuid.New().String()

	jb := NewJobBuilder("title-"+jobId, jobId)
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 * * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	err := client.AddJob(jb.Job())
	if err != nil {
		t.Error(err)
	}

	// check job is there
	job, err := client.GetJob(jobId)
	if err != nil {
		t.Error(err)
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	if job.Id != jobId {
		t.Errorf("expected job id to be '%s', got '%s'", jobId, job.Id)
	}

	// check job status
	statuses, err := client.GetJobStatuses()
	if err != nil {
		t.Error(err)
	}

	if statuses == nil {
		t.Error("expected statuses to be present")
	}

}

func TestGetJobsHistory(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate job id with guid
	jobId := "job-" + uuid.New().String()

	jb := NewJobBuilder("title-"+jobId, jobId)
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 * * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	err := client.AddJob(jb.Job())
	if err != nil {
		t.Error(err)
	}

	// check job is there
	job, err := client.GetJob(jobId)
	if err != nil {
		t.Error(err)
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	if job.Id != jobId {
		t.Errorf("expected job id to be '%s', got '%s'", jobId, job.Id)
	}

	// check job history
	history, err := client.GetJobsHistory()
	if err != nil {
		t.Error(err)
	}

	if history == nil {
		t.Error("expected history to be present")
	}

}

func TestGetJobsSchedule(t *testing.T) {
	client := NewAdminUserConfiguredClient()

	// generate job id with guid
	jobId := "job-" + uuid.New().String()

	jb := NewJobBuilder("title-"+jobId, jobId)
	jb.WithDescription("my description")
	jb.WithTags([]string{"tag1", "tag2"})
	jb.WithDatasetSource("my-source-dataset", true)
	jb.WithDatasetSink("my-sink-dataset")

	triggerBuilder := NewJobTriggerBuilder()
	triggerBuilder.AsCron("0 0 * * *")
	triggerBuilder.AsIncremental()
	triggerBuilder.AddLogErrorHandler(10)

	jb.AddTrigger(triggerBuilder.JobTrigger())

	err := client.AddJob(jb.Job())
	if err != nil {
		t.Error(err)
	}

	// check job is there
	job, err := client.GetJob(jobId)
	if err != nil {
		t.Error(err)
	}

	if job == nil {
		t.Errorf("expected job with id '%s' to be present", jobId)
	}

	if job.Id != jobId {
		t.Errorf("expected job id to be '%s', got '%s'", jobId, job.Id)
	}

	// check job schedule
	schedule, err := client.GetJobsSchedule()
	if err != nil {
		t.Error(err)
	}

	if schedule == nil {
		t.Error("expected schedule to be present")
	}
}
