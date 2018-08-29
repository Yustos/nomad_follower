package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	nomadApi "github.com/hashicorp/nomad/api"
)

//FollowedTask a container for a followed task log process
type FollowedTask struct {
	Alloc       *nomadApi.Allocation
	Client      *nomadApi.Client
	ErrorChan   chan string
	OutputChan  chan map[string]interface{}
	Quit        chan struct{}
	ServiceTags []string
	Task        *nomadApi.Task
}

//NewFollowedTask creats a new followed task
func NewFollowedTask(alloc *nomadApi.Allocation, client *nomadApi.Client, errorChan chan string, output chan map[string]interface{}, quit chan struct{}, task *nomadApi.Task) *FollowedTask {
	serviceTags := collectServiceTags(task.Services)
	return &FollowedTask{Alloc: alloc, Task: task, Quit: quit, ServiceTags: serviceTags, OutputChan: output, ErrorChan: errorChan}
}

//Start starts following a task for an allocation
func (ft *FollowedTask) Start() {
	config := nomadApi.DefaultConfig()
	config.WaitTime = 5 * time.Minute
	client, err := nomadApi.NewClient(config)
	if err != nil {
		ft.ErrorChan <- fmt.Sprintf("{ \"message\":\"%s\"}", err)
	}

	fs := client.AllocFS()
	stdErrStream, stdErrErr := fs.Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
	stdOutStream, stdOutErr := fs.Logs(ft.Alloc, true, ft.Task.Name, "stdout", "start", 0, ft.Quit, &nomadApi.QueryOptions{})

	go func() {
		for {

			select {
			case _, ok := <-ft.Quit:
				if !ok {
					return
				}
			case stdErrMsg, stdErrOk := <-stdErrStream:
				if stdErrOk {
					messages := processMessage(stdErrMsg, ft, "Error")
					for _, message := range messages {
						ft.OutputChan <- message
					}
				} else {
					stdErrStream, stdErrErr = fs.Logs(ft.Alloc, true, ft.Task.Name, "stderr", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
				}

			case stdOutMsg, stdOutOk := <-stdOutStream:
				if stdOutOk {
					messages := processMessage(stdOutMsg, ft, "Info")
					for _, message := range messages {
						ft.OutputChan <- message
					}
				} else {
					stdOutStream, stdOutErr = fs.Logs(ft.Alloc, true, ft.Task.Name, "stdout", "start", 0, ft.Quit, &nomadApi.QueryOptions{})
				}

			case errErr := <-stdErrErr:
				ft.ErrorChan <- fmt.Sprintf("Error following stderr for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, errErr)

			case outErr := <-stdOutErr:
				ft.ErrorChan <- fmt.Sprintf("Error following stdout for Allocation:%s Task:%s Error:%s", ft.Alloc.ID, ft.Task.Name, outErr)
			default:
				message := fmt.Sprintf("Processing Allocation:%s ID:%s Task:%s", ft.Alloc.Name, ft.Alloc.ID, ft.Task.Name)
				message = fmt.Sprintf("{ \"message\":\"%s\"}", message)
				_, _ = fmt.Println(message)
				time.Sleep(10 * time.Second)
			}
		}
	}()
}

func collectServiceTags(services []*nomadApi.Service) []string {
	result := make([]string, 0)

	for _, service := range services {
		result = append(result, service.Name)
	}
	return result
}

func processMessage(frame *nomadApi.StreamFrame, ft *FollowedTask, level string) []map[string]interface{} {
	messages := strings.Split(string(frame.Data[:]), "\n")
	jsons := make([]map[string]interface{}, 0)
	for _, message := range messages {
		if message != "" && message != "\n" {
			js := messageJs(message)
			encrichMessage(ft, js, level)
			jsons = append(jsons, js)
		}
	}

	return jsons
}

func messageJs(message string) map[string]interface{} {
	if isJSON(message) {
		return getJSONMessage(message)
	}
	return stringMessage(message)
}

func isJSON(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

func getJSONMessage(s string) map[string]interface{} {
	var js map[string]interface{}
	json.Unmarshal([]byte(s), &js)

	return js
}

func jsonMessage(message string) map[string]interface{} {
	return getJSONMessage(message)
}

func stringMessage(message string) map[string]interface{} {
	js := make(map[string]interface{})
	js["message"] = message
	js["gatheringTime"] = time.Now()
	return js
}

func encrichMessage(ft *FollowedTask, js map[string]interface{}, level string) {
	taskInfo := make(map[string]interface{})
	taskInfo["jobID"] = ft.Alloc.JobID
	taskInfo["allocationID"] = ft.Alloc.ID
	taskInfo["allocationName"] = ft.Alloc.Name
	taskInfo["nodeID"] = ft.Alloc.NodeID
	taskInfo["taskGroup"] = ft.Alloc.TaskGroup
	taskInfo["serviceTags"] = strings.Join(ft.ServiceTags[:], ",")
	taskInfo["name"] = ft.Task.Name
	taskInfo["artifacts"] = ft.Task.Artifacts

	js["task"] = taskInfo
	js["level"] = level
}
