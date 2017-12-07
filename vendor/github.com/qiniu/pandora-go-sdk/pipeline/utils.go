package pipeline

import (
	"fmt"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
)

func WaitWorkflowStarted(workflowName string, client PipelineAPI, logger base.Logger) (err error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			stats, err := client.GetWorkflowStatus(&GetWorkflowStatusInput{
				WorkflowName: workflowName,
			})
			if err == nil && stats.Status == "Started" {
				return nil
			}
			logger.Infof("waiting for workflow: %s to be started", workflowName)
		case <-time.After(300 * time.Second):
			return fmt.Errorf("waiting for workflow: %s to be started timeout", workflowName)
		}
	}
	return
}

func WaitWorkflowStopped(workflowName string, client PipelineAPI, logger base.Logger) (err error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			stats, err := client.GetWorkflowStatus(&GetWorkflowStatusInput{
				WorkflowName: workflowName,
			})
			if err == nil && stats.Status == "Stopped" {
				return nil
			}
			logger.Infof("waiting for workflow: %s to be stopped", workflowName)
		case <-time.After(300 * time.Second):
			return fmt.Errorf("waiting for workflow: %s to be stopped timeout", workflowName)
		}
	}
	return
}
