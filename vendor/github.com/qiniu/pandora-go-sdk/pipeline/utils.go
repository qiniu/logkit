package pipeline

import (
	"fmt"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
)

func WaitWorkflowStarted(workflowName string, client PipelineAPI, logger base.Logger, getWStatusToken PandoraToken) (err error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			stats, err := client.GetWorkflowStatus(&GetWorkflowStatusInput{
				WorkflowName: workflowName,
				PandoraToken: getWStatusToken,
			})
			if err == nil && stats.Status == base.WorkflowStarted {
				return nil
			}
			logger.Infof("waiting for workflow: %s to be started", workflowName)
		case <-time.After(300 * time.Second):
			return fmt.Errorf("waiting for workflow: %s to be started timeout", workflowName)
		}
	}
	return
}

func WaitWorkflowStopped(workflowName string, client PipelineAPI, logger base.Logger, getWStatusToken PandoraToken) (err error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			stats, err := client.GetWorkflowStatus(&GetWorkflowStatusInput{
				WorkflowName: workflowName,
				PandoraToken: getWStatusToken,
			})
			if err == nil && stats.Status == base.WorkflowStopped {
				return nil
			}
			logger.Infof("waiting for workflow: %s to be stopped", workflowName)
		case <-time.After(300 * time.Second):
			return fmt.Errorf("waiting for workflow: %s to be stopped timeout", workflowName)
		}
	}
	return
}
