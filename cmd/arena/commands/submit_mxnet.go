package commands

import (
	"fmt"
	"github.com/kubeflow/arena/pkg/util"
	"github.com/kubeflow/arena/pkg/workflow"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var (
	mxjob_chart = util.GetChartsFolder() + "/mxjob"
)

func NewSubmitMXJobCommand() *cobra.Command {
	var (
		submitArgs submitMXJobArgs
	)

	submitArgs.Mode = "mxjob"

	var command = &cobra.Command{
		Use:     "mxjob",
		Short:   "Submit MXNet job as training job.",
		Aliases: []string{"mx"},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmd.HelpFunc()(cmd, args)
				os.Exit(1)
			}

			util.SetLogLevel(logLevel)

			setupKubeconfig()
			_, err := initKubeClient()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			err = updateNamespace(cmd)
			if err != nil {
				log.Debugf("Failed due to %v", err)
				fmt.Println(err)
				os.Exit(1)
			}
			err = submitMXJob(args, &submitArgs)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	submitArgs.addCommonFlags(command)
	submitArgs.addFlag(command)
	return command
}

type submitMXJobArgs struct {
	CleanPodPolicy    string `yaml:"cleanPodPolicy"`    // --cleanTaskPolicy
	SchedulerReplicas string `yaml:"schedulerReplicas"` // --schedulerReplicas
	SchedulerCpu      string `yaml:"schedulerCPU"`      // --schedulerCPU
	SchedulerMemory   string `yaml:"schedulerMemory"`   // --schedulerMemory
	ServerCpu         string `yaml:"serverCPU"`         // --serverCPU
	ServerMemory      string `yaml:"serverMemory"`      // --serverMemory
	WorkerCpu         string `yaml:"workerCPU"`         // --workerCpu
	WorkerMemory      string `yaml:"workerMemory"`      // --workerMemory
	SuccessPolicy     string `yaml:"successPolicy"`     // --successPolicy
	// for common args
	submitArgs `yaml:",inline" json:",inline"`
}

func (submitArgs *submitMXJobArgs) addFlag(command *cobra.Command) {
	// How to clean up Task
	command.Flags().StringVar(&submitArgs.CleanPodPolicy, "cleanTaskPolicy", "None", "How to clean tasks after Training is done, only support Running, None.")
	command.Flags().MarkDeprecated("cleanTaskPolicy", "please use --clean-task-policy instead")
	command.Flags().StringVar(&submitArgs.CleanPodPolicy, "clean-task-policy", "None", "How to clean tasks after Training is done, only support Running, None.")
	//scheduler
	command.Flags().StringVar(&submitArgs.SchedulerReplicas, "schedulerReplicas", "1", "the replicas of scheduler")
	command.Flags().MarkDeprecated("schedulerReplicas", "please use --scheduler-replicas instead")
	command.Flags().StringVar(&submitArgs.SchedulerReplicas, "scheduler-replicas", "1", "the replicas of scheduler")

	command.Flags().StringVar(&submitArgs.SchedulerCpu, "schedulerCpu", "", "the cpu resource to use for the scheduler, like 1 for 1 core.")
	command.Flags().MarkDeprecated("schedulerCpu", "please use --scheduler-cpu instead")
	command.Flags().StringVar(&submitArgs.SchedulerCpu, "scheduler-cpu", "", "the cpu resource to use for the scheduler, like 1 for 1 core.")

	command.Flags().StringVar(&submitArgs.SchedulerMemory, "schedulerMemory", "", "the memory resource to use for the scheduler, like 1Gi.")
	command.Flags().MarkDeprecated("schedulerMemory", "please use --scheduler-memory instead")
	command.Flags().StringVar(&submitArgs.SchedulerMemory, "scheduler-memory", "", "the memory resource to use for the scheduler, like 1Gi.")
	//server
	command.Flags().StringVar(&submitArgs.ServerCpu, "serverCpu", "", "the cpu resource to use for the server, like 1 for 1 core.")
	command.Flags().MarkDeprecated("serverCpu", "please use --server-cpu instead")
	command.Flags().StringVar(&submitArgs.ServerCpu, "server-cpu", "", "the cpu resource to use for the server, like 1 for 1 core.")

	command.Flags().StringVar(&submitArgs.ServerMemory, "serverMemory", "", "the memory resource to use for the server, like 1Gi.")
	command.Flags().MarkDeprecated("serverMemory", "please use --server-memory instead")
	//worker
	command.Flags().StringVar(&submitArgs.WorkerCpu, "workerCpu", "", "the cpu resource to use for the training, like 1 for 1 core.")
	command.Flags().MarkDeprecated("workerCpu", "please use --worker-cpu instead")
	command.Flags().StringVar(&submitArgs.WorkerCpu, "worker-cpu", "", "the cpu resource to use for the training, like 1 for 1 core.")

	command.Flags().StringVar(&submitArgs.WorkerMemory, "workerMemory", "", "the memory resource to use for the training, like 1Gi.")
	command.Flags().MarkDeprecated("workerMemory", "please use --worker-memory instead")
	command.Flags().StringVar(&submitArgs.WorkerMemory, "worker-memory", "", "the memory resource to use for the training, like 1Gi.")
	//successPolicy
	command.Flags().StringVar(&submitArgs.SuccessPolicy, "successPolicy", "AllWorkers", "the job success of policy")
	command.Flags().MarkDeprecated("successPolicy", "please use --success-policy instead")
	command.Flags().StringVar(&submitArgs.SuccessPolicy, "success-policy", "AllWorkers", "the job success of policy")
}

func (submitArgs *submitMXJobArgs) prepare(args []string) (err error) {
	submitArgs.Command = strings.Join(args, " ")

	err = submitArgs.check()
	if err != nil {
		return err
	}

	commonArgs := &submitArgs.submitArgs
	err = commonArgs.transform()
	if err != nil {
		return nil
	}
	if err := submitArgs.addConfigFiles(); err != nil {
		return err
	}

	if len(envs) > 0 {
		submitArgs.Envs = transformSliceToMap(envs, "=")
	}
	// add node labels,if given
	submitArgs.addMxJobNodeSelectors()
	// add tolerations, if given
	submitArgs.addMxJobTolerations()
	submitArgs.addMxJobInfoToEnv()

	return nil
}

func (submitArgs submitMXJobArgs) check() error {
	err := submitArgs.submitArgs.check()
	if err != nil {
		return err
	}
	switch submitArgs.CleanPodPolicy {
	case "None", "Running":
		log.Debugf("Supported cleanTaskPolicy: %s", submitArgs.CleanPodPolicy)
	default:
		return fmt.Errorf("Unsupported cleanTaskPolicy %s", submitArgs.CleanPodPolicy)
	}
	if submitArgs.Image == "" {
		return fmt.Errorf("--image must be set ")
	}

	return nil
}

// add k8s nodes labels
func (submitArgs *submitMXJobArgs) addMxJobNodeSelectors() {
	submitArgs.addNodeSelectors()
}

// add k8s tolerations for taints
func (submitArgs *submitMXJobArgs) addMxJobTolerations() {
	submitArgs.addTolerations()
}
func (submitArgs *submitMXJobArgs) addMxJobInfoToEnv() {
	submitArgs.addJobInfoToEnv()
}

func (submitArgs *submitMXJobArgs) addConfigFiles() error {
	return submitArgs.addJobConfigFiles()
}

// Submit Job
func submitMXJob(args []string, submitArgs *submitMXJobArgs) (err error) {
	err = submitArgs.prepare(args)
	if err != nil {
		return err
	}
	err = workflow.SubmitJob(name, submitArgs.Mode, namespace, submitArgs, mxjob_chart, submitArgs.addHelmOptions()...)
	if err != nil {
		log.Fatal(err)
		return err
	}

	log.Infof("The Job %s has been submitted successfully", name)
	log.Infof("You can run `arena get %s --type %s` to check the job status", name, submitArgs.Mode)
	return nil
}
