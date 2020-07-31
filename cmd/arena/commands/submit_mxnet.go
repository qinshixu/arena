package commands

import (
	"fmt"
	"github.com/kubeflow/arena/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/kubeflow/arena/pkg/workflow"
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

	command.Flags().StringVar(&submitArgs.Cpu, "cpu", "", "the cpu resource to use for the training, like 1 for 1 core.")
	command.Flags().StringVar(&submitArgs.Memory, "memory", "", "the memory resource to use for the training, like 1Gi.")
	// Tensorboard
	//command.Flags().BoolVar(&submitArgs.UseTensorboard, "tensorboard", false, "enable tensorboard")
	//
	//msg := "the docker image for tensorboard"
	//command.Flags().StringVar(&submitArgs.TensorboardImage, "tensorboardImage", "registry.cn-zhangjiakou.aliyuncs.com/tensorflow-samples/tensorflow:1.12.0-devel", msg)
	//command.Flags().MarkDeprecated("tensorboardImage", "please use --tensorboard-image instead")
	//command.Flags().StringVar(&submitArgs.TensorboardImage, "tensorboard-image", "registry.cn-zhangjiakou.aliyuncs.com/tensorflow-samples/tensorflow:1.12.0-devel", msg)
	//
	//command.Flags().StringVar(&submitArgs.TrainingLogdir, "logdir", "/training_logs", "the training logs dir, default is /training_logs")

	submitArgs.addCommonFlags(command)

	return command
}

type submitMXJobArgs struct {
	Cpu    string `yaml:"cpu" json:"cpu"`       // --cpu
	Memory string `yaml:"memory" json:"memory"` // --memory
	// for common args
	submitArgs `yaml:",inline" json:",inline"`

	// for tensorboard
	//submitTensorboardArgs `yaml:",inline" json:",inline"`
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

	//err = submitArgs.HandleSyncCode()
	//if err != nil {
	//	return err
	//}
	if err := submitArgs.addConfigFiles(); err != nil {
		return err
	}
	// process tensorboard
	//submitArgs.processTensorboard(submitArgs.DataSet)

	if len(envs) > 0 {
		submitArgs.Envs = transformSliceToMap(envs, "=")
	}
	// add node labels,if given
	submitArgs.addbytePsNodeSelectors()
	// add tolerations, if given
	submitArgs.addBytePsTolerations()
	submitArgs.addBytePsInfoToEnv()

	return nil
}

func (submitArgs submitMXJobArgs) check() error {
	err := submitArgs.submitArgs.check()
	if err != nil {
		return err
	}

	if submitArgs.Image == "" {
		return fmt.Errorf("--image must be set ")
	}

	return nil
}

// add k8s nodes labels
func (submitArgs *submitMXJobArgs) addbytePsNodeSelectors() {
	submitArgs.addNodeSelectors()
}

// add k8s tolerations for taints
func (submitArgs *submitMXJobArgs) addBytePsTolerations() {
	submitArgs.addTolerations()
}
func (submitArgs *submitMXJobArgs) addBytePsInfoToEnv() {
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
