package commands

import (
	"fmt"
	mxv1beta1 "github.com/kubeflow/mxnet-operator/pkg/apis/mxnet/v1beta1"
	"github.com/kubeflow/mxnet-operator/pkg/client/clientset/versioned"
	"strings"
	"github.com/kubeflow/arena/pkg/types"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"time"
)

type JobPhase string

const (
	defaultMXJobTrainingType = "mxjob"
)

var (
	allMXjobs []mxv1beta1.MXJob
)

func initMXJobClient() (clientset *versioned.Clientset, err error) {
	if restConfig == nil {
		restConfig, err = clientConfig.ClientConfig()
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
	}
	clientset = versioned.NewForConfigOrDie(restConfig)
	return clientset, nil
}

// MX Job Information
type MXJob struct {
	*BasicJobInfo
	mxjob    mxv1beta1.MXJob
	pods         []v1.Pod // all the pods including statefulset and job
	chiefPod     v1.Pod   // the chief pod
	requestedGPU int64
	allocatedGPU int64
	trainerType  string
}

func (mj *MXJob) Name() string {
	return mj.name
}

func (mj *MXJob) Uid() string {
	return string(mj.mxjob.UID)
}

func (mj *MXJob) ChiefPod() v1.Pod {
	return mj.chiefPod
}

func (mj *MXJob) Trainer() string {
	return mj.trainerType
}

func (mj *MXJob) AllPods() []v1.Pod {
	return mj.pods
}

func (mj *MXJob) GetStatus() (status string) {
	status = "UNKNOWN"
	if mj.mxjob.Name == "" {
		return status
	}
	t := checkMxStatus(mj.mxjob.Status)
	if t == mxv1beta1.MXJobCreated || t == mxv1beta1.MXJobRestarting {
		status = "PENDING"
	} else {
		status = strings.ToUpper(string(t))
	}
	return status
}

func checkMxStatus(status mxv1beta1.MXJobStatus) mxv1beta1.MXJobConditionType {
	t := mxv1beta1.MXJobConditionType("Pending")
	for _, condition := range status.Conditions {
		if condition.Status == v1.ConditionTrue {
			t = condition.Type
		}
	}
	return t
}

func (mj *MXJob) StartTime() *metav1.Time {
	return &mj.mxjob.CreationTimestamp
}
func (mj *MXJob) EndTime() *metav1.Time {
	if mj.mxjob.Status.CompletionTime == nil {
		return nil
	}
	if mj.GetStatus() == "SUCCEEDED" ||  mj.GetStatus() == "FAILED" {
		return mj.mxjob.Status.CompletionTime
	}
	return nil
}

func (mj *MXJob) Age() time.Duration {
	job := mj.mxjob

	// use creation timestamp
	if job.CreationTimestamp.IsZero() {
		return 0
	}
	return metav1.Now().Sub(job.CreationTimestamp.Time)
}

func (mj *MXJob) Duration() time.Duration {
	job := mj.mxjob
	if job.Status.StartTime == nil ||
		job.Status.StartTime.IsZero() {
		return 0
	}
	if !job.Status.CompletionTime.IsZero() {
		return job.Status.CompletionTime.Time.Sub(job.Status.StartTime.Time)
	}
	if mj.GetStatus() == "FAILED" {
		cond := getPodLatestCondition(mj.chiefPod)
		if !cond.LastTransitionTime.IsZero() {
			return cond.LastTransitionTime.Time.Sub(job.Status.StartTime.Time)
		} else {
			log.Debugf("the latest condition's time is zero of pod %s", mj.chiefPod.Name)
		}
	}
	return metav1.Now().Sub(job.Status.StartTime.Time)
}

// Get Dashboard url of the job
func (mj *MXJob) GetJobDashboards(client *kubernetes.Clientset) ([]string, error) {
	urls := []string{}
	// dashboardURL, err := dashboard(client, "kubeflow", "tf-job-dashboard")
	dashboardURL, err := dashboard(client, namespace, "kubernetes-dashboard")

	if err != nil {
		log.Debugf("Get dashboard failed due to %v", err)
		// retry for the existing customers, will be deprecated in the future
		dashboardURL, err = dashboard(client, arenaNamespace, "kubernetes-dashboard")
		if err != nil {
			log.Debugf("Get dashboard failed due to %v", err)
		}
	}

	if err != nil {
		log.Debugf("Get dashboard failed due to %v", err)
		// retry for the existing customers, will be deprecated in the future
		dashboardURL, err = dashboard(client, "kube-system", "kubernetes-dashboard")
		if err != nil {
			log.Debugf("Get dashboard failed due to %v", err)
		}
	}

	if dashboardURL == "" {
		return urls, fmt.Errorf("No LOGVIEWER Installed.")
	}

	if len(mj.chiefPod.Spec.Containers) == 0 {
		return urls, fmt.Errorf("mpi launcher is not ready!")
	}

	url := fmt.Sprintf("%s/#!/log/%s/%s/%s?namespace=%s\n",
		dashboardURL,
		mj.chiefPod.Namespace,
		mj.chiefPod.Name,
		mj.chiefPod.Spec.Containers[0].Name,
		mj.chiefPod.Namespace)

	urls = append(urls, url)

	return urls, nil
}

func (mj *MXJob) RequestedGPU() int64 {
	if mj.requestedGPU > 0 {
		return mj.requestedGPU
	}
	for _, pod := range mj.pods {
		mj.requestedGPU += gpuInPod(pod)
	}
	return mj.requestedGPU
}

func (mj *MXJob) AllocatedGPU() int64 {
	if mj.allocatedGPU > 0 {
		return mj.allocatedGPU
	}
	for _, pod := range mj.pods {
		mj.allocatedGPU += gpuInActivePod(pod)
	}
	return mj.allocatedGPU
}
func (mj *MXJob) HostIPOfChief() (hostIP string) {
	hostIP = "N/A"
	if mj.GetStatus() == "RUNNING" {
		hostIP = mj.chiefPod.Status.HostIP
	}

	return hostIP
}
func (mj *MXJob) Namespace() string {
	return mj.mxjob.Namespace
}

// Get PriorityClass
func (mj *MXJob) GetPriorityClass() string {
	return ""
}

// MX Job trainer
type MXJobTrainer struct {
	client       *kubernetes.Clientset
	mxjobClient *versioned.Clientset
	trainerType  string
	// check if it's enabled
	enabled bool
}

func NewMXJobTrainer(client *kubernetes.Clientset) Trainer {
	log.Debugf("Init mxjob job trainer")
	allMXjobs = []mxv1beta1.MXJob{}
	mxjobClient, err := initMXJobClient()
	if err != nil {
		log.Debugf("unsupported mxjob job due to %v", err)
		return &MXJobTrainer{
			trainerType: defaultMXJobTrainingType,
			enabled:     false,
		}
	}

	ns := namespace
	if allNamespaces {
		ns = metav1.NamespaceAll
	}

	mxjobList, err := mxjobClient.KubeflowV1beta1().MXJobs(ns).List(metav1.ListOptions{})
	if err != nil {
		log.Debugf("unsupported job due to %v", err)
		return &MPIJobTrainer{
			trainerType: defaultMXJobTrainingType,
			enabled:     false,
		}
	}
	for _, job := range mxjobList.Items {
		allMXjobs = append(allMXjobs, job)
	}
	return &MXJobTrainer{
		mxjobClient: mxjobClient,
		client:       client,
		trainerType:  defaultMXJobTrainingType,
		enabled:      true,
	}
}

func (mt *MXJobTrainer) Type() string {
	return mt.trainerType
}

func (mt *MXJobTrainer) IsSupported(name, ns string) bool {
	if !mt.enabled {
		return false
	}

	isMXJob := false

	if useCache {
		for _, job := range allMXjobs {
			if mt.isMXJob(name, ns, job) {
				isMXJob = true
				log.Debugf("the job %s for %s in namespace %s is found.", job.Name, name, ns)
				break
			}
		}
	} else {
		mxjobList, err := mt.mxjobClient.KubeflowV1beta1().MXJobs(ns).List(metav1.ListOptions{
			LabelSelector: fmt.Sprintf("release=%s", name),
		})
		if err != nil {
			log.Debugf("failed to search job %s in namespace %s due to %v", name, ns, err)
		}
		if len(mxjobList.Items) > 0 {
			isMXJob = true
		}
	}
	return isMXJob
}

func (mt *MXJobTrainer) GetTrainingJob(name, namespace string) (tj TrainingJob, err error) {
	return mt.getTrainingJob(name, namespace)
}

func (mt *MXJobTrainer) getTrainingJob(name, namespace string) (TrainingJob, error) {
	var (
		mxjob mxv1beta1.MXJob
	)

	// 1. Get the Job of training Job
	jobList, err := mt.mxjobClient.KubeflowV1beta1().MXJobs(namespace).List(metav1.ListOptions{
		LabelSelector: fmt.Sprintf("release=%s", name),
	})
	if err != nil {
		return nil, err
	}
	if len(jobList.Items) == 0 {
		return nil, fmt.Errorf("Failed to find the job for %s", name)
	} else {
		mxjob = jobList.Items[0]
	}

	// 2. Find the pod list, and determine the pod of the job
	podList, err := mt.client.CoreV1().Pods(namespace).List(metav1.ListOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ListOptions",
			APIVersion: "v1",
		}, LabelSelector: fmt.Sprintf("mxnet_job_name=%s", name),
	})
	if err != nil {
		return nil, err
	}

	pods, chiefPod := getPodsOfMXJob(name, mt, podList.Items)
	return &MXJob{
		BasicJobInfo: &BasicJobInfo{
			resources: podResources(pods),
			name:      name,
		},
		mxjob:   mxjob,
		chiefPod:    chiefPod,
		pods:        pods,
		trainerType: mt.Type(),
	}, nil

}

// Get the training job from Cache
func (mt *MXJobTrainer) getTrainingJobFromCache(name, ns string) (TrainingJob, error) {

	var (
		mxjob mxv1beta1.MXJob
	)
	// 1. Find the  job
	for _, item := range allMXjobs {
		if mt.isMXJob(name, ns, item) {
			mxjob = item
			break
		}
	}
	// 2. Find the pods, and determine the pod of the job
	pods, chiefPod := getPodsOfMXJob(name, mt, allPods)
	return &MXJob{
		BasicJobInfo: &BasicJobInfo{
			resources: podResources(pods),
			name:      name,
		},
		mxjob:   mxjob,
		chiefPod:    chiefPod,
		pods:        pods,
		trainerType: mt.Type(),
	}, nil
}

func (mt *MXJobTrainer) isChiefPod(item v1.Pod) bool {
	if val, ok := item.Labels["mxjob"]; ok {
		log.Debugf("the mxjob %s with labels %s", item.Name, val)
	} else {
		return false
	}

	return true
}

func (mt *MXJobTrainer) isMXJob(name, ns string, item mxv1beta1.MXJob) bool {
	if val, ok := item.Labels["release"]; ok && (val == name) {
		log.Debugf("the mxjob %s with labels %s", item.Name, val)
	} else {
		return false
	}
	if val, ok := item.Labels["app"]; ok && (val == "mxjob") {
		log.Debugf("the mxjob %s with labels %s is found.", item.Name, val)
	} else {
		return false
	}
	if item.Namespace != ns {
		return false
	}
	return true
}

func (mt *MXJobTrainer) isMXJobPod(name, ns string, item v1.Pod) bool {
	log.Debugf("pod.name: %s: %v", item.Name, item.Labels)
	if val, ok := item.Labels["mxnet_job_name"]; ok && (val == name) {
		log.Debugf("the mxjob %s with labels %s", item.Name, val)
	} else {
		return false
	}
	if item.Namespace != ns {
		return false
	}
	return true
}

/**
* List Training jobs
 */
func (mt *MXJobTrainer) ListTrainingJobs() (jobs []TrainingJob, err error) {
	jobs = []TrainingJob{}
	jobInfos := []types.TrainingJobInfo{}
	for _, job := range allMXjobs {
		jobInfo := types.TrainingJobInfo{}
		log.Debugf("find mxjob %s in %s", job.Name, job.Namespace)
		if val, ok := job.Labels["release"]; ok {
			log.Debugf("the mxjob %s with labels %s found in List", job.Name, val)
			jobInfo.Name = val
		} else {
			jobInfo.Name = job.Name
		}
		jobInfo.Namespace = job.Namespace
		jobInfos = append(jobInfos, jobInfo)
	}
	log.Debugf("jobInfos %v", jobInfos)
	for _, jobInfo := range jobInfos {
		job, err := mt.getTrainingJobFromCache(jobInfo.Name, jobInfo.Namespace)
		if err != nil {
			return jobs, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func getPodsOfMXJob(name string, mt *MXJobTrainer, podList []v1.Pod) (pods []v1.Pod, chiefPod v1.Pod) {
	pods = []v1.Pod{}
	for _, item := range podList {
		if !mt.isMXJobPod(name, namespace, item) {
			continue
		}
		if item.Labels["mxnet-replica-type"] == "worker" && item.Labels["mxnet-replica-index"] == "0"{
			chiefPod = item
			log.Debugf("set pod %s as job pod, and it's time is %v", chiefPod.Name, chiefPod.CreationTimestamp)
		}
		pods = append(pods, item)
		log.Debugf("add pod %v to pods", item)
	}
	return pods, chiefPod
}
