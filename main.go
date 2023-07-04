package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"os"
	"os/signal"
	"syscall"
	"test-pipeline-flow/pipeline"
	"time"
)

const numOfPipelines = 5000

func main() {

	url := getEnv("NATS_URL", "nats://localhost:4222")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)
	defer nc.Drain()

	newJS, _ := jetstream.New(nc)
	for i := 0; i < numOfPipelines; i++ {
		pipelineName := fmt.Sprintf("my-pipeline-%d", i)
		responseSubjectName := fmt.Sprintf("response.%s", pipelineName)
		steps := []pipeline.Step{
			{
				Name:      "Create Pod",
				Component: pipeline.Kubernetes,
				Subject:   pipeline.CreatePod,
				Data: pipeline.FakeData{
					Name:   "kube",
					Nested: pipeline.NestedStruct{Name: "rnetes"},
				},
				ResponseSubject: responseSubjectName,
				PipelineName:    pipelineName,
			},
			{
				Name:      "Git Clone",
				Component: pipeline.Git,
				Subject:   pipeline.GitClone,
				Data: pipeline.FakeData{
					Name:   "git",
					Nested: pipeline.NestedStruct{Name: "clone"},
				},
				ResponseSubject: responseSubjectName,
				PipelineName:    pipelineName,
			},
			{
				Name:      "Maven build",
				Component: pipeline.RemoteExecutor,
				Subject:   pipeline.MavenBuild,
				Data: pipeline.FakeData{
					Name:   "maven",
					Nested: pipeline.NestedStruct{Name: "build"},
				},
				ResponseSubject: responseSubjectName,
				PipelineName:    pipelineName,
			},
			/*{
				Name:            "END",
				Subject:         pipeline.End,
				ResponseSubject: responseSubjectName,
				PipelineName:    pipelineName,
			},*/
		}

		p := &pipeline.Pipeline{
			Name:  pipelineName,
			Steps: steps,
		}
		f, err := os.OpenFile(fmt.Sprintf("logs/%s", p.Name),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
		}
		defer f.Close()

		if _, err := f.WriteString(fmt.Sprintf("%v - %s - sending create publisher cmd\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name)); err != nil {
			log.Println(err)
		}
		p.CreatePublisher(newJS)
		if _, err := f.WriteString(fmt.Sprintf("%v - %s - sending create consumers cmd\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name)); err != nil {
			log.Println(err)
		}
		p.CreateConsumers(newJS)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	<-signals // wait for signal
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()
}
func getEnv(envName, valueDefault string) string {
	value := os.Getenv(envName)
	if value == "" {
		return valueDefault
	}
	return value
}
