package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"os"
	"strings"
	"time"
)

type NestedStruct struct {
	Name string
}
type FakeData struct {
	Name   string
	Nested NestedStruct
}

type Pipeline struct {
	Steps     []Step
	Name      string
	StepCount int
}

type Step struct {
	Name            string
	Component       Component
	Subject         Action
	Data            any
	ResponseSubject string
	PipelineName    string
}

type Component string

const (
	Kubernetes     Component = "kubernetes"
	Git            Component = "git"
	RemoteExecutor Component = "remote-executor"
)

var numConsumers = 2

type Action string

const (
	CreatePod  Action = "create-pod"
	GitClone   Action = "clone"
	MavenBuild Action = "maven-build"
	End        Action = "end"
)

var streamName = "EVENTS-PIPELINES"

func (p *Pipeline) CreatePublisher(newJS jetstream.JetStream) {
	subjectName := fmt.Sprintf("pipeline.%s", p.Name)
	f, err := os.OpenFile(fmt.Sprintf("logs/%s", p.Name),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%v - %s - adding stream %s\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name, streamName)); err != nil {
		log.Println(err)
	}

	newJS.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:      strings.ToUpper(streamName),
		Subjects:  []string{"pipeline.>", "response.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		fmt.Println("Error adding Stream", err.Error())
	}
	// First step
	if _, err := f.WriteString(fmt.Sprintf("%v - %s - call function to publish message %v\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name, p.Steps[p.StepCount])); err != nil {
		log.Println(err)
	}
	publishMessage(p.Steps[p.StepCount], fmt.Sprintf("%s.%d", subjectName, p.StepCount), newJS)
	p.StepCount = p.StepCount + 1
	if _, err := f.WriteString(fmt.Sprintf("%v - %s - stepcount was set to %d\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name, p.StepCount)); err != nil {
		log.Println(err)
	}
}

func publishMessage(s Step, subjectName string, newJS jetstream.JetStream) {
	fmt.Println(fmt.Sprintf("publishing message-%s to %s", s.Name, subjectName))
	f, err := os.OpenFile(fmt.Sprintf("logs/%s", s.PipelineName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	step, err := json.Marshal(s)
	if err != nil {
		fmt.Println("Error marshaling step", err.Error())
	}
	if _, err := f.WriteString(fmt.Sprintf("%v - %s - marshalling step %v\n", time.Now().Format("2006-01-02 15:04:05.000"), s.PipelineName, s)); err != nil {
		log.Println(err)
	}

	if _, err := f.WriteString(fmt.Sprintf("%v - %s - publishing message step %v\n", time.Now().Format("2006-01-02 15:04:05.000"), s.PipelineName, s)); err != nil {
		log.Println(err)
	}
	_, err = newJS.Publish(context.Background(), subjectName, step)
	if err != nil {
		fmt.Println("Error publishing to", subjectName, err.Error())
	}
}

func (p *Pipeline) CreateConsumers(newJS jetstream.JetStream) {
	for i := 0; i < numConsumers; i++ {
		s := i
		go func() {
			addConsumer(p, s, newJS)
			addResponseConsumer(p, s, newJS)
		}()
	}
}

// Receive response from Component and publish a message in the subject pipelineName.
func addResponseConsumer(p *Pipeline, s int, newJS jetstream.JetStream) {
	fmt.Println(fmt.Sprintf("# adding response-consumer-%d", s))
	subjectName := fmt.Sprintf("pipeline.%s", p.Name)
	responseSubjectName := fmt.Sprintf("response.%s.>", p.Name)

	queueName := fmt.Sprintf("%s-queue-response", strings.ToLower(p.Name))

	stream, _ := newJS.Stream(context.Background(), streamName)

	cons, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		//InactiveThreshold: 300 * time.Millisecond,
		FilterSubject: responseSubjectName,
		//FilterSubjects: []string{responseSubjectName},
		Name:          queueName,
		Durable:       queueName,
		MaxAckPending: 1,
		AckWait:       30 * time.Minute,
	})

	if err != nil {
		fmt.Println(fmt.Sprintf("Error subscribing: %s", err.Error()))
	}
	var consumer jetstream.ConsumeContext
	consumer, _ = cons.Consume(func(msg jetstream.Msg) {
		msg.InProgress()
		f, err := os.OpenFile(fmt.Sprintf("logs/%s", p.Name),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
		}
		defer f.Close()
		if _, err := f.WriteString(fmt.Sprintf("%v - %s - working message %s\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name, msg.Data())); err != nil {
			log.Println(err)
		}
		msg.Ack()
		if len(p.Steps) > p.StepCount {
			step := p.Steps[p.StepCount]
			if _, err := f.WriteString(fmt.Sprintf("%v - %s - calling function to publish message with for step %v\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name, step)); err != nil {
				log.Println(err)
			}
			publishMessage(p.Steps[p.StepCount], fmt.Sprintf("%s.%d", subjectName, p.StepCount), newJS)

			if step.Subject == End {
				unsubscribe(stream, consumer, cons.CachedInfo().Name)
				fmt.Println(fmt.Sprintf("%v - Pipeline %s completed,End step done\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name))
				return
			}
			p.StepCount = p.StepCount + 1
			if _, err := f.WriteString(fmt.Sprintf("%v - %s - stepcount set to %d\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name, p.StepCount)); err != nil {
				log.Println(err)
			}
		} else {
			fmt.Println(fmt.Sprintf("Pipeline %s completed\n", p.Name))
			if _, err := f.WriteString(fmt.Sprintf("%v - %s - pipeline completed\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name)); err != nil {
				log.Println(err)
			}

			unsubscribe(stream, consumer, cons.CachedInfo().Name)
			return
		}
	})
}

func unsubscribe(stream jetstream.Stream, consumer jetstream.ConsumeContext, name string) {
	fmt.Println("REMOVING SUBSCRIBTION", name)
	consumer.Stop()
	stream.DeleteConsumer(context.Background(), name)
	fmt.Println("REMOVING SUBSCRIBTION", name)
	name = fmt.Sprintf("%s-response", name)
	stream.DeleteConsumer(context.Background(), name)
}
func addConsumer(p *Pipeline, s int, newJS jetstream.JetStream) {
	fmt.Println(fmt.Sprintf("# adding consumer-%d", s))
	subjectName := fmt.Sprintf("pipeline.%s.>", p.Name)
	queueName := fmt.Sprintf("%s-queue", strings.ToLower(p.Name))

	stream, _ := newJS.Stream(context.Background(), streamName)
	fmt.Println("ADD Consumer to", subjectName)
	cons, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		//InactiveThreshold: 300 * time.Millisecond,
		FilterSubject: subjectName,
		//FilterSubjects: []string{subjectName},
		Name:          queueName,
		Durable:       queueName,
		MaxAckPending: 1,
		AckWait:       30 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
	_, _ = cons.Consume(func(msg jetstream.Msg) {
		fmt.Printf("%v - received %q\n", time.Now().Format("2006-01-02 15:04:05.000"), msg.Subject())
		msg.InProgress()
		var step Step

		if err := json.Unmarshal(msg.Data(), &step); err != nil {
			fmt.Println(err.Error())
		}
		f, err := os.OpenFile(fmt.Sprintf("logs/%s", p.Name),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
		}
		defer f.Close()
		if _, err := f.WriteString(fmt.Sprintf("%v - %s - working message to send to remote component %v\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name, step)); err != nil {
			log.Println(err)
		}
		if step.Subject == End {
			msg.Ack()
			return
		}
		if _, err := f.WriteString(fmt.Sprintf("%v - %s - Calling the internal doTheJob func\n", time.Now().Format("2006-01-02 15:04:05.000"), p.Name)); err != nil {
			log.Println(err)
		}
		doTheJob(step, newJS, s, msg)
		msg.Ack()
	})
}

func doTheJob(step Step, newJS jetstream.JetStream, s int, msg jetstream.Msg) {

	fmt.Println(fmt.Sprintf("%v - [PIPELINE] consumer-%d running step %s", time.Now().Format("2006-01-02 15:04:05.000"), s, step.Name))
	streamName := fmt.Sprintf("%s", strings.ToUpper(string(step.Component)))
	subjectName := fmt.Sprintf("%s.>", strings.ToLower(string(step.Component)))

	_, err := newJS.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:      strings.ToUpper(streamName),
		Subjects:  []string{subjectName},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		fmt.Println("Stream already exists:", err.Error())
	}

	f, err := os.OpenFile(fmt.Sprintf("logs/%s", step.PipelineName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%v - %s - Publishing message in order to be worked by component %s to subj %s\n", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName, step.Component, step.Subject)); err != nil {
		log.Println(err)
	}
	_, err = newJS.Publish(context.Background(), fmt.Sprintf("%s.%s", strings.ToLower(string(step.Component)), strings.ToLower(string(step.Subject))), msg.Data())
}
