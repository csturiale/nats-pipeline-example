package main

import (
	"context"
	"encoding/json"
	"fmt"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"test-pipeline-flow/pipeline"
	"test-pipeline-flow/utils"
	"time"
)

const (
	numConsumers = 600
)

var streamName = strings.ToUpper(string(pipeline.Kubernetes))
var subjectName = fmt.Sprintf("%s.>", strings.ToLower(streamName))

func main() {
	url := utils.GetEnv("NATS_URL", nats.DefaultURL)
	nc, _ := nats.Connect(url)
	defer nc.Drain()

	newJS, _ := jetstream.New(nc)
	newJS.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:      strings.ToUpper(streamName),
		Subjects:  []string{subjectName},
		Retention: jetstream.WorkQueuePolicy,
	})
	stream, _ := newJS.Stream(context.Background(), streamName)

	for i := 0; i < numConsumers; i++ {
		s := i
		go func() {
			addConsumer(s, stream, newJS)
		}()
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

func addConsumer(s int, stream jetstream.Stream, newJS jetstream.JetStream) {
	fmt.Println(fmt.Sprintf("[KUBERNETES] # adding consumer-%d", s))
	cons, _ := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		//InactiveThreshold: 300 * time.Millisecond,
		FilterSubject: subjectName,
		Name:          "kubernetes",
		Durable:       "kubernetes",
		AckWait:       30 * time.Minute,
	})
	_, _ = cons.Consume(func(msg jetstream.Msg) {
		fmt.Printf("%v - received %q\n", time.Now().Format("2006-01-02 15:04:05.000"), msg.Subject())
		var step pipeline.Step
		if err := json.Unmarshal(msg.Data(), &step); err != nil {
			fmt.Println(err.Error())
		}

		f, err := os.OpenFile(fmt.Sprintf("../logs/%s", step.PipelineName),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
		}
		err = msg.InProgress()
		if err != nil {
			if _, err := f.WriteString(fmt.Sprintf("%v - %s - [KUBERNETES] - err %s\n", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName, err.Error())); err != nil {
				log.Println(err)
			}
		}
		defer f.Close()
		if _, err := f.WriteString(fmt.Sprintf("%v - %s - [KUBERNETES] - consumer-%d Received message Sub: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName, s, msg.Subject())); err != nil {
			log.Println(err)
		}
		if _, err := f.WriteString(fmt.Sprintf("%v - %s - [KUBERNETES] - doing the job\n", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName)); err != nil {
			log.Println(err)
		}
		doTheJob(s, step)

		if _, err := f.WriteString(fmt.Sprintf("%v - %s - [KUBERNETES] - publishing message to %s response\n", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName, step.ResponseSubject)); err != nil {
			log.Println(err)
		}

		fmt.Println(fmt.Sprintf("%v - [KUBERNETES] %s consumer-%d publishing to %s...", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName, s, fmt.Sprintf("%s.ack", step.ResponseSubject)))
		_, err = newJS.Publish(context.Background(), fmt.Sprintf("%s.ack", step.ResponseSubject), nil)
		if err != nil {
			fmt.Println("Error publishing", err.Error())
		}
		err = msg.Ack()
		if _, err := f.WriteString(fmt.Sprintf("%v - %s - [KUBERNETES] - acking message", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName)); err != nil {
			log.Println(err)
		}
		if err != nil {
			if _, err := f.WriteString(fmt.Sprintf("%v - %s - [KUBERNETES] - err acking msg %s\n", time.Now().Format("2006-01-02 15:04:05.000"), step.PipelineName, err.Error())); err != nil {
				log.Println(err)
			}
		}
	})
	fmt.Println(fmt.Sprintf("[KUBERNETES] # created consumer-%d", s), cons.CachedInfo().Name)
}

func doTheJob(consumerNumber int, s pipeline.Step) {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rand.Intn(20)
	f, err := os.OpenFile(fmt.Sprintf("../logs/%s", s.PipelineName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%v - %s - [KUBERNETES] consumer-%d Sleeping for %d seconds...\n", time.Now().Format("2006-01-02 15:04:05.000"), s.PipelineName, consumerNumber, randNum)); err != nil {
		log.Println(err)
	}
	fmt.Println(fmt.Sprintf("%v - [KUBERNETES] %s consumer-%d Sleeping for %d seconds...", time.Now().Format("2006-01-02 15:04:05.000"), s.PipelineName, consumerNumber, randNum))
	time.Sleep(time.Duration(randNum) * time.Second)
	f, err = os.OpenFile(fmt.Sprintf("../output/%s", s.PipelineName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("[KUBERNETES]\n")); err != nil {
		log.Println(err)
	}
	f, err = os.OpenFile(fmt.Sprintf("../timings/%s", s.PipelineName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%v - [KUBERNETES]\n", time.Now().Format("2006-01-02 15:04:05.000"))); err != nil {
		log.Println(err)
	}
}
