package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/google/uuid"
)

func batchEvents(values <-chan *cloudwatchlogs.InputLogEvent, maxItems int, maxLength int, maxTimeout time.Duration) chan []*cloudwatchlogs.InputLogEvent {
	batches := make(chan []*cloudwatchlogs.InputLogEvent)

	go func() {
		defer close(batches)

		for keepGoing := true; keepGoing; {
			var batch []*cloudwatchlogs.InputLogEvent
			length := 0
			expire := time.After(maxTimeout)
			for {
				select {
				case value, ok := <-values:
					if !ok {
						keepGoing = false
						goto done
					}

					batch = append(batch, value)
					length += len([]byte(*value.Message)) + 26
					if len(batch) == maxItems {
						goto done
					}
					if length > maxLength {
						goto done
					}

				case <-expire:
					goto done
				}
			}

		done:
			if len(batch) > 0 {
				batches <- batch
			}
		}
	}()

	return batches
}

func main() {
	var logGroupName string
	var logStreamName string
	quiet := false

	flag.StringVar(&logGroupName, "log-group-name", "", "The name of the log group.")
	flag.StringVar(&logStreamName, "log-stream-name", "", "The name of the log stream. (Default=<log-group-name>/<uuid>")
	flag.BoolVar(&quiet, "quiet", quiet, "Suppress output. (Default=false)")
	flag.Parse()

	if logGroupName == "" {
		fmt.Println("log-group-name is required.")
		os.Exit(1)
	}
	if logStreamName == "" {
		id, err := uuid.NewUUID()
		if err != nil {
			panic(err)
		}
		logStreamName = fmt.Sprintf("%s/%s", logGroupName, id.String())
	}

	scanner := bufio.NewScanner(os.Stdin)
	events := make(chan *cloudwatchlogs.InputLogEvent)
	batches := batchEvents(events, 1000, 8000000, time.Second)
	go func() {
		for scanner.Scan() {
			if len(scanner.Text()) > 0 {
				events <- &cloudwatchlogs.InputLogEvent{
					Message:   aws.String(scanner.Text()),
					Timestamp: aws.Int64(aws.TimeUnixMilli(time.Now())),
				}
			}
		}
	}()

	var sequenceToken *string

	session, err := session.NewSession()
	if err != nil {
		panic(err)
	}
	client := cloudwatchlogs.New(session)

	client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(logGroupName),
	})

	streams, err := client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(logGroupName),
		LogStreamNamePrefix: aws.String(logStreamName),
	})

	for _, stream := range streams.LogStreams {
		if *stream.LogStreamName == logStreamName {
			sequenceToken = stream.UploadSequenceToken
		}
	}

	if sequenceToken == nil {
		_, err := client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(logStreamName),
		})
		if err != nil {
			panic(err)
		}
	}

	for batch := range batches {
		input := cloudwatchlogs.PutLogEventsInput{
			LogEvents:     batch,
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(logStreamName),
			SequenceToken: sequenceToken,
		}

		res, err := client.PutLogEvents(&input)
		if err != nil {
			panic(err)
		}
		for _, event := range batch {
			// 	return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
			timestamp := time.Unix(0, *event.Timestamp*int64(time.Millisecond/time.Nanosecond))
			fmt.Println(fmt.Sprintf("%s %s", timestamp.String(), *event.Message))
		}
		sequenceToken = res.NextSequenceToken
	}

}
