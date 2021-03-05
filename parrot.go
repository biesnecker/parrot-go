package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/polly"
	"github.com/jessevdk/go-flags"
	"go.uber.org/ratelimit"
)

type opts struct {
	Input string `short:"i" long:"input" description:"path to input file" required:"true"`

	Output string `short:"o" long:"output" description:"path to output file" required:"true"`

	AudioOut string `short:"a" long:"audio-out" description:"path to the audio output directory" required:"true"`

	Language string `short:"l" long:"language" description:"language code for input text" required:"true"`

	Voice string `short:"v" long:"voice" description:"AWS Polly voice to use" required:"true"`

	Neural bool `short:"n" long:"neural" description:"Use neural voice"`
}

func main() {
	var options opts

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	pollyClient := polly.New(sess)

	var parser = flags.NewParser(&options, flags.Default)
	if _, err := parser.Parse(); err != nil {
		if flagErr, ok := err.(*flags.Error); ok && flagErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		os.Exit(1)
	}

	var maxRequestsPerSecond int
	if options.Neural {
		maxRequestsPerSecond = 9
	} else {
		maxRequestsPerSecond = 90
	}

	rl := ratelimit.New(maxRequestsPerSecond)

	seenTracker := makeSeenTracker()
	seenTracker.Start()

	inputRecordChan := make(chan CSVRecord)
	outputRecordChan := make(chan []string)

	// Start reading the file.
	go func() {
		if err := ReadCSVFile(options.Input, inputRecordChan); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		close(inputRecordChan)
	}()

	// Start the writer goroutine too. When it's done we'll be done.
	stopChan := make(chan struct{})
	go func() {
		if err := WriteCSV(options.Output, outputRecordChan); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
			os.Exit(1)
		}
		close(stopChan)
	}()

	// Start processing the incoming records.
	var wg sync.WaitGroup
	for record := range inputRecordChan {
		if err := seenTracker.Check(record.columns[0], record.lineNo); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
		wg.Add(1)
		go func(r *CSVRecord) {
			defer wg.Done()

			h := sha1.New()
			h.Write([]byte(r.columns[0]))

			audioFilename := fmt.Sprintf("%x.mp3", h.Sum(nil))
			audioFilepath := filepath.Join(options.AudioOut, audioFilename)
			outputRecord := append(r.columns, audioFilename)

			// Does the audio file already exist? If so, then just write to the
			// file. If it doesn't, then we'll need to hit AWS.
			if _, err := os.Stat(audioFilepath); err == nil {
				// File exists. Just write the output and we're done.
				outputRecordChan <- outputRecord
				return
			} else if errors.Is(err, os.ErrNotExist) {
				// File doesn't exist. Let's ask Polly.
				input := &polly.SynthesizeSpeechInput{
					OutputFormat: aws.String("mp3"),
					Text:         aws.String(r.columns[0]),
					VoiceId:      aws.String(options.Voice),
					LanguageCode: aws.String(options.Language)}

				if options.Neural {
					input.Engine = aws.String(polly.EngineNeural)
				} else {
					input.Engine = aws.String(polly.EngineStandard)
				}

				rl.Take()
				pollyResponse, err := pollyClient.SynthesizeSpeech(input)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error synthesizing speech: %v\n", err)
					os.Exit(1)
				}
				outputFile, err := os.Create(audioFilepath)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error writing audio file: %v\n", err)
					os.Exit(1)
				}
				defer outputFile.Close()
				_, err = io.Copy(outputFile, pollyResponse.AudioStream)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error writing audio file: %v\n", err)
					os.Exit(1)
				}

				outputRecordChan <- outputRecord
			} else {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
		}(&record)
	}
	wg.Wait()
	close(outputRecordChan)

	<-stopChan
}
