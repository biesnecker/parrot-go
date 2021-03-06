package main

import (
	"crypto/sha1"
	"encoding/csv"
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

	Region string `short:"r" long:"region" description:"The AWS region to call" default:"us-west-2"`
}

func printErrAndExit(err error) {
	fmt.Fprintf(os.Stderr, "%v", err)
	os.Exit(1)
}

type fetchAudioParams struct {
	pollyClient *polly.Polly
	rateLimiter ratelimit.Limiter
	waitGroup   *sync.WaitGroup
}

func fetchAudio(
	text string,
	languageCode string,
	voice string,
	useNeural bool,
	audioFilepath string,
	params *fetchAudioParams,
) {
	defer params.waitGroup.Done()
	input := &polly.SynthesizeSpeechInput{
		OutputFormat: aws.String("mp3"),
		Text:         aws.String(text),
		VoiceId:      aws.String(voice),
		LanguageCode: aws.String(languageCode)}

	if useNeural {
		input.Engine = aws.String(polly.EngineNeural)
	} else {
		input.Engine = aws.String(polly.EngineStandard)
	}

	params.rateLimiter.Take()
	pollyResponse, err := params.pollyClient.SynthesizeSpeech(input)
	if err != nil {
		printErrAndExit(err)
	}
	outputFile, err := os.Create(audioFilepath)
	if err != nil {
		printErrAndExit(err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, pollyResponse.AudioStream)
	if err != nil {
		printErrAndExit(err)
	}
}

func main() {
	var options opts

	var parser = flags.NewParser(&options, flags.Default)
	if _, err := parser.Parse(); err != nil {
		if flagErr, ok := err.(*flags.Error); ok && flagErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		os.Exit(1)
	}

	sess := session.Must(session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config:            aws.Config{Region: aws.String(options.Region)},
		}))

	pollyClient := polly.New(sess)

	var maxRequestsPerSecond int
	if options.Neural {
		maxRequestsPerSecond = 8
	} else {
		maxRequestsPerSecond = 80
	}

	seen := make(map[string]int)

	inputfile, err := os.Open(options.Input)
	if err != nil {
		printErrAndExit(err)
	}
	defer inputfile.Close()

	outputfile, err := os.Create(options.Output)
	if err != nil {
		printErrAndExit(err)
	}
	defer outputfile.Close()

	csvreader := csv.NewReader(inputfile)
	csvwriter := csv.NewWriter(outputfile)

	fetchParams := fetchAudioParams{
		pollyClient: pollyClient,
		waitGroup:   &sync.WaitGroup{},
		rateLimiter: ratelimit.New(maxRequestsPerSecond),
	}

	lineNo := 0
	numColumns := -1
	for {
		lineNo++
		record, err := csvreader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			printErrAndExit(err)
		}

		recordLen := len(record)
		if recordLen == 0 {
			printErrAndExit(fmt.Errorf(
				"empty record found on line %d",
				lineNo))
		}

		// If this is the first line, then set the expected columns. All lines
		// should have the same number of columns.
		if numColumns == -1 {
			numColumns = recordLen
		} else if numColumns != recordLen {
			printErrAndExit(
				fmt.Errorf(
					"expected %d columns but found %d columns on line %d",
					numColumns,
					recordLen,
					lineNo))
		}

		if lastSeenLineNo, ok := seen[record[0]]; ok {
			printErrAndExit(
				fmt.Errorf(
					"duplicate \"%s\" found on line %d, previously on line %d",
					record[0],
					lineNo,
					lastSeenLineNo))
		} else {
			seen[record[0]] = lineNo
		}

		// Figure out what the audio filename and path should be.
		h := sha1.New()
		h.Write([]byte(record[0]))

		audioFilename := fmt.Sprintf("%x.mp3", h.Sum(nil))
		audioFilepath := filepath.Join(options.AudioOut, audioFilename)
		outputRecord := append(record, audioFilename)

		if _, err := os.Stat(audioFilepath); err == nil {
			// File exists. Just write the output and we're done.
			csvwriter.Write(outputRecord)
			continue
		} else if errors.Is(err, os.ErrNotExist) {
			// File doesn't exist, so spawn the job to fetch it.
			fetchParams.waitGroup.Add(1)
			go fetchAudio(
				record[0],
				options.Language,
				options.Voice,
				options.Neural,
				audioFilepath,
				&fetchParams,
			)
			csvwriter.Write(outputRecord)
		} else {
			// Some other error.
			printErrAndExit(err)
		}
	}

	csvwriter.Flush()
	fetchParams.waitGroup.Wait()
}
