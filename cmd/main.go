package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	filter "s3filter/pkg/utils/s3_object_filter"
)

var (
	input    string
	fromTime string
	toTime   string
	withWord string
	withId   int
)

func init() {
	flag.StringVar(&input, "input", "",
		"An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered. (Required)")

	flag.IntVar(&withId, "with-id", 0,
		"An integer that contains the `id` of a JSON object to be selected.")

	flag.StringVar(&fromTime, "from-time", "",
		"An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected.")

	flag.StringVar(&toTime, "to-time", "",
		"An RFC3339 timestamp that represents the latest `time` of JSON object to be selected.")

	flag.StringVar(&withWord, "with-word", "",
		"A string containing a word that must be contained in `words` of a JSON objec to be selected.")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func main() {
	flag.Parse()

	if len(input) == 0 {
		flag.Usage()
	}

	sBucketKey, err := parseInputURI()
	if err != nil {
		log.Printf("Error: %s", err)
		flag.Usage()
	}

	query, err := buildQuery()
	if err != nil {
		log.Printf("Error: %s", err)
		flag.Usage()
	}

	s3Client, err := newS3Client()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	err = filter.NewS3ObjectFilterer(sBucketKey, query, s3Client).FilterS3ObjectData()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}

func newS3Client() (*s3.S3, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("error creating session %s", err)
	}

	return s3.New(sess), nil
}

func parseInputURI() ([]string, error) {
	const s3Protocol = "s3://"
	const s3URIErrMsg = "-input s3 URI value is invalid"

	if !strings.HasPrefix(input, s3Protocol) {
		return nil, errors.New(s3URIErrMsg)
	}

	sBucketKey := strings.Split(strings.Replace(input, s3Protocol, "", 1), "/")
	if len(sBucketKey) != 2 || len(sBucketKey[0]) == 0 || len(sBucketKey[1]) == 0 {
		return nil, errors.New(s3URIErrMsg)
	}

	return sBucketKey, nil
}

func buildQuery() (string, error) {

	query := "SELECT s.id, s.\"time\", s.words FROM S3Object s "
	where := "WHERE"
	and := "AND"

	if withId != 0 {
		query += "WHERE s.id = " + fmt.Sprint(withId)
	}

	if len(fromTime) != 0 {
		var ft string

		if _, err := time.Parse(time.RFC3339, fromTime); err != nil {
			return "", fmt.Errorf("-from-time value is invalid - %s", err)
		}

		fromTime = " TO_TIMESTAMP(s.\"time\") >= TO_TIMESTAMP('" + fromTime + "') "
		if !strings.Contains(query, where) {
			ft = where
		} else {
			ft = and
		}
		ft += fromTime
		query += ft
	}

	if len(toTime) != 0 {
		var tt string

		if _, err := time.Parse(time.RFC3339, toTime); err != nil {
			return "", fmt.Errorf("-to-time value is invalid - %s", err)
		}

		toTime = " TO_TIMESTAMP(s.\"time\") <= TO_TIMESTAMP('" + toTime + "') "
		if !strings.Contains(query, where) {
			tt = where
		} else {
			tt = and
		}
		tt += toTime
		query += tt
	}

	if len(withWord) != 0 {
		var ww string

		withWord = " '" + withWord + "' IN s.words"

		if !strings.Contains(query, where) {
			ww = where
		} else {
			ww = and
		}
		ww += withWord
		query += ww
	}

	return query, nil
}
