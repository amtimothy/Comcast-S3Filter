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
	input    string // An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered. (Required)
	fromTime string // An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected.
	toTime   string // An RFC3339 timestamp that represents the latest `time` of JSON object to be selected.
	withWord string // A string containing a word that must be contained in `words` of a JSON objec to be selected.
	withId   int    // An integer that contains the `id` of a JSON object to be selected.
)

func init() {
	// CLI flag for input
	flag.StringVar(&input, "input", "",
		"An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered. (Required)")

	// CLI flag for withId
	flag.IntVar(&withId, "with-id", 0,
		"An integer that contains the `id` of a JSON object to be selected.")

	// CLI flag for fromTime
	flag.StringVar(&fromTime, "from-time", "",
		"An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected.")

	// CLI flag for toTime
	flag.StringVar(&toTime, "to-time", "",
		"An RFC3339 timestamp that represents the latest `time` of JSON object to be selected.")

	// CLI flag for withWord
	flag.StringVar(&withWord, "with-word", "",
		"A string containing a word that must be contained in `words` of a JSON objec to be selected.")

	// To show the usage of each flag
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func main() {
	flag.Parse()

	// If the input flag is empty, the show the usage of each flag and exit
	if len(input) == 0 {
		flag.Usage()
	}

	// Split the input to separate into bucket and key
	sBucketKey, err := parseInputURI()
	if err != nil {
		// if there is sth wrong with input, prints the error and the usage of each flag
		log.Printf("Error: %s", err)
		flag.Usage()
	}

	// Build query to fetch JSON objects
	query, err := buildQuery()
	if err != nil {
		log.Printf("Error: %s", err)
		flag.Usage()
	}

	// create a new s3 client
	s3Client, err := newS3Client()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	// create a new s3 filterer and do filtering with bucket, key and query
	err = filter.NewS3ObjectFilterer(sBucketKey, query, s3Client).FilterS3ObjectData()
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}

// create a new s3 client with new session
func newS3Client() (*s3.S3, error) {
	// create a aws session
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("error creating session %s", err)
	}

	return s3.New(sess), nil
}

// a function that gets the bucket and key from the input
func parseInputURI() ([]string, error) {
	const s3Protocol = "s3://"
	const s3URIErrMsg = "-input s3 URI value is invalid"

	// if the input does not start with "s3://" then bad bad input.
	if !strings.HasPrefix(input, s3Protocol) {
		return nil, errors.New(s3URIErrMsg)
	}

	// split the input to separated bucket and key
	sBucketKey := strings.Split(strings.Replace(input, s3Protocol, "", 1), "/")
	if len(sBucketKey) != 2 || len(sBucketKey[0]) == 0 || len(sBucketKey[1]) == 0 {
		return nil, errors.New(s3URIErrMsg)
	}

	return sBucketKey, nil
}

// a function for building a query for fetching json data
func buildQuery() (string, error) {

	query := "SELECT s.id, s.\"time\", s.words FROM S3Object s "
	where := "WHERE"
	and := "AND"

	// if withId flag is set, generate where query with 'withId'
	if withId != 0 {
		query += "WHERE s.id = " + fmt.Sprint(withId)
	}

	// if fromTime flag is set, generate TO_TIMESTAMP query with `fromTime`
	if len(fromTime) != 0 {
		var ft string

		// parse string to time using parse function in RFC3339 format
		if _, err := time.Parse(time.RFC3339, fromTime); err != nil {
			return "", fmt.Errorf("-from-time value is invalid - %s", err)
		}

		// add Query
		fromTime = " TO_TIMESTAMP(s.\"time\") >= TO_TIMESTAMP('" + fromTime + "') "
		if !strings.Contains(query, where) {
			ft = where
		} else {
			ft = and
		}
		ft += fromTime
		query += ft
	}

	// if toTime flag is set, generate TO_TIMESTAMP query with `toTime`
	if len(toTime) != 0 {
		var tt string

		// parse string to time using parse function in RFC3339 formats
		if _, err := time.Parse(time.RFC3339, toTime); err != nil {
			return "", fmt.Errorf("-to-time value is invalid - %s", err)
		}

		// add Query
		toTime = " TO_TIMESTAMP(s.\"time\") <= TO_TIMESTAMP('" + toTime + "') "
		if !strings.Contains(query, where) {
			tt = where
		} else {
			tt = and
		}
		tt += toTime
		query += tt
	}

	// if withWord flag is set, generate a proper query with withWord
	if len(withWord) != 0 {
		var ww string

		// add Query
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
