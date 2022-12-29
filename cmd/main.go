package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	aws_region := os.Getenv("AWS_REGION")
	aws_access_key_id := os.Getenv("AWS_ACCESS_KEY_ID")
	aws_secret_access_key := os.Getenv("AWS_SECRET_ACCESS_KEY")

	s3Url := flag.String("input", "", "An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered.")
	resCnt := flag.Int("with-id", 0, "An integer that contains the `id` of a JSON object to be selected.")
	startTime := flag.String("from-time", "", "An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected.")
	endTime := flag.String("to-time", "", "An RFC3339 timestamp that represents the latest `time` of a JSON object to be selected.")
	contains := flag.String("with-word", "", "A string containing a word that must be contained in `words` of a JSON object to be selected.")

	fmt.Printf("Env files\nAWS_REGION : %v\nAWS_ACCESS_KEY_ID : %v\nAWS_SECRET_ACCESS_KEY : %v\nS3URL : %v\nRESCNT : %v\nSTARTTIME : %v\nENDTIME : %v\nCONTAINs : %v\n ",
		aws_region, aws_access_key_id, aws_secret_access_key, s3Url, resCnt, startTime, endTime, contains,
	)
}
