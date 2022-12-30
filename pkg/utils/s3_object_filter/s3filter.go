package s3objectfilter

import (
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Objectfilterer interface {
	FilterS3ObjectData() error
}

type s3ObjectFilterer struct {
	bucket   string
	key      string
	query    string
	s3Client *s3.S3
}

// create a new s3object filter instance
func NewS3ObjectFilterer(sBucketKey []string, query string, s3Client *s3.S3) s3Objectfilterer {
	return &s3ObjectFilterer{
		bucket:   sBucketKey[0],
		key:      sBucketKey[1],
		query:    query,
		s3Client: s3Client,
	}
}

// main function of s3object filter : filter objects in s3 bucket using sql typed query
func (s3Filter *s3ObjectFilterer) FilterS3ObjectData() error {
	params := &s3.SelectObjectContentInput{
		Bucket:         aws.String(s3Filter.bucket),      // set bucket
		Key:            aws.String(s3Filter.key),         // set Key
		ExpressionType: aws.String(s3.ExpressionTypeSql), // set the expression type to sql
		Expression:     aws.String(s3Filter.query),       // set query as an Expression
		InputSerialization: &s3.InputSerialization{ // set the input serialization
			JSON: &s3.JSONInput{
				Type: aws.String(s3.JSONTypeLines),
			},
			CompressionType: aws.String(s3.CompressionTypeGzip), // set the compression type to GZip
		},
		OutputSerialization: &s3.OutputSerialization{
			JSON: &s3.JSONOutput{},
		},
	}

	resp, err := s3Filter.s3Client.SelectObjectContent(params)
	if err != nil {
		return err
	}
	defer resp.EventStream.Close()

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for event := range resp.EventStream.Events() {
			switch e := event.(type) {
			case *s3.RecordsEvent:
				pw.Write(e.Payload)
			}
		}
	}()

	if _, err := io.Copy(os.Stdout, pr); err != nil {
		return err
	}

	if err := resp.EventStream.Err(); err != nil {
		return fmt.Errorf("failed to read from SelectObjectContent EventStream, %v", err)
	}

	return nil
}
