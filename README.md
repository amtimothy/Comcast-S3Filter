# Filtering JSON from S3

## Introduction

The Comcast MAF (Media Analysis Framework) processes many types of media as well as metadata that is extracted from media. Most of the metadata is represented as a sequence of [newline-delimited JSON](http://ndjson.org) objects. This simply means that the JSON objects are encoded without newlines (`'\n'`), and are separated by either a newline (`'\n'`) or a carriage-return (`'\r'`) followed by a newline (`'\n'`).

This technical assessment will evaluate your skills in [The Go Programming Language](https://golang.org), your ability to work with the [AWS SDK for Go](https://aws.amazon.com/sdk-for-go), and your ability to create a [Docker](https://docker.com).

## Data

There are a number of objects in [AWS S3](https://aws.amazon.com/s3) that contain newline-delimited JSON data. Each JSON object contains several fields:

| Name | JSON Type | Description |
| ---- | ---- | ------------|
| `id` | `number` | A randomly generated 64-bit integer identifier. |
| `time` | `string` | A string containing an [RFC3339](https://tools.ietf.org/html/rfc3339) timestamp. |
| `words` | `array` | An array of strings containing randomly selected English words. |

Some examples of data:

```json
{"id":5559641812032348625,"time":"1973-03-26T06:18:27.894733102-08:00","words":["scarfing","chip","nuke"]}
{"id":2700778169311697635,"time":"2000-04-12T06:50:23.866113335-06:00","words":["bouncier"]}
{"id":3622438848630508647,"time":"1994-04-15T11:12:04.921735153-06:00","words":["payoffs","winters"]}
{"id":6022900352107475984,"time":"2008-11-24T05:28:08.519322526-08:00","words":["astringency","entertain"]}
```

The objects in S3 are GZIP compressed, and can be arbitrarily large up to the S3 size limit (5 TiB).

## S3 Inputs

| S3 URI | Number of JSON |
| ------ | -------------- |
| s3://maf-sample-data/1k.ndjson.gz | 1000 |
| s3://maf-sample-data/10k.ndjson.gz | 10000 |
| s3://maf-sample-data/100k.ndjson.gz | 100000 |
| s3://maf-sample-data/1M.ndjson.gz | 1000000 |

## Task

Your task for this exercise is to build a utility in Go, packaged as an executable Docker image, that filters the data from an object in S3, and prints the result to `stdout`.

## Flags

The utility that you develop must support the following flags:

| Name | Required | Description |
| ---- | -------- | ----------- |
| `-input` | Yes | An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered. |
| `-with-id` | No | An integer that contains the `id` of a JSON object to be selected. |
| `-from-time` | No | An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected. |
| `-to-time` | No | An RFC3339 timestamp that represents the latest `time` of JSON object to be selected. |
| `-with-word` | No | A string containing a word that must be contained in `words` of a JSON objec to be selected. |

If no `-input` flag is present, the utility should print a usage message and exit with a non-zero status. If only the `-input` flag is present, the output of the S3 object should be written to `stdout`. The `-with-id` flag should select only JSON objects that have the specified `id` field. The `-from-time` flag should select only objects with `time` fields greater than or equal to the specified value. The `-to-time` flag should select only objects with `time` fields less than the specified value. Finally, the `-with-word` flag should select only object where the `words` array contains the specified word. If multiple filtering flags are provided, the conjunction (AND) of the conditions should be applied.

## Examples

Docker command:
```bash
docker run --rm -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY ronlenk/s3filter -input s3://maf-sample-data/1k.ndjson.gz -from-time=2000-01-01T00:00:00Z -to-time=2001-01-01T00:00:00Z
```
Output:
```json
{"id":436652077101148683,"time":"2000-08-23T05:05:53.904157946-05:00","words":["diver","yank","cartier"]}
{"id":7892463464442546726,"time":"2000-11-10T05:42:26.358716064Z","words":["horrify","jimmied","jeep"]}
{"id":5829526662594237344,"time":"2000-12-09T06:36:44.431714072-06:00","words":["chris","negations"]}
{"id":1445223536067301509,"time":"2000-08-22T22:41:37.9169669-05:00","words":["malediction","unionization","libellous","changelings","horsehide","implausibilities","theological","assertions"]}
{"id":5769359701490350650,"time":"2000-01-11T07:21:44.208970052Z","words":["harassment","congratulatory","recruitment","informed","tammie"]}
{"id":3549852124943271274,"time":"2000-03-11T05:00:01.996838102-08:00","words":["tortilla","adversarial"]}
{"id":1785244842879564889,"time":"2000-11-07T10:52:15.905112892-05:00","words":["professorship","cords"]}
{"id":6678614626173260417,"time":"2000-03-22T22:36:42.102961373-08:00","words":["abracadabra","car","vectored","precede","sixth","sitars"]}
{"id":2368808962129832106,"time":"2000-12-26T21:33:43.592791792-05:00","words":["innards","boobing","buckskins","labeling","level","polecat"]}
{"id":1441449964174188806,"time":"2000-11-30T06:01:35.675470557Z","words":["typographers","michiganders","versions","snappish","pounds","antenna","appalls"]}
{"id":915302341850787053,"time":"2000-02-24T09:23:38.131092229-05:00","words":["heartbreak","biassing","milagros","ionizing","unsafe","writes","fizziest","blowziest","smoggy"]}
{"id":7394723564412751784,"time":"2000-05-05T02:38:49.775023814-04:00","words":["mercenaries","wanes","juneau","gravies","rampant","rings"]}
{"id":4020826120341196720,"time":"2000-12-25T10:41:05.047117235Z","words":["rodeos","prematurely"]}
{"id":8808067477585436271,"time":"2000-05-20T14:00:54.914556308Z","words":["etcher","bilge","helen","dunant","cowper","baghdad"]}

```
Docker command:
```bash
docker run --rm -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY ronlenk/s3filter -input s3://maf-sample-data/1M.ndjson.gz -from-time=2000-01-01T00:00:00Z -to-time=2001-01-01T00:00:00Z --with-word=payoff
```
Output:
```json
{"id":1922314047597168442,"time":"2000-12-18T01:59:08.326132627-07:00","words":["noblemen","drumstick","defers","brainstorms","payoff","loosely","backhanded","heehaw"]}
{"id":8838668320865922731,"time":"2000-02-25T02:09:22.103969193-06:00","words":["cooperative","centrals","copulae","payoff","pianist","recoverable","adjudicate","iodine","paddies"]}
{"id":6005224023315071911,"time":"2000-08-01T02:37:03.473446189-06:00","words":["martin","misjudges","devotionals","plowman","payoff","rods","oppressors"]}
{"id":32342107759780325,"time":"2000-05-10T04:07:41.051889377-06:00","words":["payoff","dunn","cads","spurred","debra","its","colluding","poodle"]}
{"id":8427651505142399022,"time":"2000-10-19T06:33:56.810505527-07:00","words":["ayurveda","cicadae","renters","impulsed","goddesses","flysheet","payoff"]}
{"id":4706769799750409419,"time":"2000-09-27T00:20:04.451818891-05:00","words":["absorbs","payoff","phantoms","srivijaya","geneticists","protestant","minibikes"]}
{"id":6270481945295025728,"time":"2000-08-10T08:39:17.782437091-05:00","words":["sleuths","payoff","skullduggery","unsettles","vending","menials","annoy","hooded","placebos"]}
{"id":3733108737347584344,"time":"2000-03-14T13:21:27.80330593-05:00","words":["solstice","payoff","wavelet","misprints","quieter","hepatitis","stimulation"]}
{"id":8077963365064656080,"time":"2000-12-25T09:01:02.504615028Z","words":["guttersnipe","afterword","plinth","alimony","payoff","counterclaim","engulfed","snivelled"]}
{"id":4881200486829219737,"time":"2000-01-15T10:18:18.594970086-07:00","words":["angels","moistness","payoff","autocracy","laundryman","tonalities"]}
{"id":6502534963868020453,"time":"2000-07-08T19:49:13.885398703-05:00","words":["airways","mortised","payoff","entity","inexpensive","byline"]}
{"id":4076246218738946002,"time":"2000-01-13T22:01:02.497968282-08:00","words":["loadstone","closer","redirecting","immortality","selfishness","ameer","payoff"]}
```

## Requirements
1. You must write the utility in Go and compile with version `1.13` or later.
2. You must use Go modules for dependency management.
3. You may use packages from the Go standard library, and the AWS SDK for Go or the [Minio Go Client API](https://docs.min.io/docs/golang-client-api-reference.html)
4. You must use the standard [AWS SDK mechanism(s) for obtaining credentials](https://docs.aws.amazon.com/sdk-for-go/api/aws/session). In other words, just do `session.NewSession()`, and let the SDK do the rest.
5. You must provide a `Dockerfile` that builds an executable container image for your utility.
6. Your utility must *not* use local storage within the container.
7. You can either email a (`.tar.gz` or `.zip`) archive of the source code for your utility to Andy_Kaiser@comcast.com, Leyna_Michela@comcast.com, and Srikanth_Kanuri@comcast.com or check your code into a *private* GitHub repository shared with [gakaiser](https://github.com/gakaiser), [mickbytes](https://github.com/mickbytes), and [theroadnotaken](https://github.com/theroadnotaken) as collaborators. (A GitHub repository is the preferred method.)

## Environment

| Name | Value |
| ---- | ----- |
| `AWS_REGION` | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | `AKIA2IUTP2UD55QMFPVC` |
| `AWS_SECRET_ACCESS_KEY` | `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` |

## User Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::maf-sample-data/*"
        }
    ]
}
```
