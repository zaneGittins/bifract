package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3PartSize is the size of each multipart upload part (10 MB).
// S3 minimum is 5 MB (except last part); 10 MB balances memory use and request count.
const s3PartSize = 10 * 1024 * 1024

// S3Storage implements StorageBackend using S3-compatible object storage.
type S3Storage struct {
	client *s3.Client
	bucket string
}

// NewS3Storage creates an S3-compatible storage backend.
func NewS3Storage(cfg StorageConfig) (*S3Storage, error) {
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}

	region := cfg.S3Region
	if region == "" {
		region = "us-east-1"
	}

	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		// Fast failure on bad S3 config (wrong endpoint, firewall, etc.)
		// These only affect connection setup, not data transfer.
		t.DialContext = (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
		t.TLSHandshakeTimeout = 10 * time.Second
		t.ResponseHeaderTimeout = 30 * time.Second
	})

	opts := func(o *s3.Options) {
		o.Region = region
		o.HTTPClient = httpClient
		o.Credentials = credentials.NewStaticCredentialsProvider(
			cfg.S3AccessKey,
			cfg.S3SecretKey,
			"",
		)
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
			o.UsePathStyle = true // Required for MinIO, DO Spaces, etc.
		}
	}

	client := s3.New(s3.Options{}, opts)

	return &S3Storage{
		client: client,
		bucket: cfg.S3Bucket,
	}, nil
}

func (s *S3Storage) Write(ctx context.Context, path string, r io.Reader) (int64, error) {
	// Use multipart upload to stream data without buffering the entire
	// archive in memory. Each part is 10 MB except the final part.
	createResp, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return 0, fmt.Errorf("S3 create multipart upload: %w", err)
	}
	uploadID := createResp.UploadId

	var completedParts []types.CompletedPart
	var partNumber int32 = 1
	var totalBytes int64
	buf := make([]byte, s3PartSize)

	for {
		// Fill buffer to s3PartSize (or EOF)
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			uploadResp, uploadErr := s.client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:        aws.String(s.bucket),
				Key:           aws.String(path),
				UploadId:      uploadID,
				PartNumber:    aws.Int32(partNumber),
				Body:          bytes.NewReader(buf[:n]),
				ContentLength: aws.Int64(int64(n)),
			})
			if uploadErr != nil {
				s.abortUpload(path, uploadID)
				return 0, fmt.Errorf("S3 upload part %d: %w", partNumber, uploadErr)
			}

			completedParts = append(completedParts, types.CompletedPart{
				ETag:       uploadResp.ETag,
				PartNumber: aws.Int32(partNumber),
			})
			partNumber++
			totalBytes += int64(n)
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			s.abortUpload(path, uploadID)
			return 0, fmt.Errorf("read data for S3 upload: %w", readErr)
		}
	}

	// Handle empty file: S3 requires at least one part
	if len(completedParts) == 0 {
		uploadResp, uploadErr := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(path),
			UploadId:      uploadID,
			PartNumber:    aws.Int32(1),
			Body:          bytes.NewReader(nil),
			ContentLength: aws.Int64(0),
		})
		if uploadErr != nil {
			s.abortUpload(path, uploadID)
			return 0, fmt.Errorf("S3 upload empty part: %w", uploadErr)
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(1),
		})
	}

	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(path),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("S3 complete multipart upload: %w", err)
	}

	return totalBytes, nil
}

// abortUpload cancels an incomplete multipart upload. Uses a fresh background
// context so the abort succeeds even if the original operation's context was
// cancelled (which would otherwise leave orphaned incomplete uploads in S3).
func (s *S3Storage) abortUpload(path string, uploadID *string) {
	s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(path),
		UploadId: uploadID,
	})
}

func (s *S3Storage) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, fmt.Errorf("S3 get object: %w", err)
	}
	return out.Body, nil
}

func (s *S3Storage) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return fmt.Errorf("S3 delete object: %w", err)
	}
	return nil
}

func (s *S3Storage) List(ctx context.Context, prefix string) ([]StoredFile, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	var files []StoredFile
	paginator := s3.NewListObjectsV2Paginator(s.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("S3 list objects: %w", err)
		}
		for _, obj := range page.Contents {
			files = append(files, StoredFile{
				Path:     aws.ToString(obj.Key),
				Size:     aws.ToInt64(obj.Size),
				Modified: aws.ToTime(obj.LastModified),
			})
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Modified.After(files[j].Modified)
	})

	return files, nil
}
