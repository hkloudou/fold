// Package oss provides an Aliyun OSS (Object Storage Service) backend for
// fold.Storage. It maps the local-style paths Fold computes under its data
// root onto object keys under a configurable prefix, so a Fold DB can keep
// its inc/ workspace and DuckDB staging on the local disk while manifests,
// commit records, and published Parquet segments live in an OSS bucket:
//
//	st, err := oss.New(oss.Config{
//		Region:          "cn-hangzhou",
//		Bucket:          "my-bucket",
//		AccessKeyID:     os.Getenv("OSS_ACCESS_KEY_ID"),
//		AccessKeySecret: os.Getenv("OSS_ACCESS_KEY_SECRET"),
//		Prefix:          "fold",
//		LocalRoot:       "./data",
//	})
//	if err != nil { ... }
//	db, err := fold.Open("./data", fold.WithStorage(st))
//
// LocalRoot must be the same directory passed to fold.Open: it is the part of
// every path that is stripped before the remainder becomes the object key.
// Fold assumes a single writer per table; OSS object writes are atomic
// (last-writer-wins) but Fold does not coordinate concurrent writers across
// machines.
package oss

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	aliyun "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"

	"github.com/hkloudou/fold"
)

// Config describes an OSS-backed fold.Storage.
type Config struct {
	// Region is the OSS region id, e.g. "cn-hangzhou". Required unless a
	// custom Endpoint that implies the region is provided.
	Region string
	// Endpoint optionally overrides the OSS endpoint, e.g.
	// "https://oss-cn-hangzhou-internal.aliyuncs.com" for VPC-internal access.
	Endpoint string
	// Bucket is the bucket name. Required.
	Bucket string
	// AccessKeyID / AccessKeySecret are static credentials. When both are
	// empty the SDK's environment credentials provider is used
	// (OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET / OSS_SESSION_TOKEN).
	AccessKeyID     string
	AccessKeySecret string
	// SecurityToken is the optional STS token accompanying temporary
	// credentials.
	SecurityToken string
	// Prefix is prepended to every object key, e.g. "fold" or "prod/fold".
	// Optional.
	Prefix string
	// LocalRoot is the local data root passed to fold.Open. Required: Fold
	// hands Storage paths under this directory, and the path relative to it
	// becomes the object key (joined to Prefix). Required.
	LocalRoot string
}

// Storage implements fold.Storage on an Aliyun OSS bucket.
type Storage struct {
	client   *aliyun.Client
	uploader *aliyun.Uploader
	bucket   string
	root     string
	prefix   string
}

var _ fold.Storage = (*Storage)(nil)

// New builds an OSS-backed Storage from Config.
func New(cfg Config) (*Storage, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("fold/oss: Config.Bucket is required")
	}
	if cfg.LocalRoot == "" {
		return nil, errors.New("fold/oss: Config.LocalRoot is required (the directory passed to fold.Open)")
	}

	var provider credentials.CredentialsProvider
	if cfg.AccessKeyID != "" || cfg.AccessKeySecret != "" {
		provider = credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.AccessKeySecret, cfg.SecurityToken)
	} else {
		provider = credentials.NewEnvironmentVariableCredentialsProvider()
	}

	sdkCfg := aliyun.LoadDefaultConfig().WithCredentialsProvider(provider)
	if cfg.Region != "" {
		sdkCfg = sdkCfg.WithRegion(cfg.Region)
	}
	if cfg.Endpoint != "" {
		sdkCfg = sdkCfg.WithEndpoint(cfg.Endpoint)
	}

	return NewFromClient(aliyun.NewClient(sdkCfg), cfg.Bucket, cfg.LocalRoot, cfg.Prefix), nil
}

// NewFromClient wraps an existing OSS SDK client. localRoot must be the
// directory passed to fold.Open; prefix (optional) is prepended to every key.
func NewFromClient(client *aliyun.Client, bucket, localRoot, prefix string) *Storage {
	s := &Storage{
		client: client,
		bucket: bucket,
		root:   filepath.Clean(localRoot),
		prefix: strings.Trim(prefix, "/"),
	}
	if client != nil {
		// The uploader switches to multipart upload for files beyond its part
		// size, so published segments are not capped by the 5 GiB single-PUT
		// limit of PutObject.
		s.uploader = client.NewUploader()
	}
	return s
}

// keyFor maps a Fold path under the local root to an object key.
func (s *Storage) keyFor(p string) (string, error) {
	rel, err := filepath.Rel(s.root, filepath.Clean(p))
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("fold/oss: path %q is outside local root %q", p, s.root)
	}
	if rel == "." {
		rel = ""
	}
	return path.Join(s.prefix, filepath.ToSlash(rel)), nil
}

// pathFor maps an object key back to the local-style path Fold expects.
func (s *Storage) pathFor(key string) string {
	rel := strings.TrimPrefix(key, s.prefix)
	rel = strings.TrimPrefix(rel, "/")
	return filepath.Join(s.root, filepath.FromSlash(rel))
}

// List returns every object whose key is under the prefix that corresponds to
// the given path (recursive).
func (s *Storage) List(prefix string) ([]fold.Object, error) {
	key, err := s.keyFor(prefix)
	if err != nil {
		return nil, err
	}
	if key != "" {
		key += "/"
	}
	var objs []fold.Object
	p := s.client.NewListObjectsV2Paginator(&aliyun.ListObjectsV2Request{
		Bucket: aliyun.Ptr(s.bucket),
		Prefix: aliyun.Ptr(key),
	})
	for p.HasNext() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("fold/oss: list %q: %w", key, err)
		}
		for _, o := range page.Contents {
			if o.Key == nil || strings.HasSuffix(*o.Key, "/") {
				continue // skip directory placeholder objects
			}
			objs = append(objs, fold.Object{Path: s.pathFor(*o.Key), Size: o.Size})
		}
	}
	return objs, nil
}

// ReadJSON decodes the JSON object at path into dst. A missing object returns
// an error satisfying errors.Is(err, fs.ErrNotExist).
func (s *Storage) ReadJSON(p string, dst any) error {
	key, err := s.keyFor(p)
	if err != nil {
		return err
	}
	res, err := s.client.GetObject(context.Background(), &aliyun.GetObjectRequest{
		Bucket: aliyun.Ptr(s.bucket),
		Key:    aliyun.Ptr(key),
	})
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("fold/oss: %s: %w", key, fs.ErrNotExist)
		}
		return fmt.Errorf("fold/oss: get %q: %w", key, err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("fold/oss: read %q: %w", key, err)
	}
	return json.Unmarshal(b, dst)
}

// WriteJSON encodes src as JSON at path. An OSS PUT replaces the object
// atomically, which is the property the manifest commit point relies on.
func (s *Storage) WriteJSON(p string, src any, _ fold.WriteOptions) error {
	key, err := s.keyFor(p)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(src, "", "  ")
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(context.Background(), &aliyun.PutObjectRequest{
		Bucket:      aliyun.Ptr(s.bucket),
		Key:         aliyun.Ptr(key),
		Body:        bytes.NewReader(b),
		ContentType: aliyun.Ptr("application/json"),
	})
	if err != nil {
		return fmt.Errorf("fold/oss: put %q: %w", key, err)
	}
	return nil
}

// UploadFile publishes a finished local file to dst and removes the local
// staging copy on success, mirroring the local backend's rename semantics.
// Uploads go through the SDK uploader, which uses multipart upload for large
// files, so merged segments are not capped by the 5 GiB single-PUT limit.
func (s *Storage) UploadFile(localPath, dst string) error {
	key, err := s.keyFor(dst)
	if err != nil {
		return err
	}
	_, err = s.uploader.UploadFile(context.Background(), &aliyun.PutObjectRequest{
		Bucket: aliyun.Ptr(s.bucket),
		Key:    aliyun.Ptr(key),
	}, localPath)
	if err != nil {
		return fmt.Errorf("fold/oss: upload %q: %w", key, err)
	}
	return os.Remove(localPath)
}

// DownloadFile fetches an object into the local workspace.
func (s *Storage) DownloadFile(src, localPath string) error {
	key, err := s.keyFor(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return err
	}
	_, err = s.client.GetObjectToFile(context.Background(), &aliyun.GetObjectRequest{
		Bucket: aliyun.Ptr(s.bucket),
		Key:    aliyun.Ptr(key),
	}, localPath)
	if err != nil {
		return fmt.Errorf("fold/oss: download %q: %w", key, err)
	}
	return nil
}

// Delete removes an object. Deleting a missing object is not an error (OSS
// DeleteObject is idempotent).
func (s *Storage) Delete(p string) error {
	key, err := s.keyFor(p)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(context.Background(), &aliyun.DeleteObjectRequest{
		Bucket: aliyun.Ptr(s.bucket),
		Key:    aliyun.Ptr(key),
	}); err != nil {
		return fmt.Errorf("fold/oss: delete %q: %w", key, err)
	}
	return nil
}

func isNotFound(err error) bool {
	var serr *aliyun.ServiceError
	if errors.As(err, &serr) {
		return serr.StatusCode == 404 || serr.Code == "NoSuchKey"
	}
	return false
}
