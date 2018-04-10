package reader

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
)

const (
	KeyS3Region    = "s3_region"
	KeyS3AccessKey = "s3_access_key"
	KeyS3SecretKey = "s3_secret_key"
	KeyS3Bucket    = "s3_bucket"
	KeyS3Prefix    = "s3_prefix"

	KeySyncDirectory  = "sync_directory"
	KeySyncMetastore  = "sync_metastore"
	KeySyncInterval   = "sync_interval"
	KeySyncConcurrent = "sync_concurrent"
)

var (
	ignoredSuffixes = []string{".json.gz"}
)

type ClockTrailReader struct {
	*BufReader
	syncMgr *syncManager
}

func NewClockTrailReader(meta *Meta, conf conf.MapConf) (*ClockTrailReader, error) {
	opts, err := buildSyncOptions(conf)
	if err != nil {
		return nil, err
	}
	syncMgr, err := newSyncManager(opts)
	if err != nil {
		return nil, err
	}
	validFilePattern, _ := conf.GetStringOr(KeyValidFilePattern, "*")
	bufSize, _ := conf.GetIntOr(KeyBufSize, defaultBufSize)
	sf, err := NewSeqFile(meta, opts.directory, true, true, ignoredSuffixes, validFilePattern, WhenceOldest)
	if err != nil {
		return nil, err
	}
	br, err := NewReaderSize(sf, meta, bufSize)
	if err != nil {
		return nil, err
	}

	ctr := &ClockTrailReader{
		BufReader: br,
		syncMgr:   syncMgr,
	}
	go ctr.syncMgr.startSync()

	return ctr, nil
}

func (ctr *ClockTrailReader) Close() error {
	ctr.syncMgr.stopSync()
	return ctr.BufReader.Close()
}

type syncOptions struct {
	region    string
	accessKey string
	secretKey string

	bucket    string
	prefix    string
	directory string
	metastore string

	interval   time.Duration
	concurrent int
}

type configError string

func (err configError) Error() string {
	return "invalid config: " + string(err)
}

func emptyConfigError(key string) error {
	return configError(fmt.Sprintf("%q must not be empty", key))
}

func invalidConfigError(key, value string, err error) error {
	return configError(fmt.Sprintf("invalid value(%q) for key %q: %v", key, value, err))
}

func buildSyncOptions(conf conf.MapConf) (*syncOptions, error) {
	var opts syncOptions
	var err error

	opts.region, _ = conf.GetString(KeyS3Region)
	if opts.region == "" {
		return nil, emptyConfigError(KeyS3Region)
	}
	opts.accessKey, _ = conf.GetString(KeyS3AccessKey)
	if opts.accessKey == "" {
		return nil, emptyConfigError(KeyS3AccessKey)
	}
	opts.secretKey, _ = conf.GetString(KeyS3SecretKey)
	if opts.secretKey == "" {
		return nil, emptyConfigError(KeyS3SecretKey)
	}
	opts.bucket, _ = conf.GetString(KeyS3Bucket)
	if opts.bucket == "" {
		return nil, emptyConfigError(KeyS3Bucket)
	}
	opts.prefix, _ = conf.GetStringOr(KeyS3Prefix, "")
	opts.directory, _ = conf.GetStringOr(KeySyncDirectory, "./data")
	if opts.directory == "" {
		return nil, emptyConfigError(KeySyncDirectory)
	}
	if err = os.MkdirAll(opts.directory, 0755); err != nil {
		return nil, fmt.Errorf("cannot create target directory %q: %v", opts.directory, err)
	}
	opts.metastore, _ = conf.GetStringOr(KeySyncMetastore, "./.metastore")
	if opts.metastore == "" {
		return nil, emptyConfigError(KeySyncMetastore)
	}

	s, _ := conf.GetStringOr(KeySyncInterval, "5m")
	if opts.interval, err = time.ParseDuration(s); err != nil {
		return nil, invalidConfigError(KeySyncInterval, s, err)
	}
	s, _ = conf.GetStringOr(KeySyncConcurrent, "5")
	if opts.concurrent, err = strconv.Atoi(s); err != nil {
		return nil, invalidConfigError(KeySyncInterval, s, err)
	}

	return &opts, nil
}

type syncManager struct {
	*syncOptions

	auth   aws.Auth
	source string

	quit chan struct{}
}

func newSyncManager(opts *syncOptions) (*syncManager, error) {
	auth, err := aws.GetAuth(opts.accessKey, opts.secretKey)
	if err != nil {
		return nil, err
	}
	mgr := &syncManager{
		syncOptions: opts,
		auth:        auth,
		source:      makeSyncSource(opts.bucket, opts.prefix),
		quit:        make(chan struct{}, 0),
	}
	return mgr, nil
}

func makeSyncSource(bucket, prefix string) string {
	if prefix == "" {
		return fmt.Sprintf("s3://%s", bucket)
	}
	if strings.HasPrefix(prefix, "/") {
		return fmt.Sprintf("s3://%s%s", bucket, prefix)
	}
	return fmt.Sprintf("s3://%s/%s", bucket, prefix)
}

func (mgr *syncManager) startSync() {
	ticker := time.NewTicker(mgr.interval)
	defer ticker.Stop()

	if err := mgr.syncOnce(); err != nil {
		log.Errorf("sync failed: %v", err)
	}

Sync:
	for {
		select {
		case <-ticker.C:
			if err := mgr.syncOnce(); err != nil {
				log.Errorf("sync failed: %v", err)
			}
		case <-mgr.quit:
			break Sync
		}
	}

	log.Info("sync stopped working")
}

func (mgr *syncManager) syncOnce() error {
	ctx := &syncContext{
		auth:       mgr.auth,
		source:     mgr.source,
		target:     mgr.directory,
		metastore:  mgr.metastore,
		concurrent: mgr.concurrent,
		region:     mgr.region,
	}
	runner := newSyncRunner(ctx)
	return runner.Sync()
}

func (mgr *syncManager) stopSync() {
	close(mgr.quit)
}

type syncContext struct {
	auth       aws.Auth
	source     string
	target     string
	metastore  string
	concurrent int
	region     string
}

type syncRunner struct {
	*syncContext
}

func newSyncRunner(ctx *syncContext) *syncRunner {
	return &syncRunner{
		syncContext: ctx,
	}
}

func (s *syncRunner) Sync() error {
	if !validSource(s.source) {
		return fmt.Errorf("invalid sync source %q", s.source)
	}
	if !validTarget(s.target) {
		return fmt.Errorf("invalid sync target %q", s.target)
	}
	return s.syncToDir()
}

func validSource(path string) bool {
	return strings.HasPrefix(path, "s3://")
}

func validTarget(target string) bool {
	_, err := os.Stat(target)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func (s *syncRunner) syncToDir() error {
	log.Info("syncing from s3...")

	s3url := newS3Url(s.source)
	bucket, err := lookupBucket(s3url.Bucket(), s.auth, s.region)
	if err != nil {
		return err
	}

	sourceFiles := make(map[string]bool)
	sourceFiles, err = loadS3Files(bucket, s3url.Path(), sourceFiles, "")
	if err != nil {
		return err
	}

	syncedFiles, err := s.loadSyncedFiles()
	if err != nil {
		return err
	}
	err = s.concurrentSyncToDir(s3url, bucket, syncedFiles, sourceFiles)
	if err != nil {
		return err
	}
	return s.storeSyncedFiles(sourceFiles)
}

type s3Url struct {
	Url string
}

func newS3Url(url string) s3Url {
	return s3Url{Url: url}
}

func (r *s3Url) Bucket() string {
	return r.keys()[0]
}

func (r *s3Url) Key() string {
	return strings.Join(r.keys()[1:len(r.keys())], "/")
}

func (r *s3Url) Path() string {
	return r.Key()
}

func (r *s3Url) Valid() bool {
	return strings.HasPrefix(r.Url, "s3://")
}

func (r *s3Url) keys() []string {
	trimmed := strings.TrimPrefix(r.Url, "s3://")
	return strings.Split(trimmed, "/")
}

func lookupBucket(bucketName string, auth aws.Auth, region string) (*s3.Bucket, error) {
	log.Infof("looking for bucket %q in region %q", bucketName, region)

	s3 := s3.New(auth, aws.Regions[region])
	bucket := s3.Bucket(bucketName)
	_, err := bucket.List("", "", "", 0)
	if err == nil {
		log.Infof("found bucket %q in region %q", bucketName, region)
		return bucket, nil
	}
	return nil, fmt.Errorf("list bucket failed: %v", err)
}

func loadS3Files(bucket *s3.Bucket, path string, files map[string]bool, marker string) (map[string]bool, error) {
	log.Infof("loading files from 's3://%s/%s'", bucket.Name, path)

	data, err := bucket.List(path, "", marker, 0)
	if err != nil {
		return files, err
	}

	for _, key := range data.Contents {
		files[key.Key] = true
	}

	if data.IsTruncated {
		lastKey := data.Contents[(len(data.Contents) - 1)].Key
		log.Infof("results truncated, loading additional files via previous last key %q", lastKey)
		loadS3Files(bucket, path, files, lastKey)
	}

	log.Infof("load %d files from 's3://%s/%s' succesfully", len(files), bucket.Name, path)
	return files, nil
}

func (s *syncRunner) loadSyncedFiles() (map[string]bool, error) {
	files := map[string]bool{}

	f, err := os.OpenFile(s.metastore, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return files, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		files[string(line)] = true
	}

	log.Infof("load %d synced files from %q", len(files), s.metastore)

	return files, nil
}

func (s *syncRunner) storeSyncedFiles(files map[string]bool) error {
	f, err := os.OpenFile(s.metastore, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for path := range files {
		w.WriteString(filepath.Base(path))
		w.WriteByte('\n')
	}

	log.Infof("write %d synced files to %q", len(files), s.metastore)

	return w.Flush()
}

func relativePath(path string, filePath string) string {
	if path == "." {
		return strings.TrimPrefix(filePath, "/")
	}
	return strings.TrimPrefix(strings.TrimPrefix(filePath, path), "/")
}

func (s *syncRunner) concurrentSyncToDir(s3url s3Url, bucket *s3.Bucket, targetFiles, sourceFiles map[string]bool) error {
	doneChan := newDoneChan(s.concurrent)
	pool := newPool(s.concurrent)

	var wg sync.WaitGroup
	for s3file := range sourceFiles {
		basename := filepath.Base(s3file)
		unzipPath := strings.TrimSuffix(basename, ".gz")
		if !targetFiles[basename] {
			filePath := strings.Join([]string{s.target, unzipPath}, "/")
			if filepath.Dir(filePath) != "." {
				err := os.MkdirAll(filepath.Dir(filePath), 0755)
				if err != nil {
					return err
				}
			}
			<-pool

			log.Infof("starting sync: s3://%s/%s -> %s", bucket.Name, s3file, filePath)

			wg.Add(1)
			go func(doneChan chan error, filePath string, bucket *s3.Bucket, s3file string) {
				defer wg.Done()
				syncSingleFile(doneChan, filePath, bucket, s3file)
				pool <- 1
			}(doneChan, filePath, bucket, s3file)
		} else {
			log.Infof("skip synced %s", unzipPath)
		}
	}
	wg.Wait()

	log.Info("sync done in this round")
	return nil
}

func syncSingleFile(doneChan chan error, filePath string, bucket *s3.Bucket, file string) {
	err := writeFile(filePath, bucket, file)
	if err != nil {
		doneChan <- err
	}
	log.Infof("sync completed: s3://%s/%s -> %s", bucket.Name, file, filePath)
	doneChan <- nil
}

func writeFile(filename string, bucket *s3.Bucket, path string) error {
	data, err := bucket.Get(path)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, data, os.FileMode(0644))
}

func newPool(concurrent int) chan int {
	pool := make(chan int, concurrent)
	for x := 0; x < concurrent; x++ {
		pool <- 1
	}
	return pool
}

func newDoneChan(concurrent int) chan error {
	doneChan := make(chan error, concurrent)
	go func() {
		for {
			select {
			case err := <-doneChan:
				if err != nil {
					log.Error(err)
				}
			}
		}
	}()
	return doneChan
}
