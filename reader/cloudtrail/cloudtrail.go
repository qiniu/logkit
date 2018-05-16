package cloudtrail

import (
	"archive/zip"
	"bufio"
	"bytes"
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
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/utils/models"
)

func GetDefaultSyncDir(bucket, prefix, region, ak, sk, runnerName string) string {
	return filepath.Join("s3data", "data"+models.Hash(ak+sk+region+bucket+prefix+runnerName))
}

func GetDefaultMetaStore(bucket, prefix, region, ak, sk, runnerName string) string {
	return ".metastore" + models.Hash(ak+sk+region+bucket+prefix+runnerName)
}

var (
	ignoredSuffixes = []string{".json.gz", ".csv.zip"}
)

func init() {
	reader.RegisterConstructor(reader.ModeCloudTrail, NewReader)
}

type Reader struct {
	*reader.BufReader
	syncMgr *syncManager
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	opts, err := buildSyncOptions(conf)
	if err != nil {
		return nil, err
	}
	syncMgr, err := newSyncManager(opts)
	if err != nil {
		return nil, err
	}
	validFilePattern, _ := conf.GetStringOr(reader.KeyValidFilePattern, "*")
	bufSize, _ := conf.GetIntOr(reader.KeyBufSize, reader.DefaultBufSize)
	skipFirstLine, _ := conf.GetBoolOr(reader.KeySkipFileFirstLine, false)
	sf, err := reader.NewSeqFile(meta, opts.directory, true, true, ignoredSuffixes, validFilePattern, reader.WhenceOldest)
	if err != nil {
		return nil, err
	}
	sf.SkipFileFirstLine = skipFirstLine
	br, err := reader.NewReaderSize(sf, meta, bufSize)
	if err != nil {
		return nil, err
	}

	ctr := &Reader{
		BufReader: br,
		syncMgr:   syncMgr,
	}
	go ctr.syncMgr.startSync()

	return ctr, nil
}

func (r *Reader) Reset() (err error) {
	dirErr := os.RemoveAll(r.syncMgr.directory)
	if dirErr != nil && os.IsNotExist(dirErr) {
		dirErr = nil
	}
	metaErr := os.Remove(r.syncMgr.metastore)
	if metaErr != nil && os.IsNotExist(metaErr) {
		metaErr = nil
	}
	if metaErr != nil || dirErr != nil {
		err = fmt.Errorf("reset remove s3 data dir err %v, remove metafile err %v", dirErr, metaErr)
	}
	return
}

func (r *Reader) Close() error {
	log.Debugf("runner[%v] syncMgr.stopSync...", r.Meta.RunnerName)
	r.syncMgr.stopSync()
	log.Debugf("runner[%v] syncMgr closed, wait for BufReader closed...", r.Meta.RunnerName)
	return r.BufReader.Close()
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

func GetS3UserInfo(conf conf.MapConf) (bucket, prefix, region, ak, sk string, err error) {
	region, _ = conf.GetString(reader.KeyS3Region)
	if region == "" {
		err = emptyConfigError(reader.KeyS3Region)
		return
	}
	ak, _ = conf.GetString(reader.KeyS3AccessKey)
	if ak == "" {
		err = emptyConfigError(reader.KeyS3AccessKey)
		return
	}
	sk, _ = conf.GetString(reader.KeyS3SecretKey)
	if sk == "" {
		err = emptyConfigError(reader.KeyS3SecretKey)
		return
	}
	bucket, _ = conf.GetString(reader.KeyS3Bucket)
	if bucket == "" {
		err = emptyConfigError(reader.KeyS3Bucket)
		return
	}
	prefix, _ = conf.GetStringOr(reader.KeyS3Prefix, "")
	return
}

func buildSyncOptions(conf conf.MapConf) (*syncOptions, error) {
	var opts syncOptions
	var err error

	opts.bucket, opts.prefix, opts.region, opts.accessKey, opts.secretKey, err = GetS3UserInfo(conf)
	if err != nil {
		return nil, err
	}
	runnerName, err := conf.GetString(models.KeyRunnerName)
	if err != nil {
		return nil, err
	}
	opts.directory, _ = conf.GetStringOr(reader.KeySyncDirectory, "")
	if opts.directory == "" {
		opts.directory = GetDefaultSyncDir(opts.bucket, opts.prefix, opts.region, opts.accessKey, opts.secretKey, runnerName)
	}
	if err = os.MkdirAll(opts.directory, 0755); err != nil {
		return nil, fmt.Errorf("cannot create target directory %q: %v", opts.directory, err)
	}
	opts.metastore, _ = conf.GetStringOr(reader.KeySyncMetastore, "")
	if opts.metastore == "" {
		opts.metastore = GetDefaultMetaStore(opts.bucket, opts.prefix, opts.region, opts.accessKey, opts.secretKey, runnerName)
	}

	s, _ := conf.GetStringOr(reader.KeySyncInterval, "5m")
	if opts.interval, err = time.ParseDuration(s); err != nil {
		return nil, invalidConfigError(reader.KeySyncInterval, s, err)
	}
	s, _ = conf.GetStringOr(reader.KeySyncConcurrent, "5")
	if opts.concurrent, err = strconv.Atoi(s); err != nil {
		return nil, invalidConfigError(reader.KeySyncInterval, s, err)
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
	syncedFiles map[string]bool
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
	if s.syncedFiles == nil {
		s.syncedFiles, err = s.loadSyncedFiles()
		if err != nil {
			return err
		}
	}

	err = s.concurrentSyncToDir(s3url, bucket, sourceFiles)
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
	if bucketRegionError(err) {
		return nil, fmt.Errorf("bucket %q not in region %q", bucketName, region)
	}
	return nil, fmt.Errorf("list bucket failed: %v", err)
}

func bucketRegionError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "301 response missing Location header") {
		return true
	}
	return false
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
	if len(files) <= 0 {
		return nil
	}

	f, err := os.OpenFile(s.metastore, os.O_WRONLY|os.O_APPEND, 0644)
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

func (s *syncRunner) concurrentSyncToDir(s3url s3Url, bucket *s3.Bucket, sourceFiles map[string]bool) error {
	doneChan := newDoneChan(s.concurrent)
	pool := newPool(s.concurrent)

	var wg sync.WaitGroup
	for s3file := range sourceFiles {
		//对于目录不同步
		if strings.HasSuffix(s3file, string(os.PathSeparator)) {
			delete(sourceFiles, s3file)
			continue
		}
		basename := filepath.Base(s3file)
		unzipPath := strings.TrimSuffix(basename, ".gz")
		if !s.syncedFiles[basename] {
			filePath := strings.Join([]string{s.target, unzipPath}, "/")
			if filepath.Dir(filePath) != "." {
				err := os.MkdirAll(filepath.Dir(filePath), 0755)
				if err != nil {
					return err
				}
			}
			<-pool
			s.syncedFiles[basename] = true

			log.Debugf("starting sync: s3://%s/%s -> %s", bucket.Name, s3file, filePath)

			wg.Add(1)
			go func(doneChan chan error, filePath string, bucket *s3.Bucket, s3file string) {
				defer wg.Done()
				syncSingleFile(doneChan, filePath, bucket, s3file)
				pool <- 1
			}(doneChan, filePath, bucket, s3file)
		} else {
			delete(sourceFiles, s3file)
			log.Debugf("%s already synced, skip it...", unzipPath)
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
	log.Debugf("sync completed: s3://%s/%s -> %s", bucket.Name, file, filePath)
	doneChan <- nil
}

func writeToFile(zipf *zip.File, filename string) error {
	srcF, err := zipf.Open()
	if err != nil {
		return err
	}
	defer srcF.Close()
	distF, err := os.OpenFile(filepath.Join(filepath.Dir(filename), zipf.Name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		return err
	}
	defer distF.Close()
	_, err = io.Copy(distF, srcF)
	if err != nil {
		return err
	}
	return nil
}

func writeFile(filename string, bucket *s3.Bucket, path string) error {
	data, err := bucket.Get(path)
	if err != nil {
		return err
	}
	if strings.HasSuffix(filename, ".zip") {
		rd, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			log.Errorf("reader file %v as zip error %v", filename, err)
			return ioutil.WriteFile(filename, data, os.FileMode(0644))
		}
		var writeErr error
		for _, f := range rd.File {
			err = writeToFile(f, filename)
			if err != nil {
				writeErr = fmt.Errorf("write to %v err %v; %v", f.Name, err, writeErr)
			}
		}
		return writeErr
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
