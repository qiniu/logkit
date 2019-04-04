package cli

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	utilsos "github.com/qiniu/logkit/utils/os"
	"github.com/stretchr/testify/assert"
)

func TestParseCliVersion(t *testing.T) {
	testData := []struct {
		input string
		exp   CliVersion
	}{
		{
			input: "v1",
			exp: CliVersion{
				Major:    1,
				Minor:    0,
				Revision: 0,
			},
		},
		{
			input: "v1.0",
			exp: CliVersion{
				Major:    1,
				Minor:    0,
				Revision: 0,
			},
		},
		{
			input: "v1.0.0",
			exp: CliVersion{
				Major:    1,
				Minor:    0,
				Revision: 0,
			},
		},
		{
			input: "v1.0.1",
			exp: CliVersion{
				Major:    1,
				Minor:    0,
				Revision: 1,
			},
		},
		{
			input: "v1.2",
			exp: CliVersion{
				Major:    1,
				Minor:    2,
				Revision: 0,
			},
		},
		{
			input: "v1.2.0",
			exp: CliVersion{
				Major:    1,
				Minor:    2,
				Revision: 0,
			},
		},
		{
			input: "v1.2.3",
			exp: CliVersion{
				Major:    1,
				Minor:    2,
				Revision: 3,
			},
		},
	}

	for _, val := range testData {
		got, err := parseCliVersion(val.input)
		assert.NoError(t, err)
		assert.Equal(t, val.exp, got)
	}
}

func TestIsUpgradeNeeded(t *testing.T) {
	testData := []struct {
		cur    string
		latest string
		exp    bool
	}{
		{
			cur:    "v1",
			latest: "v1",
			exp:    false,
		},
		{
			cur:    "v1.1",
			latest: "v1",
			exp:    false,
		},
		{
			cur:    "v2",
			latest: "v1.1",
			exp:    false,
		},
		{
			cur:    "v1.1.0",
			latest: "v1.1",
			exp:    false,
		},
		{
			cur:    "v1.2.1",
			latest: "v1.2.2",
			exp:    true,
		},
		{
			cur:    "v1.2.3",
			latest: "v1.2.4",
			exp:    true,
		},
		{
			cur:    "v1.0.1",
			latest: "v1.0",
			exp:    false,
		},
		{
			cur:    "v1.9.9",
			latest: "v2.0.0",
			exp:    true,
		},
		{
			cur:    "v1.0.2",
			latest: "v1.0.1",
			exp:    false,
		},
		{
			cur:    "v1.0.1",
			latest: "v1.1.0",
			exp:    true,
		},
	}
	for _, val := range testData {
		got, err := isUpgradeNeeded(val.cur, val.latest)
		assert.NoError(t, err)
		assert.Equal(t, val.exp, got)
	}
}

func TestGetPackNameByKernelPlatform(t *testing.T) {
	macName := "logkit_mac_%s.tar.gz"
	linux32 := "logkit_linux32_%s.tar.gz"
	linux64 := "logkit_%s.tar.gz"
	win32 := "logkit_windows32_%s.zip"
	win64 := "logkit_windows_%s.zip"
	testData := []struct {
		kernel   string
		platform string
		version  string
		packName string
	}{
		{
			kernel:   GoOSLinux,
			platform: Arch386,
			version:  "v1.3.2",
			packName: linux32,
		},
		{
			kernel:   GoOSLinux,
			platform: Arch64,
			version:  "v1.3.2",
			packName: linux64,
		},
		{
			kernel:   GoOSWindows,
			platform: Arch386,
			version:  "v1.3.2",
			packName: win32,
		},
		{
			kernel:   GoOSWindows,
			platform: Arch64,
			version:  "v1.3.2",
			packName: win64,
		},
		{
			kernel:   GoOSMac,
			platform: Arch386,
			version:  "v1.3.2",
			packName: macName,
		},
	}

	for _, val := range testData {
		got, err := getPackNameByKernelPlatform(val.kernel, val.platform, val.version)
		exp := fmt.Sprintf(val.packName, val.version)
		assert.NoError(t, err)
		assert.Equal(t, exp, got)
	}
}

func TestMoveFiles(t *testing.T) {
	fileName1 := "file1"
	fileName2 := "file2"
	dirPathName1 := "test1"
	dirPathName2 := "test2"
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	dir := "TestMoveFiles"
	rootDir := filepath.Join(pwd, dir)
	dirPath1 := filepath.Join(rootDir, dirPathName1)
	dirPath2 := filepath.Join(rootDir, dirPathName2)
	filePathSrc1 := filepath.Join(dirPath1, fileName1)
	filePathSrc2 := filepath.Join(dirPath1, fileName2)
	filePathDst1 := filepath.Join(dirPath2, fileName1)
	filePathDst2 := filepath.Join(dirPath2, fileName2)
	os.RemoveAll(rootDir)
	if err = os.Mkdir(rootDir, DefaultDirPerm); err != nil {
		t.Fatalf("mmake dir %v error, %v", rootDir, err)
	}
	if err = os.Mkdir(dirPath1, DefaultDirPerm); err != nil {
		t.Fatalf("mmake dir %v error, %v", rootDir, err)
	}
	if err = os.Mkdir(dirPath2, DefaultDirPerm); err != nil {
		t.Fatalf("mmake dir %v error, %v", rootDir, err)
	}
	defer os.RemoveAll(rootDir)

	txt := "123456"
	if err := ioutil.WriteFile(filePathSrc1, []byte(txt), 0666); err != nil {
		t.Fatalf("write file error %v", err)
	}
	if err := ioutil.WriteFile(filePathSrc2, []byte(txt), 0666); err != nil {
		t.Fatalf("write file error %v", err)
	}
	_, err = os.Stat(filePathSrc1)
	assert.NoError(t, err)
	_, err = os.Stat(filePathSrc2)
	assert.NoError(t, err)

	err = moveFile(dirPath1, []string{fileName1, fileName2}, dirPath2)
	assert.NoError(t, err)

	_, err = os.Stat(filePathSrc1)
	assert.Error(t, err)
	_, err = os.Stat(filePathSrc2)
	assert.Error(t, err)

	_, err = os.Stat(filePathDst1)
	assert.NoError(t, err)
	_, err = os.Stat(filePathDst2)
	assert.NoError(t, err)

	err = moveFile(dirPath2, []string{fileName1}, dirPath1)
	assert.NoError(t, err)
	_, err = os.Stat(filePathDst1)
	assert.Error(t, err)
	_, err = os.Stat(filePathSrc1)
	assert.NoError(t, err)
}

func compress(file *os.File, prefix string, tw *tar.Writer) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		header, err := tar.FileInfoHeader(info, "")
		header.Name = prefix + "/" + header.Name + "/"
		if err != nil {
			return err
		}
		err = tw.WriteHeader(header)
		if err != nil {
			return err
		}
		prefix = prefix + "/" + info.Name()
		fileInfos, err := file.Readdir(-1)
		if err != nil {
			return err
		}
		for _, fi := range fileInfos {
			f, err := os.Open(file.Name() + "/" + fi.Name())
			if err != nil {
				return err
			}
			err = compress(f, prefix, tw)
			if err != nil {
				return err
			}
		}
	} else {
		header, err := tar.FileInfoHeader(info, "")
		header.Name = prefix + "/" + header.Name
		if err != nil {
			return err
		}
		err = tw.WriteHeader(header)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, file)
		file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func createTarGz(files []*os.File, dstPath string) error {
	d, _ := os.Create(dstPath)
	defer d.Close()
	gw := gzip.NewWriter(d)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()
	for _, file := range files {
		err := compress(file, "", tw)
		if err != nil {
			return err
		}
	}
	return nil
}

type mockGithub struct {
	tarFilePath string
	packageName string
}

func (m *mockGithub) getParams(c echo.Context) (errType string, err error) {
	req := c.Request()
	if err = req.ParseForm(); err != nil {
		return
	}
	if _, err = ioutil.ReadAll(req.Body); err != nil {
		return
	}
	errType = req.Form.Get("error")
	return
}

func (m *mockGithub) respFunction(c echo.Context, data map[string]interface{}) error {
	c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
	c.Response().Header().Set(RateLimitReset, data[RateLimitReset].(string))
	c.Response().Header().Set(RateLimitRemaining, data[RateLimitRemaining].(string))
	c.Response().WriteHeader(data["statusCode"].(int))
	return jsoniter.NewEncoder(c.Response()).Encode(data["data"])
}

// 请求含有错误参数，该函数通过错误参数来构造不同的错误
func (m *mockGithub) getLatestRelease() echo.HandlerFunc {
	return func(c echo.Context) error {
		data := map[string]interface{}{
			"url":  "https://api.github.com/repos/qiniu/logkit/releases/8810555",
			"name": "v1.4.1",
			"assets": []interface{}{
				map[string]interface{}{
					"name":                 "1111111.zip",
					"browser_download_url": "%s/test/github/release",
					"content_type":         "application/x-gzip",
				},
				map[string]interface{}{
					"name":                 m.packageName,
					"browser_download_url": "%s/test/github/release",
					"content_type":         "application/x-gzip",
				},
			},
			"body": "update info",
		}
		respData := map[string]interface{}{
			"data":             data,
			RateLimitRemaining: "34",
			"statusCode":       http.StatusOK,
			RateLimitReset:     strconv.FormatInt(time.Now().Add(1*time.Hour).Unix(), 10),
		}
		errType, err := m.getParams(c)
		if err != nil {
			return m.respFunction(c, respData)
		}
		switch errType {
		case "statusCode500":
			respData["data"] = "test internal server error"
			respData["statusCode"] = http.StatusInternalServerError
		case "remaining0":
			respData["data"] = "test Remaining 0"
			respData["statusCode"] = http.StatusOK
			respData[RateLimitRemaining] = "0"
		case "noName":
			delete(data, "name")
		case "jsonError":
			respData["data"] = "test json error"
		}
		return m.respFunction(c, respData)
	}
}

// 请求含有错误参数，该函数通过错误参数来构造不同的错误
func (m *mockGithub) getReleasePackage() echo.HandlerFunc {
	return func(c echo.Context) error {
		errType, err := m.getParams(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, nil)
		}
		switch errType {
		case "statusCode500":
			return c.JSON(http.StatusInternalServerError, nil)
		case "fileEmpty":
			return c.JSON(http.StatusOK, nil)
		}
		return c.File(m.tarFilePath)
	}
}

func TestRestRequest(t *testing.T) {
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	dirName := "TestRestRequest"
	rootDir := filepath.Join(pwd, dirName)
	os.RemoveAll(rootDir)
	if err = os.Mkdir(rootDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error, error is %v", rootDir, err)
	}
	defer os.RemoveAll(rootDir)

	osInfo := utilsos.GetOSInfo()
	tarDirName := "tarPath"
	tarfileName, err := getPackNameByKernelPlatform(osInfo.Kernel, osInfo.Platform, "v1.4.1")
	assert.NoError(t, err)
	tarPath := filepath.Join(rootDir, tarDirName)
	tarfilePath := filepath.Join(rootDir, tarDirName, tarfileName)

	if err = os.Mkdir(tarPath, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error, error is %v", tarPath, err)
	}

	file := "123456"
	logkit := "123456789"
	filePathDir := filepath.Join(rootDir, "_package")
	filePathDirDir := filepath.Join(filePathDir, "package1")
	if err = os.Mkdir(filePathDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error, error is %v", filePathDir, err)
	}
	if err = os.Mkdir(filePathDirDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error, error is %v", filePathDir, err)
	}
	filePath1 := filepath.Join(filePathDir, "file")
	filePath2 := filepath.Join(filePathDir, "logkit")
	filePath3 := filepath.Join(filePathDir, "logkit.exe")
	filePath4 := filepath.Join(filePathDirDir, "aaaa")
	if err = ioutil.WriteFile(filePath1, []byte(file), 0666); err != nil {
		t.Fatalf("write file fail %v", err)
	}
	if err = ioutil.WriteFile(filePath2, []byte(logkit), 0666); err != nil {
		t.Fatalf("write logkit fail %v", err)
	}
	if err = ioutil.WriteFile(filePath3, []byte(logkit), 0666); err != nil {
		t.Fatalf("write logkit.exe fail %v", err)
	}
	if err = ioutil.WriteFile(filePath4, []byte(logkit), 0666); err != nil {
		t.Fatalf("write logkit.exe fail %v", err)
	}

	filePath, err := os.Open(filePathDir)
	assert.NoError(t, err)
	err = createTarGz([]*os.File{filePath}, tarfilePath)
	assert.NoError(t, err)

	github := &mockGithub{
		tarFilePath: tarfilePath,
		packageName: tarfileName,
	}
	router := echo.New()
	router.GET("/test/github/latest", github.getLatestRelease())
	router.GET("/test/github/release", github.getReleasePackage())

	var port = 9001
	for {
		address := ":" + strconv.Itoa(port)
		ch := make(chan error, 1)
		go func() {
			defer close(ch)
			err := http.ListenAndServe(address, router)
			if err != nil {
				ch <- err
			}
		}()
		flag := 0
		start := time.Now()
		for {
			select {
			case _ = <-ch:
				flag = 1
			default:
				if time.Now().Sub(start) > time.Second {
					flag = 2
				}
			}
			if flag != 0 {
				break
			}
		}
		if flag == 2 {
			log.Infof("start to listen and serve at %v\n", address)
			break
		}
		port++
	}

	url := "http://127.0.0.1:" + strconv.Itoa(port)

	c := make(chan string)
	funcMap := map[string]func(t *testing.T, rootDir, url string){
		"testCheckLatestVersion": testCheckLatestVersion,
		"testDownloadPackage":    testDownloadPackage,
		"testDecompress":         testDecompress,
	}
	for k, f := range funcMap {
		go func(k string, f func(t *testing.T, rtDir, url string), c chan string) {
			f(t, rootDir, url)
			c <- k
		}(k, f, c)
	}
	funcCnt := len(funcMap)
	for i := 0; i < funcCnt; i++ {
		fmt.Println(<-c, "done")
	}
}

func testCheckLatestVersion(t *testing.T, _, url string) {
	osInfo := utilsos.GetOSInfo()
	packageName, _ := getPackNameByKernelPlatform(osInfo.Kernel, osInfo.Platform, "v1.4.1")
	expReleaseInfo := ReleaseInfo{
		Url:  "https://api.github.com/repos/qiniu/logkit/releases/8810555",
		Name: "v1.4.1",
		Body: "update info",
		Assets: []struct {
			DownloadUrl string `json:"browser_download_url"` // 压缩包下载链接
			PackageName string `json:"name"`                 // 压缩包名称
			ContentType string `json:"content_type"`         // 类型 zip/gzip
			FileSize    int64  `json:"size"`
		}{
			{
				DownloadUrl: "%s/test/github/release",
				PackageName: "1111111.zip",
				ContentType: "application/x-gzip",
			},
			{
				DownloadUrl: "%s/test/github/release",
				PackageName: packageName,
				ContentType: "application/x-gzip",
			},
		},
	}

	// 测试正常情况
	uri := url + "/test/github/latest"
	gotReleaseInfo, err := checkLatestVersion(uri)
	assert.NoError(t, err)
	assert.Equal(t, expReleaseInfo, gotReleaseInfo)

	// 测试服务器出错的情况
	uri = url + "/test/github/latest?error=statusCode500"
	gotReleaseInfo, err = checkLatestVersion(uri)
	assert.Error(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "test internal server error"))

	// 测试超过请求次数
	uri = url + "/test/github/latest?error=remaining0"
	gotReleaseInfo, err = checkLatestVersion(uri)
	assert.Error(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "please try again after 60 minutes"))

	// 测试返回的 version 有问题
	uri = url + "/test/github/latest?error=noName"
	gotReleaseInfo, err = checkLatestVersion(uri)
	assert.NoError(t, err)
	needUpdate, err := isUpgradeNeeded("v1.4.0", gotReleaseInfo.Name)
	assert.Equal(t, false, needUpdate)
	assert.Error(t, err)

	// 测试返回的 json 格式有问题
	uri = url + "/test/github/latest?error=jsonError"
	gotReleaseInfo, err = checkLatestVersion(uri)
	assert.Error(t, err)
}

func testDownloadPackage(t *testing.T, rtDir, url string) {
	dirName := "testDownloadPackage"
	rootDir := filepath.Join(rtDir, dirName)
	if err := os.Mkdir(rootDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error, error is %v", rootDir, err)
	}
	osInfo := utilsos.GetOSInfo()
	packageName, _ := getPackNameByKernelPlatform(osInfo.Kernel, osInfo.Platform, "v1.4.1")
	packageFilePath := filepath.Join(rootDir, packageName)

	// 测试正常情况
	uri := url + "/test/github/latest"
	releaseInfo, err := checkLatestVersion(uri)
	assert.NoError(t, err)
	downloadUrl := ""
	for _, val := range releaseInfo.Assets {
		if val.PackageName == packageName {
			downloadUrl = fmt.Sprintf(val.DownloadUrl, url)
		}
	}
	assert.NotEqual(t, "", downloadUrl)
	tmpFile, err := downloadPackage(downloadUrl, rootDir)
	assert.Nil(t, err)
	if err = os.Rename(tmpFile.Name(), packageFilePath); err != nil {
		t.Fatalf("package rename error %v", err)
	}
	_, err = os.Stat(tmpFile.Name())
	assert.Error(t, err)
	_, err = os.Stat(packageFilePath)
	assert.NoError(t, err)

	// 测试服务返回码错误
	downloadErrUrl := downloadUrl + "?error=statusCode500"
	tmpFile, err = downloadPackage(downloadErrUrl, rootDir)
	assert.Error(t, err)
	assert.Nil(t, tmpFile)

	// 测试路径不存在
	tmpFile, err = downloadPackage(downloadUrl, rootDir+"/aaa")
	assert.Error(t, err)
	assert.Nil(t, tmpFile)
}

func testDecompress(t *testing.T, rtDir, url string) {
	dirName := "testDecompress"
	rootDir := filepath.Join(rtDir, dirName)
	if err := os.Mkdir(rootDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error, error is %v", rootDir, err)
	}
	osInfo := utilsos.GetOSInfo()
	packageName, _ := getPackNameByKernelPlatform(osInfo.Kernel, osInfo.Platform, "v1.4.1")
	packageFilePath := filepath.Join(rootDir, packageName)

	uri := url + "/test/github/latest"
	releaseInfo, err := checkLatestVersion(uri)
	assert.NoError(t, err)
	downloadUrl := ""
	for _, val := range releaseInfo.Assets {
		if val.PackageName == packageName {
			downloadUrl = fmt.Sprintf(val.DownloadUrl, url)
		}
	}
	assert.NotEqual(t, "", downloadUrl)
	tmpFile, err := downloadPackage(downloadUrl, rootDir)
	assert.Nil(t, err)
	if err = os.Rename(tmpFile.Name(), packageFilePath); err != nil {
		t.Fatalf("package rename error %v", err)
	}
	_, err = os.Stat(tmpFile.Name())
	assert.Error(t, err)
	_, err = os.Stat(packageFilePath)
	assert.NoError(t, err)

	unpackDir := filepath.Join(rootDir, strconv.FormatInt(time.Now().Unix(), 10))
	if err := os.Mkdir(unpackDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error, error is %v", unpackDir, err)
	}
	unpackPath, err := decompress(packageFilePath, unpackDir)
	assert.NoError(t, err)
	filePath1 := filepath.Join(unpackPath, "file")
	filePath2 := filepath.Join(unpackPath, "logkit")
	filePath3 := filepath.Join(unpackPath, "logkit.exe")
	_, err = os.Stat(filePath1)
	assert.NoError(t, err)
	_, err = os.Stat(filePath2)
	assert.NoError(t, err)
	_, err = os.Stat(filePath3)
	assert.NoError(t, err)
}
