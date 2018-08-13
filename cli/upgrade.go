package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

const (
	Logkit      = "logkit"
	GoOSWindows = "windows"
	GoOSLinux   = "Linux"
	GoOSMac     = "Darwin"
	Arch386     = "386"
	Arch64      = "amd64"

	UserAgent          = "User-Agent"
	RateLimitReset     = "X-RateLimit-Reset"
	RateLimitRemaining = "X-RateLimit-Remaining"
)

// 为了测试，将这个声明为变量，但是不要修改
var LatestVersionUrl = "https://api.github.com/repos/qiniu/logkit/releases/latest"

type ReleaseInfo struct {
	Url    string     `json:"url"`  // api 请求地址
	Name   string     `json:"name"` // version
	Body   string     `json:"body"` // 更新内容
	Assets []struct { // release 的具体信息，目前有 5 个(linux 2 个, windows 2 个, mac 1 个)
		DownloadUrl string `json:"browser_download_url"` // 压缩包下载链接
		PackageName string `json:"name"`                 // 压缩包名称
		ContentType string `json:"content_type"`         // 类型 zip/gzip
		FileSize    int64  `json:"size"`                 // 文件大小
	} `json:"assets"`
}

type CliVersion struct {
	Major    int
	Minor    int
	Revision int
}

func (v CliVersion) toString() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Revision)
}

func (v CliVersion) lessThan(v2 CliVersion) bool {
	if v.Major < v2.Major {
		return true
	} else if v.Major > v2.Major {
		return false
	}
	if v.Minor < v2.Minor {
		return true
	} else if v.Minor > v2.Minor {
		return false
	}
	if v.Revision < v2.Revision {
		return true
	} else if v.Revision > v2.Revision {
		return false
	}
	return false
}

// 解析版本号
func parseCliVersion(version string) (ret CliVersion, err error) {
	if strings.HasPrefix(version, "v") {
		version = version[1:]
	}
	digits := make([]int, 0)
	version = strings.TrimSpace(version)
	if len(version) == 0 {
		err = fmt.Errorf("can not parse version <%v>", version)
		return
	}
	fields := strings.Split(version, ".")
	for _, field := range fields {
		i := 0
		if i, err = strconv.Atoi(field); err != nil {
			return
		}
		digits = append(digits, i)
	}
	for i := 0; i < len(digits); i++ {
		switch i {
		case 0:
			ret.Major = digits[i]
		case 1:
			ret.Minor = digits[i]
		case 2:
			ret.Revision = digits[i]
		}
	}
	return
}

// 通过 github 的 api 获取最新版本号
func checkLatestVersion(url string) (ReleaseInfo, error) {
	releaseInfo := ReleaseInfo{}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return releaseInfo, err
	}
	req.Header.Set(UserAgent, Logkit)
	req.Header.Set(ContentTypeHeader, ApplicationJson)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return releaseInfo, err
	}
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return releaseInfo, err
		}
		return releaseInfo, errors.New(string(body))
	}
	remainCnt, err := strconv.ParseInt(resp.Header.Get(RateLimitRemaining), 10, 32)
	if err != nil {
		return releaseInfo, err
	}
	if remainCnt <= 0 {
		resetTimeStr := resp.Header.Get(RateLimitReset)
		resetTimeInt, err := strconv.ParseInt(resetTimeStr, 10, 64)
		if err != nil {
			return releaseInfo, fmt.Errorf("parser reset time %v to int64 error, %v", resetTimeStr, err)
		}
		now := time.Now()
		resetTime := time.Unix(resetTimeInt, 0)
		holdTime := math.Ceil(resetTime.Sub(now).Minutes())
		return releaseInfo, fmt.Errorf("please try again after %v minutes", holdTime)
	}
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return releaseInfo, err
	}
	if err := json.Unmarshal(content, &releaseInfo); err != nil {
		return releaseInfo, fmt.Errorf(err.Error()+", response body is %v", string(content))
	}
	return releaseInfo, nil
}

func getBinaryName() string {
	if runtime.GOOS == "windows" {
		return "logkit.exe"
	}
	return "logkit"
}

func getNeedBackupFiles() []string {
	return []string{getBinaryName()}
}

// 检查是否需要更新
func isUpgradeNeeded(curVersionStr, latestVersionStr string) (needUpgrade bool, err error) {
	var curVersion, latestVersion CliVersion
	if curVersion, err = parseCliVersion(curVersionStr); err != nil {
		return
	}

	if latestVersion, err = parseCliVersion(latestVersionStr); err != nil {
		return
	}

	if curVersion.lessThan(latestVersion) {
		needUpgrade = true
	}
	return
}

// 根据 kernel 和 platform 获取安装包的名称
func getPackNameByKernelPlatform(kernel, platform, version string) (fileName string, err error) {
	if kernel == GoOSLinux {
		if platform == Arch386 {
			fileName = "logkit_linux32_%s.tar.gz"
		} else if platform == Arch64 {
			fileName = "logkit_%s.tar.gz"
		} else {
			err = fmt.Errorf("unknown platform %v", platform)
		}
	} else if kernel == GoOSMac {
		fileName = "logkit_mac_%s.tar.gz"
	} else if kernel == GoOSWindows {
		if platform == Arch386 {
			fileName = "logkit_windows32_%s.zip"
		} else if platform == Arch64 {
			fileName = "logkit_windows_%s.zip"
		} else {
			err = fmt.Errorf("unknown platform %v", platform)
		}
	} else {
		err = fmt.Errorf("unknown kernel %v", kernel)
	}
	fileName = fmt.Sprintf(fileName, version)
	return
}

// 将下载的安装包放在 file 中
func downloadPackage(url, dstPath string) (file *os.File, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Set(UserAgent, Logkit)
	req.Header.Set(ContentTypeHeader, ApplicationJson)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyByte, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("response code is %v, error is %v", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("response code is %v, response body is %v", resp.StatusCode, string(bodyByte))
	}
	file, err = ioutil.TempFile(dstPath, "package_downloading_")
	if err != nil {
		return
	}
	defer file.Close()
	finishChan := make(chan bool, 1)
	if resp.ContentLength != 0 {
		go func() {
			for {
				select {
				case <-finishChan:
					break
				default:
					downloaded := int64(0)
					info, err := os.Stat(file.Name())
					if err == nil {
						downloaded = info.Size()
					}
					fmt.Printf("\rDownloading: %2d%%", downloaded*100/resp.ContentLength)
					time.Sleep(1 * time.Second)
				}
			}
		}()
	}
	if _, err = io.Copy(file, resp.Body); err != nil {
		return
	}
	finishChan <- true
	fmt.Println("\rDownloading: 100%")
	if err = os.Chmod(file.Name(), 0744); err != nil {
		return
	}
	return file, err
}

// 将 srcDir 中的 fileName 文件/文件夹 移动到 dstDir 文件夹, dstDir 不存在时自动创建
func moveFile(srcDir string, fileName []string, dstDir string) error {
	if _, err := os.Stat(dstDir); err != nil {
		if err := os.Mkdir(dstDir, DefaultDirPerm); err != nil {
			return err
		}
	}
	for _, file := range fileName {
		srcPath := filepath.Join(srcDir, file)
		dstPath := filepath.Join(dstDir, file)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// 备份需要备份的文件, 移动到 dstDir
func backupCurFile(srcDir string, backupFiles []string, dstDir string) error {
	return moveFile(srcDir, backupFiles, dstDir)
}

// 备份文件还原
func restoreFile(backDir string, backupFiles []string, curDir string) error {
	return moveFile(backDir, backupFiles, curDir)
}

// 压缩包路径、解压目的路径
// 没有做多嵌套文件适配，目前仅支持当前压缩包的文件目录格式
// 即: 解压后有一个文件夹(_package_linux32等), 文件夹里面即为 logkit binary
// 返回的 string 为文件夹名称(如 _package_linux32, _package_linux64 等)
func decompress(packFilePath, dstDir string) (string, error) {
	return DecompressTarGzip(packFilePath, dstDir, getBinaryName())
}

func CheckAndUpgrade(curVersion string) {
	fmt.Println("Current version is " + curVersion)
	fmt.Println("Checking the latest version...")

	// 检查最新版本号
	releaseInfo, err := checkLatestVersion(LatestVersionUrl)
	if err != nil {
		fmt.Printf("Automatic upgrade failed, check latest version error, %v\n", err)
		return
	}
	latestVersion := releaseInfo.Name
	fmt.Println("The latest version is " + latestVersion)

	//检查是否需要升级
	needUpgrade, err := isUpgradeNeeded(curVersion, latestVersion)
	if err != nil {
		fmt.Printf("Automatic upgrade failed, check whether the program needs to upgrade error, %v\n", err)
		return
	}
	if !needUpgrade {
		fmt.Println("Need not to upgrade, automatic upgrade exit")
		return
	}

	osInfo := utilsos.GetOSInfo()
	kernel := osInfo.Kernel
	platform := osInfo.Platform

	// 根据 kernel 和 platform 来获取安装包的名称
	packFileName, err := getPackNameByKernelPlatform(kernel, platform, latestVersion)
	if err != nil {
		fmt.Printf("Automatic upgrade failed, get package name error, %v\n", err)
		return
	}

	// 遍历返回数据中的 assets 获取相应安装包的下载地址
	downloadUrl := ""
	fileSize := int64(0)
	for _, asset := range releaseInfo.Assets {
		if asset.PackageName == packFileName {
			fileSize = asset.FileSize
			downloadUrl = asset.DownloadUrl
		}
	}

	if downloadUrl == "" {
		fmt.Printf("Automatic upgrade failed, package %v is not found\n", packFileName)
		return
	}

	var input string
	fmt.Printf("Warning: Automatic upgrade will only update logkit binary file without web front end, press y to continue: ")
	fmt.Scanf("%s", &input)
	if input != "y" && input != "Y" {
		fmt.Println("Automatic upgrade exit")
		return
	}

	// 获取程序根目录
	var rootDir string
	if rootDir, err = os.Getwd(); err != nil {
		fmt.Printf("Automatic upgrade failed, get program root path error, %v\n", err)
		return
	}

	// 获取安装包
	var tmpFile *os.File
	fmt.Printf("Begin to download %v package\n", latestVersion)
	if tmpFile, err = downloadPackage(downloadUrl, rootDir); err != nil {
		fmt.Printf("Automatic upgrade failed, download package error, %v\n", err)
		return
	}
	packFilePath := filepath.Join(rootDir, packFileName)
	if err := os.Rename(tmpFile.Name(), packFilePath); err != nil {
		fmt.Printf("Automatic upgrade failed, move new package file to %v error, %v\n", packFilePath, err)
	}
	defer os.RemoveAll(packFilePath)

	// 校验刚刚下载的文件是否存在, 大小是否与 releaseInfo 中相等
	packFile, err := os.Stat(packFilePath)
	if err != nil {
		fmt.Printf("Automatic upgrade failed, %v\n", err)
		return
	}
	if packFile.Size() != fileSize {
		fmt.Printf("File verification failed, expect package size is %v, but got %v, please try again.", fileSize, packFile.Size())
		return
	}

	// 备份现有的文件
	fmt.Println("Backup old file")
	backupDir := filepath.Join(rootDir, "logkit_bak_"+strconv.FormatInt(time.Now().Unix(), 10))
	if err = backupCurFile(rootDir, getNeedBackupFiles(), backupDir); err != nil {
		fmt.Println("Automatic upgrade failed, backup old files error,", err)
		return
	}
	defer os.RemoveAll(backupDir)

	// 解压安装包
	fmt.Println("Decompress new package")
	var packDir string
	if packDir, err = decompress(packFilePath, rootDir); err != nil {
		fmt.Println("Automatic upgrade failed, decompress new package error,", err)
		restoreFile(backupDir, getNeedBackupFiles(), rootDir)
		return
	}
	// 如果这个压缩包里面没有任何文件夹
	if packDir == "" {
		packDir = rootDir
	}
	defer os.RemoveAll(packDir)

	// 将文件、文件夹从安装包中移动出来
	fmt.Println("Replace old file")
	if err = moveFile(packDir, getNeedBackupFiles(), rootDir); err != nil {
		fmt.Println("Automatic upgrade failed, copy new file to rootDir error,", err)
		restoreFile(backupDir, getNeedBackupFiles(), rootDir)
		return
	}
	fmt.Println("Upgrade successfully, enjoy it!")
	bodyItem := strings.Split(releaseInfo.Body, "\r\n")
	for _, item := range bodyItem {
		fmt.Println(item)
	}
}
