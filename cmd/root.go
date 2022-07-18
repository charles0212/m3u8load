package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/golang/groupcache/lru"
	"github.com/grafov/m3u8"
	"github.com/spf13/cobra"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "m3u8load",
	Short: "download m3u8 ts video",
	Long:  `download m3u8 ts video from url`,
	Run:   downloadFunc,
}

type Download struct {
	URI string
}

type DownloadProcess struct {
	// 下载路径
	Path string
	// 下载的ts文件状态
	MediaStatus map[string]bool
	// 下载的ts文件列表
	MediaList []string
	// ts文件内部状态
	status *sync.Map
	// 同步锁
	sync.Mutex
}

var (
	parallel int
	m3u8Url  string
	outPath  string
)

var bar *pb.ProgressBar
var downloadProcess = &DownloadProcess{}
var UserAgent string
var client = &http.Client{}

func Execute() {
	// root命名执行
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// 并行线程数，默认10个
	rootCmd.Flags().IntVarP(&parallel, "num", "n", 10, "concurrent channel num")
	// 下载m3u8链接
	rootCmd.Flags().StringVarP(&m3u8Url, "url", "u", "", "m3u8 url to download video")
	// 输出目录
	rootCmd.Flags().StringVarP(&outPath, "out", "o", "", "the download output file path")
}

func downloadFunc(cmd *cobra.Command, args []string) {
	if m3u8Url == "" || outPath == "" {
		fmt.Println("args miss, for example: ")
		fmt.Println("m3u8load -u https://v2.szjal.cn/20191215/B6UVqUJm/index.m3u8 -o charles")
		cmd.Help()
		os.Exit(1)
	}
	if !strings.HasPrefix(m3u8Url, "http") || !strings.HasSuffix(m3u8Url, "m3u8") {
		fmt.Println("m3u8 url illegal, for example: https://v2.szjal.cn/20191215/B6UVqUJm/index.m3u8 ")
		cmd.Help()
		os.Exit(1)
	}
	fmt.Println("")
	fmt.Println("concurrent num : " + strconv.Itoa(parallel))
	fmt.Println("m3u8 url: " + m3u8Url)
	fmt.Println("output file path: " + outPath)
	fmt.Println("")

	// defer 在资源释放、连接关闭、函数结束时调用
	// 多个defer为堆栈结构，先进后出，也就是先进的后执行
	defer catchException()

	// 退出的钩子
	go listenSignal()

	name := outPath + string(os.PathSeparator) + ".index"
	if _, err := os.Stat(name); os.IsNotExist(err) {
		// 1、下载新文件
		msChan := make(chan *Download, 1024)
		go getPlaylist(m3u8Url, msChan)

		// 并发下载ts文件, 文件输出目录，默认当前文件
		downloadSegmentLimit(outPath, msChan)
	} else {
		// 2、已存在已有文件
		load(name, downloadProcess)
		if len(downloadProcess.MediaList) > 0 {
			msChan := make(chan *Download, 1024)

			// 异步继续下载未完成的ts
			go getContinuePlaylist(msChan)

			// 并发下载ts文件, 文件输出目录，默认当前文件
			downloadSegmentLimit(outPath, msChan)
		} else {
			// 文件不完整，重新下载
			msChan := make(chan *Download, 1024)
			go getPlaylist(m3u8Url, msChan)

			// 并发下载ts文件, 文件输出目录，默认当前文件
			downloadSegmentLimit(outPath, msChan)
		}
	}

	bar.Finish()
	fmt.Println("")
	// 写入进度和合并ts文件
	writeAndMergeFile(outPath)
	// 应用正常退出
	os.Exit(0)
}

// 异常捕获处理
func catchException() {
	//fmt.Println("catch_exception")
	// 获取异常
	err := recover()
	if err != nil {
		fmt.Println("error msg: " + fmt.Sprintf("%s", err))
	}

	// 任务进度
	writeJsonFile()
}

func load(filename string, v interface{}) {
	//ReadFile函数会读取文件的全部内容，并将结果以[]byte类型返回
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	//读取的数据为json格式，需要进行解码
	err = json.Unmarshal(data, v)
	if err != nil {
		return
	}
}

func doRequest(c *http.Client, req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", UserAgent)
	resp, err := c.Do(req)
	return resp, err
}

// 并发限制
func downloadSegmentLimit(outPath string, dlc chan *Download) {
	defer catchException()

	// 目录不存在创建目录
	_, err := os.Stat(outPath)
	if os.IsNotExist(err) {
		err := os.Mkdir(outPath, os.ModePerm)
		if err != nil {
			log.Panic(err)
		}
	}

	// 5个并发
	chLimit := make(chan bool, parallel)
	wg := sync.WaitGroup{}

	for v := range dlc {
		chLimit <- true
		wg.Add(1)
		// 并发下载
		go downloadSegment(chLimit, &wg, outPath, v)
	}

	close(chLimit)
	wg.Wait()
}

func downloadSegment(chLimit chan bool, wg *sync.WaitGroup, outPath string, v *Download) {
	defer catchException()

	index := strings.LastIndex(v.URI, "/")
	if index != -1 {
		// 已经成功下载直接跳过
		_, ok := downloadProcess.status.Load(v.URI)
		if ok == true {
			return
		}

		req, err := http.NewRequest("GET", string(v.URI), nil)
		if err != nil {
			log.Panic(err)
		}
		resp, err := doRequest(client, req)
		if err != nil {
			log.Print(err)
			setMediaStatus(v.URI, false)
			return
		}
		if resp.StatusCode != 200 {
			setMediaStatus(v.URI, false)
			log.Printf("Received HTTP %v for %v\n", resp.StatusCode, v.URI)
			return
		}

		// 根据路径 + 文件.ts 拼接路径 （直接创建文件）
		name := getFileName(v.URI)
		out, _ := os.Create(outPath + "/" + name)
		// ts文件写入到对应文件中
		_, err = io.Copy(out, resp.Body)
		if err != nil {
			log.Panic(err)
		}
		resp.Body.Close()

		// 当前链接下载成功
		setMediaStatus(v.URI, true)
		// 进度+1
		bar.Increment()
	}

	wg.Done()
	// 从channel读取数据
	<-chLimit
}

func getFileName(uri string) string {
	index := strings.LastIndex(uri, "/")
	// 根据路径 + 文件.ts 拼接路径 （直接创建文件）
	name := uri[index+1:]
	return name
}

func getFilePath(uri string, playlistUrl *url.URL) string {
	// 获取uri的绝对路径
	uri = getAbsoluteUri(uri, playlistUrl)
	index := strings.LastIndex(uri, "/")
	// 根据路径 + 文件.ts 拼接路径 （直接创建文件）
	path := uri[:index+1]
	return path
}

func getContinuePlaylist(dlc chan *Download) {
	// defer 在资源释放、连接关闭、函数结束时调用
	// 多个defer为堆栈结构，先进后出，也就是先进的后执行
	defer catchException()

	// 进度条
	bar = pb.StartNew(len(downloadProcess.MediaList))
	downloadProcess.status = &sync.Map{}
	for key, value := range downloadProcess.MediaStatus {
		if value == false {
			downloadProcess.status.Store(key, false)
			dlc <- &Download{downloadProcess.Path + key}
		} else {
			downloadProcess.status.Store(key, true)
			// 已完成的文件数
			bar.Increment()
		}
	}
	// 关闭通道
	close(dlc)
}

func getPlaylist(urlStr string, dlc chan *Download) {
	// defer 在资源释放、连接关闭、函数结束时调用
	// 多个defer为堆栈结构，先进后出，也就是先进的后执行
	defer catchException()

	cache := lru.New(1024)
	playlistUrl, err := url.Parse(urlStr)
	if err != nil {
		log.Panic(err)
	}

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		log.Panic(err)
	}
	resp, err := doRequest(client, req)
	if err != nil {
		log.Print(err)
		time.Sleep(time.Duration(3) * time.Second)
	}
	playlist, listType, err := m3u8.DecodeFrom(resp.Body, true)
	if err != nil {
		log.Panic(err)
	}
	resp.Body.Close()

	// media 类型
	if listType == m3u8.MEDIA {
		mpl := playlist.(*m3u8.MediaPlaylist)

		// 初始化map
		downloadProcess.status = &sync.Map{}
		for _, vv := range mpl.Segments {
			if vv != nil {
				name := getFileName(vv.URI)
				if downloadProcess.Path == "" {
					downloadProcess.Path = getFilePath(vv.URI, playlistUrl)
				}

				downloadProcess.status.Store(name, false)
				downloadProcess.MediaList = append(downloadProcess.MediaList, name)
			}
		}

		// 进度条
		bar = pb.StartNew(len(downloadProcess.MediaList))

		for _, v := range mpl.Segments {
			// ts文件列表
			if v != nil {
				// 获取绝对路径uri
				var msURI = getAbsoluteUri(v.URI, playlistUrl)
				_, hit := cache.Get(msURI)
				if !hit {
					cache.Add(msURI, nil)
					dlc <- &Download{msURI}
				}
			}
		}
		if mpl.Closed {
			// 需要需要确认什么情况下回关闭（这个地方有问题）
			close(dlc)
			return
		} else {
			time.Sleep(time.Duration(int64(mpl.TargetDuration * 1000000000)))
		}
	} else if listType == m3u8.MASTER {
		// 数据类型转换 m3u8.Playlist 转成  *m3u8.MasterPlaylist
		mpl := playlist.(*m3u8.MasterPlaylist)
		// 获取最大带宽，对应的链接index.m3u8
		var masterURI string
		var maxBandwidth uint32 = 0
		for _, v := range mpl.Variants {
			if v.Bandwidth > maxBandwidth {
				maxBandwidth = v.Bandwidth
				masterURI = v.URI
			}
		}

		// 获取绝对路径
		var msURI = getAbsoluteUri(masterURI, playlistUrl)
		fmt.Println("master m3u8 url " + msURI)
		// 调用获取media playlist
		getPlaylist(msURI, dlc)
	} else {
		log.Panic("Not a valid media playlist")
	}
}

// 协程设置sync.map
func setMediaStatus(uri string, value bool) {
	downloadProcess.status.Store(getFileName(uri), value)
}

func getAbsoluteUri(masterURI string, playlistUrl *url.URL) string {
	var msURI string
	var err error

	// 相对路径，转换成绝对路径
	if !strings.HasPrefix(masterURI, "http") {
		var masterURL *url.URL
		masterURL, err = playlistUrl.Parse(masterURI)
		masterURI = masterURL.String()
		if err != nil {
			log.Print(err)
			return msURI
		}
	}

	msURI, err = url.QueryUnescape(masterURI)
	if err != nil {
		log.Panic(err)
	}

	return msURI
}

func listenSignal() {
	signs := make(chan os.Signal, 1)
	signal.Notify(signs,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGUSR1,
		syscall.SIGUSR2,
		syscall.SIGTSTP)
	select {
	case <-signs:
		fmt.Println("exit program , signs: ", signs)
		writeJsonFile()
		os.Exit(0)
	}
}

func writeAndMergeFile(outPath string) {
	// 写文件进度到文件中
	writeJsonFile()
	// 合并所有ts文件
	mergeMediaFile(outPath)
}

func writeJsonFile() {
	// 写入ts文件进度加锁
	downloadProcess.Lock()
	// status写入到MediaStatus中
	// 遍历所有sync.Map中的键值对，加锁判断是否初始化
	if downloadProcess.MediaStatus == nil {
		downloadProcess.MediaStatus = make(map[string]bool)
	}

	downloadProcess.status.Range(func(k, v interface{}) bool {
		downloadProcess.MediaStatus[k.(string)] = v.(bool)
		return true
	})

	// 最后面4个空格，json格式缩进
	result, _ := json.MarshalIndent(downloadProcess, "", "  ")
	name := outPath + string(os.PathSeparator) + ".index"
	_ = ioutil.WriteFile(name, result, 0644)

	// 写入ts文件进度释放锁
	downloadProcess.Unlock()
}

func mergeMediaFile(outPath string) {
	fileName := outPath + ".ts"

	// 文件存在需要删除
	if _, err := os.Stat(fileName); err == nil {
		if err := os.Remove(fileName); err != nil {
			fmt.Println("remove file " + fileName + " failed. ")
		}
	}

	tsMergeFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic(err)
		return
	}
	for _, value := range downloadProcess.MediaList {
		tsFile, err := os.OpenFile(outPath+string(os.PathSeparator)+value, os.O_RDONLY, os.ModePerm)
		if err != nil {
			fmt.Println(err)
			return
		}
		b, err := ioutil.ReadAll(tsFile)
		if err != nil {
			fmt.Println(err)
			return
		}
		tsMergeFile.Write(b)
		tsFile.Close()
	}

}
