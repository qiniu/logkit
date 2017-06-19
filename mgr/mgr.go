package mgr

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/logkit/cleaner"
	config "github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"

	"github.com/howeyc/fsnotify"
	"github.com/qiniu/log"
)

var DIR_NOT_EXIST_SLEEP_TIME = 300 //300 s

type ManagerConfig struct {
	BindHost string `json:"bind_host"`
	Idc      string `json:"idc"`
	Zone     string `json:"zone"`
}

type cleanQueue struct {
	cleanerCount int
	filecount    map[string]int
}

type Manager struct {
	ManagerConfig
	lock        sync.RWMutex
	cleanlock   sync.Mutex
	cleanChan   chan cleaner.CleanSignal
	cleanQueues map[string]*cleanQueue
	runners     map[string]Runner
	watchers    map[string]*fsnotify.Watcher // inode到watcher的映射表
	pregistry   *parser.ParserRegistry
	sregistry   *sender.SenderRegistry
}

func NewManager(conf ManagerConfig) (*Manager, error) {
	ps := parser.NewParserRegistry()
	sr := sender.NewSenderRegistry()
	return NewCustomManager(conf, ps, sr)
}

func NewCustomManager(conf ManagerConfig, pr *parser.ParserRegistry, sr *sender.SenderRegistry) (*Manager, error) {
	m := &Manager{
		ManagerConfig: conf,
		cleanChan:     make(chan cleaner.CleanSignal),
		cleanQueues:   make(map[string]*cleanQueue),
		runners:       make(map[string]Runner),
		watchers:      make(map[string]*fsnotify.Watcher),
		pregistry:     pr,
		sregistry:     sr,
	}
	return m, nil
}

func (m *Manager) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, runner := range m.runners {
		runner.Stop()
	}
	for _, w := range m.watchers {
		if w != nil {
			w.Close()
		}
	}
	close(m.cleanChan)
	return nil
}

func (m *Manager) Remove(confPath string) {
	if !strings.HasSuffix(confPath, ".conf") {
		log.Warn(confPath, "not end with .conf, skipped")
		return
	}
	log.Info("try remove", confPath)
	confPathAbs, err := filepath.Abs(confPath)
	if err != nil {
		log.Warnf("filepath.Abs(%s) failed: %v", confPath)
		return
	}
	confPath = confPathAbs
	m.lock.Lock()
	defer m.lock.Unlock()

	runner, ok := m.runners[confPath]
	if !ok {
		log.Warnf("%s not added, nothing to do", confPath)
		return
	}

	m.removeCleanQueue(runner.Cleaner())
	runner.Stop()
	delete(m.runners, confPath)
	log.Infof("runner %s be removed, total %d", runner.Name(), len(m.runners))
	return
}

func (m *Manager) addCleanQueue(info CleanInfo) {
	if !info.enable {
		return
	}
	m.cleanlock.Lock()
	defer m.cleanlock.Unlock()
	cq, ok := m.cleanQueues[info.logdir]
	if ok {
		cq.cleanerCount++
	} else {
		cq = &cleanQueue{
			cleanerCount: 1,
			filecount:    make(map[string]int),
		}
	}
	log.Info(">>>>>>>>>>>> add clean queue", cq.cleanerCount, info.logdir)
	m.cleanQueues[info.logdir] = cq
	return
}

func (m *Manager) removeCleanQueue(info CleanInfo) {
	if !info.enable {
		return
	}
	m.cleanlock.Lock()
	defer m.cleanlock.Unlock()
	cq, ok := m.cleanQueues[info.logdir]
	if !ok {
		log.Errorf("can't find clean queue %v to remove", info.logdir)
		return
	}
	cq.cleanerCount--
	if cq.cleanerCount <= 0 {
		delete(m.cleanQueues, info.logdir)
	}
	log.Info(">>>>>>>>>>>> remove clean queue", cq.cleanerCount, info.logdir)
	return
}

func (m *Manager) Add(confPath string) {
	if !strings.HasSuffix(confPath, ".conf") {
		log.Warn(confPath, "not end with .conf, skipped")
		return
	}
	log.Info("try add", confPath)
	confPathAbs, _, err := utils.GetRealPath(confPath)
	if err != nil {
		log.Warnf("filepath.Abs(%s) failed: %v", confPath)
		return
	}
	confPath = confPathAbs
	if m.isRunning(confPath) {
		log.Errorf("%s already added", confPath)
		return
	}
	var conf RunnerConfig
	err = config.LoadEx(&conf, confPath)
	if err != nil {
		log.Warnf("config.LoadEx %s failed:%v", confPath, err)
		return
	}

	forkRunner := func(confPath string) {
		var runner Runner
		i := 0
		for {
			if m.isRunning(confPath) {
				log.Errorf("%s already added - ", confPath)
				return
			}

			if runner, err = NewCustomRunner(conf, m.cleanChan, m.pregistry, m.sregistry); err != nil {
				errVal, ok := err.(*os.PathError)
				if !ok {
					log.Errorf("NewRunner(%v) failed: %v", conf.RunnerName, err)
					return
				}
				i++
				log.Warnf("LogDir(%v) does not exsit after %d rounds, sleep 5 minute and try again...", errVal.Path, i)
				time.Sleep(time.Duration(DIR_NOT_EXIST_SLEEP_TIME) * time.Second)
				continue
			}
			break
		}
		m.lock.Lock()
		defer m.lock.Unlock()
		// double check
		if _, ok := m.runners[confPath]; ok {
			return
		}
		m.addCleanQueue(runner.Cleaner())
		log.Infof("Runner[%v] added: %#v", conf.RunnerName, confPath)
		go runner.Run()
		m.runners[confPath] = runner
		log.Infof("new Runner[%v] is added, total %d", conf.RunnerName, len(m.runners))
		return
	}

	log.Infof("Start to try add: %v", conf.RunnerName)
	go forkRunner(confPath)

	return
}
func (m *Manager) isRunning(confPath string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if _, ok := m.runners[confPath]; ok {
		return true
	}
	return false
}

// 创建文件会触发 create和modify事件
// 重命名会触发 rename和create事件
// 删除会触发 delete事件
// 修改会触发 delete事件、create事件、modify事件以及modify|ATTRIB事件
func (m *Manager) handle(path string, watcher *fsnotify.Watcher) {
	for {
		select {
		case ev, ok := <-watcher.Event:
			if !ok {
				log.Info("Manager watcher chan was closed")
				return
			}
			log.Info("event:", ev)
			if ev.IsDelete() || ev.IsRename() {
				_, err := os.Stat(path)
				if os.IsNotExist(err) {
					// 如果当前监听文件被删除，则不再监听，退出
					log.Warnf("close file watcher path %v", path)
					watcher.Close()
					delete(m.watchers, path)
					return
				}
				m.Remove(ev.Name)
			}
			if ev.IsCreate() {
				m.Add(ev.Name)
			}
			if ev.IsModify() && !ev.IsCreate() {
				m.Remove(ev.Name)
				m.Add(ev.Name)
			}
		case err := <-watcher.Error:
			if err != nil {
				log.Error("error:", err)
			}
		}
	}
}

func (m *Manager) doClean(sig cleaner.CleanSignal) {
	m.cleanlock.Lock()
	defer m.cleanlock.Unlock()

	dir := sig.Logdir
	dir, err := filepath.Abs(dir)
	if err != nil {
		log.Errorf("get abs for %v error %v", dir, err)
		return
	}
	file := sig.Filename
	q, ok := m.cleanQueues[dir]
	if !ok {
		log.Errorf("%v cleaner dir %v not exist but got clean signal for delete file %v", sig.Cleaner, dir, file)
		return
	}
	count := q.filecount[file] + 1
	if count >= q.cleanerCount {
		catdir := filepath.Join(dir, file)
		err := os.Remove(catdir)
		if err != nil {
			if os.IsNotExist(err) {
				log.Warnf("clean %v failed as logfile is not exist: %v", catdir, err)
			} else {
				log.Errorf("clean %v failed: %v", catdir, err)
			}
		} else {
			log.Infof("log <%v> was successfully cleaned by cleaner", catdir)
		}
		delete(q.filecount, file)
	} else {
		q.filecount[file] = count
		m.cleanQueues[dir] = q
	}
	return
}

func (m *Manager) clean() {
	for sig := range m.cleanChan {
		m.doClean(sig)
	}
}

func (m *Manager) detectMoreWatchers(confsPath []string) {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			log.Debug("try to detect more watchers ")
			m.addWatchers(confsPath)
		}
	}
}

func (m *Manager) addWatchers(confsPath []string) (err error) {
	for _, dir := range confsPath {
		paths, err := filepath.Glob(dir)
		if err != nil {
			log.Errorf("filepath.Glob(%s): %v, err:%v", dir, paths, err)
			continue
		}
		for _, path := range paths {
			_, exist := m.watchers[path]
			if exist {
				// 如果文件已经被监听，则不再重复监听
				continue
			}
			files, err := ioutil.ReadDir(path)
			if err != nil {
				log.Errorf("ioutil.ReadDir(%s): %v, err:%v", path, files, err)
				continue
			}
			log.Warnf("start to add watcher of conf path %v", path)
			for _, f := range files {
				if f.IsDir() {
					log.Warn("skipped dir", f.Name)
					continue
				}
				m.Add(filepath.Join(path, f.Name()))
			}
			watcher, err := fsnotify.NewWatcher()
			if err != nil {
				log.Errorf("fsnotify.NewWatcher: %v", err)
				continue
			}
			m.watchers[path] = watcher
			go m.handle(path, watcher)
			if err = watcher.Watch(path); err != nil {
				log.Errorf("watch %v error %v", path, err)
			}
		}
	}
	return nil
}

func (m *Manager) Watch(confsPath []string) (err error) {
	err = m.addWatchers(confsPath)
	if err != nil {
		log.Errorf("addWatchers error : %v", err)
	}
	go m.detectMoreWatchers(confsPath)
	go m.clean()
	return
}

func (m *Manager) Status() (rss map[string]RunnerStatus) {
	rss = make(map[string]RunnerStatus)
	for _, r := range m.runners {
		rss[r.Name()] = r.Status()
	}
	return
}
