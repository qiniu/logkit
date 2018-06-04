package ip

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
)

const Null = "N/A"

var (
	ErrInvalidIP = errors.New("invalid IP format")
	std          *Locator
)

type ErrInvalidFile struct {
	Reason string
}

func (e ErrInvalidFile) Error() string {
	return fmt.Sprintf("invalid file format: %s", e.Reason)
}

// Init defaut locator with dataFile
func Init(dataFile string) error {
	if std != nil {
		return nil
	}

	var err error
	std, err = NewLocator(dataFile)
	if err != nil {
		return err
	}
	return nil
}

func Update(dataFile string) (err error) {
	std, err = NewLocator(dataFile)
	return
}

// Init defaut locator with data
func InitWithData(data []byte) error {
	if std != nil {
		return nil
	}

	var err error
	std, err = NewLocatorWithData(data)
	if err != nil {
		return err
	}
	return nil
}

// Find locationInfo by ip string
// It will return err when ipstr is not a valid format
func Find(ipstr string) (*LocationInfo, error) {
	return std.Find(ipstr)
}

// Find locationInfo by uint32
func FindByUint(ip uint32) *LocationInfo {
	return std.FindByUint(ip)
}

//-----------------------------------------------------------------------------

// New locator with dataFile
func NewLocator(dataFile string) (*Locator, error) {
	data, err := ioutil.ReadFile(dataFile)
	if err != nil {
		return nil, err
	}

	loc, err := NewLocatorWithData(data)
	if err != nil {
		return nil, err
	}

	return loc, nil
}

// New locator with data
func NewLocatorWithData(data []byte) (*Locator, error) {
	loc := new(Locator)
	if err := loc.init(data); err != nil {
		return nil, err
	}
	return loc, nil
}

type Locator struct {
	textData   []byte
	indexData1 []uint32
	indexData2 []int
	indexData3 []int
	index      []int
}

type LocationInfo struct {
	Country      string
	Region       string
	City         string
	Isp          string
	CountryCode  string
	Latitude     string
	Longitude    string
	DistrictCode string
}

// Find locationInfo by ip string
// It will return err when ipstr is not a valid format
func (loc *Locator) Find(ipstr string) (info *LocationInfo, err error) {
	ip := net.ParseIP(ipstr)
	if ip == nil {
		err = ErrInvalidIP
		return
	}
	info = loc.FindByUint(binary.BigEndian.Uint32([]byte(ip.To4())))
	return
}

// Find locationInfo by uint32
func (loc *Locator) FindByUint(ip uint32) (info *LocationInfo) {
	end := len(loc.indexData1) - 1
	if ip>>16 != 0xffff {
		end = loc.index[(ip>>16)+1]
	}
	idx := loc.findIndexOffset(ip, loc.index[ip>>16], end)
	off := loc.indexData2[idx]
	return newLocationInfo(loc.textData[off : off+loc.indexData3[idx]])
}

// binary search
func (loc *Locator) findIndexOffset(ip uint32, start, end int) int {
	for start < end {
		mid := (start + end) / 2
		if ip > loc.indexData1[mid] {
			start = mid + 1
		} else {
			end = mid
		}
	}

	if loc.indexData1[end] >= ip {
		return end
	}

	return start
}

func (loc *Locator) init(data []byte) error {
	textoff := int(binary.BigEndian.Uint32(data[:4]))
	if textoff < 262144 {
		return ErrInvalidFile{"textoff is smaller than 262144"}
	}

	loc.textData = data[textoff-262144:]

	loc.index = make([]int, 256*256)
	for i := 0; i < 256*256; i++ {
		off := 4 + i*4
		loc.index[i] = int(binary.LittleEndian.Uint32(data[off : off+4]))
	}

	nidx := (textoff - 4 - 262144 - 262144) / 9

	loc.indexData1 = make([]uint32, nidx)
	loc.indexData2 = make([]int, nidx)
	loc.indexData3 = make([]int, nidx)

	for i := 0; i < nidx; i++ {
		off := 4 + 262144 + i*9
		loc.indexData1[i] = binary.BigEndian.Uint32(data[off : off+4])
		loc.indexData2[i] = int(uint32(data[off+4]) | uint32(data[off+5])<<8 | uint32(data[off+6])<<16)
		loc.indexData3[i] = int(uint32(data[off+7])<<8 | uint32(data[off+8]))
	}
	return nil
}

func newLocationInfo(str []byte) *LocationInfo {

	var info *LocationInfo

	fields := bytes.Split(str, []byte("\t"))
	switch len(fields) {
	case 4:
		// free version
		info = &LocationInfo{
			Country: string(fields[0]),
			Region:  string(fields[1]),
			City:    string(fields[2]),
		}
	case 5:
		// pay version
		info = &LocationInfo{
			Country: string(fields[0]),
			Region:  string(fields[1]),
			City:    string(fields[2]),
			Isp:     string(fields[4]),
		}
	case 13:
		info = &LocationInfo{
			Country:      string(fields[0]),
			Region:       string(fields[1]),
			City:         string(fields[2]),
			Isp:          string(fields[4]),
			CountryCode:  string(fields[11]),
			Latitude:     string(fields[5]),
			Longitude:    string(fields[6]),
			DistrictCode: string(fields[9]),
		}
	default:
		//panic("unexpected ip info:" + string(str))
	}

	if len(info.Country) == 0 {
		info.Country = Null
	}
	if len(info.Region) == 0 {
		info.Region = Null
	}
	if len(info.City) == 0 {
		info.City = Null
	}
	if len(info.Isp) == 0 {
		info.Isp = Null
	}
	if len(info.CountryCode) == 0 {
		info.CountryCode = Null
	}
	return info
}
