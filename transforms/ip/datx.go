package ip

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"net"
)

// datxLocator represents an IP loc in datx format.
type datxLocator struct {
	datLocator
}

// newDatxLocator returns a new datx format IP loc with given data file.
func newDatxLocator(dataFile string) (*datxLocator, error) {
	data, err := ioutil.ReadFile(dataFile)
	if err != nil {
		return nil, err
	}

	loc := new(datxLocator)
	loc.path = dataFile
	if err := loc.init(data); err != nil {
		return nil, err
	}
	return loc, nil
}

func (loc *datxLocator) init(data []byte) error {
	textoff := int(binary.BigEndian.Uint32(data[:4]))
	if textoff < 262144 {
		return ErrInvalidFile{"datx", "textoff is smaller than 262144"}
	} else if len(data) < textoff-262144 {
		return ErrInvalidFile{"datx", "data is incomplete < textoff-262144"}
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

// binary search
func (loc *datxLocator) findIndexOffset(ip uint32, start, end int) int {
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

// LocationInfo stores IP location information.
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

func (*datxLocator) newLocationInfo(str []byte) *LocationInfo {
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

// Find locationInfo by uint32
func (loc *datxLocator) FindByUint(ip uint32) *LocationInfo {
	end := len(loc.indexData1) - 1
	if ip>>16 != 0xffff {
		end = loc.index[(ip>>16)+1]
	}
	idx := loc.findIndexOffset(ip, loc.index[ip>>16], end)
	off := loc.indexData2[idx]
	return loc.newLocationInfo(loc.textData[off : off+loc.indexData3[idx]])
}

// Find locationInfo by ip string
// It will return err when ipstr is not a valid format
func (loc *datxLocator) Find(ipstr string) (*LocationInfo, error) {
	ip := net.ParseIP(ipstr)
	if ip == nil || ip.To4() == nil {
		return nil, ErrInvalidIP
	}
	return loc.FindByUint(binary.BigEndian.Uint32([]byte(ip.To4()))), nil
}

func (loc *datxLocator) Close() error {
	if loc.index != nil && len(loc.index) > 0 {
		locatorStore.Remove(loc.path)
	}
	return nil
}
