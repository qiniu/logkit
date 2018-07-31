package ip

import (
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"
)

// mmdbLocator represents an IP loc in datx format.
type mmdbLocator struct {
	language string
	reader   *geoip2.Reader
	path     string
}

func newMmdbLocator(dataFile, language string) (*mmdbLocator, error) {
	reader, err := geoip2.Open(dataFile)
	if err != nil {
		return nil, ErrInvalidFile{"mmdb", err.Error()}
	}
	loc := new(mmdbLocator)
	loc.reader = reader
	loc.path = dataFile
	loc.language = "zh-CN"
	if language != "" {
		loc.language = language
	}
	return loc, nil
}

func (loc *mmdbLocator) Find(ipstr string) (*LocationInfo, error) {
	ip := net.ParseIP(ipstr)
	if ip == nil || ip.To4() == nil {
		return nil, ErrInvalidIP
	}
	info := &LocationInfo{
		Country:      "N/A",
		Region:       "N/A",
		City:         "N/A",
		Isp:          "N/A",
		CountryCode:  "N/A",
		Latitude:     "N/A",
		Longitude:    "N/A",
		DistrictCode: "N/A",
	}

	if c, err := loc.reader.City(ip); err == nil {
		info.Country = c.Country.Names[loc.language]
		info.CountryCode = c.Country.IsoCode
		info.City = c.City.Names[loc.language]
		if len(c.Subdivisions) == 1 {
			info.Region = c.Subdivisions[0].Names[loc.language]
			info.DistrictCode = c.Subdivisions[0].IsoCode
		}
		info.Latitude = fmt.Sprintf("%g", c.Location.Latitude)
		info.Longitude = fmt.Sprintf("%g", c.Location.Longitude)
	}
	if c, err := loc.reader.ISP(ip); err == nil {
		info.Isp = c.ISP
	}

	return info, nil
}

func (loc *mmdbLocator) Close() error {
	if loc.reader != nil {
		locatorStore.Remove(loc.path)
	}
	return nil
}
