// +build darwin
package system

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNumFromOut(t *testing.T) {
	out := `/dev/disk0 (internal):
   #:                       TYPE NAME                    SIZE       IDENTIFIER
   0:      GUID_partition_scheme                         251.0 GB   disk0
   1:                        EFI EFI                     314.6 MB   disk0s1
   2:                 Apple_APFS Container disk1         250.7 GB   disk0s2

/dev/disk1 (synthesized):
   #:                       TYPE NAME                    SIZE       IDENTIFIER
   0:      APFS Container Scheme -                      +250.7 GB   disk1
                                 Physical Store disk0s2
   1:                APFS Volume Macintosh HD            33.9 GB    disk1s1
   2:                APFS Volume Preboot                 22.0 MB    disk1s2
   3:                APFS Volume Recovery                518.1 MB   disk1s3
   4:                APFS Volume VM                      3.2 GB     disk1s4

/dev/disk2 (external, physical):
   #:                       TYPE NAME                    SIZE       IDENTIFIER
   0:     FDisk_partition_scheme                        *16.1 GB    disk2
   1:             Windows_FAT_32 JERRY                   16.1 GB    disk2s4
`
	assert.Equal(t, 3, getNumFromOutput(out))
}
