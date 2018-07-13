package unicodeh

import "unicode"

// Unicode property "Jamo_Short_Name" (known as "JSN", "Jamo_Short_Name").
// Kind of property: "Miscellaneous".
// Based on file "Jamo.txt".
var (
	JamoShortNameA    = jamoShortNameA    // Value "A".
	JamoShortNameAE   = jamoShortNameAE   // Value "AE".
	JamoShortNameB    = jamoShortNameB    // Value "B".
	JamoShortNameBB   = jamoShortNameBB   // Value "BB".
	JamoShortNameBS   = jamoShortNameBS   // Value "BS".
	JamoShortNameC    = jamoShortNameC    // Value "C".
	JamoShortNameD    = jamoShortNameD    // Value "D".
	JamoShortNameDD   = jamoShortNameDD   // Value "DD".
	JamoShortNameE    = jamoShortNameE    // Value "E".
	JamoShortNameEO   = jamoShortNameEO   // Value "EO".
	JamoShortNameEU   = jamoShortNameEU   // Value "EU".
	JamoShortNameG    = jamoShortNameG    // Value "G".
	JamoShortNameGG   = jamoShortNameGG   // Value "GG".
	JamoShortNameGS   = jamoShortNameGS   // Value "GS".
	JamoShortNameH    = jamoShortNameH    // Value "H".
	JamoShortNameI    = jamoShortNameI    // Value "I".
	JamoShortNameJ    = jamoShortNameJ    // Value "J".
	JamoShortNameJJ   = jamoShortNameJJ   // Value "JJ".
	JamoShortNameK    = jamoShortNameK    // Value "K".
	JamoShortNameL    = jamoShortNameL    // Value "L".
	JamoShortNameLB   = jamoShortNameLB   // Value "LB".
	JamoShortNameLG   = jamoShortNameLG   // Value "LG".
	JamoShortNameLH   = jamoShortNameLH   // Value "LH".
	JamoShortNameLM   = jamoShortNameLM   // Value "LM".
	JamoShortNameLP   = jamoShortNameLP   // Value "LP".
	JamoShortNameLS   = jamoShortNameLS   // Value "LS".
	JamoShortNameLT   = jamoShortNameLT   // Value "LT".
	JamoShortNameM    = jamoShortNameM    // Value "M".
	JamoShortNameN    = jamoShortNameN    // Value "N".
	JamoShortNameNG   = jamoShortNameNG   // Value "NG".
	JamoShortNameNH   = jamoShortNameNH   // Value "NH".
	JamoShortNameNJ   = jamoShortNameNJ   // Value "NJ".
	JamoShortNameO    = jamoShortNameO    // Value "O".
	JamoShortNameOE   = jamoShortNameOE   // Value "OE".
	JamoShortNameP    = jamoShortNameP    // Value "P".
	JamoShortNameR    = jamoShortNameR    // Value "R".
	JamoShortNameS    = jamoShortNameS    // Value "S".
	JamoShortNameSS   = jamoShortNameSS   // Value "SS".
	JamoShortNameT    = jamoShortNameT    // Value "T".
	JamoShortNameU    = jamoShortNameU    // Value "U".
	JamoShortNameWA   = jamoShortNameWA   // Value "WA".
	JamoShortNameWAE  = jamoShortNameWAE  // Value "WAE".
	JamoShortNameWE   = jamoShortNameWE   // Value "WE".
	JamoShortNameWEO  = jamoShortNameWEO  // Value "WEO".
	JamoShortNameWI   = jamoShortNameWI   // Value "WI".
	JamoShortNameYA   = jamoShortNameYA   // Value "YA".
	JamoShortNameYAE  = jamoShortNameYAE  // Value "YAE".
	JamoShortNameYE   = jamoShortNameYE   // Value "YE".
	JamoShortNameYEO  = jamoShortNameYEO  // Value "YEO".
	JamoShortNameYI   = jamoShortNameYI   // Value "YI".
	JamoShortNameYO   = jamoShortNameYO   // Value "YO".
	JamoShortNameYU   = jamoShortNameYU   // Value "YU".
	JamoShortNameNone = jamoShortNameNone // Value "None" (known as "", "None", "<none>").
)

var (
	jamoShortNameA    = &unicode.RangeTable{[]unicode.Range16{{0x1161, 0x1161, 0x1}}, nil, 0}
	jamoShortNameAE   = &unicode.RangeTable{[]unicode.Range16{{0x1162, 0x1162, 0x1}}, nil, 0}
	jamoShortNameB    = &unicode.RangeTable{[]unicode.Range16{{0x1107, 0x11b8, 0xb1}}, nil, 0}
	jamoShortNameBB   = &unicode.RangeTable{[]unicode.Range16{{0x1108, 0x1108, 0x1}}, nil, 0}
	jamoShortNameBS   = &unicode.RangeTable{[]unicode.Range16{{0x11b9, 0x11b9, 0x1}}, nil, 0}
	jamoShortNameC    = &unicode.RangeTable{[]unicode.Range16{{0x110e, 0x11be, 0xb0}}, nil, 0}
	jamoShortNameD    = &unicode.RangeTable{[]unicode.Range16{{0x1103, 0x11ae, 0xab}}, nil, 0}
	jamoShortNameDD   = &unicode.RangeTable{[]unicode.Range16{{0x1104, 0x1104, 0x1}}, nil, 0}
	jamoShortNameE    = &unicode.RangeTable{[]unicode.Range16{{0x1166, 0x1166, 0x1}}, nil, 0}
	jamoShortNameEO   = &unicode.RangeTable{[]unicode.Range16{{0x1165, 0x1165, 0x1}}, nil, 0}
	jamoShortNameEU   = &unicode.RangeTable{[]unicode.Range16{{0x1173, 0x1173, 0x1}}, nil, 0}
	jamoShortNameG    = &unicode.RangeTable{[]unicode.Range16{{0x1100, 0x11a8, 0xa8}}, nil, 0}
	jamoShortNameGG   = &unicode.RangeTable{[]unicode.Range16{{0x1101, 0x11a9, 0xa8}}, nil, 0}
	jamoShortNameGS   = &unicode.RangeTable{[]unicode.Range16{{0x11aa, 0x11aa, 0x1}}, nil, 0}
	jamoShortNameH    = &unicode.RangeTable{[]unicode.Range16{{0x1112, 0x11c2, 0xb0}}, nil, 0}
	jamoShortNameI    = &unicode.RangeTable{[]unicode.Range16{{0x1175, 0x1175, 0x1}}, nil, 0}
	jamoShortNameJ    = &unicode.RangeTable{[]unicode.Range16{{0x110c, 0x11bd, 0xb1}}, nil, 0}
	jamoShortNameJJ   = &unicode.RangeTable{[]unicode.Range16{{0x110d, 0x110d, 0x1}}, nil, 0}
	jamoShortNameK    = &unicode.RangeTable{[]unicode.Range16{{0x110f, 0x11bf, 0xb0}}, nil, 0}
	jamoShortNameL    = &unicode.RangeTable{[]unicode.Range16{{0x11af, 0x11af, 0x1}}, nil, 0}
	jamoShortNameLB   = &unicode.RangeTable{[]unicode.Range16{{0x11b2, 0x11b2, 0x1}}, nil, 0}
	jamoShortNameLG   = &unicode.RangeTable{[]unicode.Range16{{0x11b0, 0x11b0, 0x1}}, nil, 0}
	jamoShortNameLH   = &unicode.RangeTable{[]unicode.Range16{{0x11b6, 0x11b6, 0x1}}, nil, 0}
	jamoShortNameLM   = &unicode.RangeTable{[]unicode.Range16{{0x11b1, 0x11b1, 0x1}}, nil, 0}
	jamoShortNameLP   = &unicode.RangeTable{[]unicode.Range16{{0x11b5, 0x11b5, 0x1}}, nil, 0}
	jamoShortNameLS   = &unicode.RangeTable{[]unicode.Range16{{0x11b3, 0x11b3, 0x1}}, nil, 0}
	jamoShortNameLT   = &unicode.RangeTable{[]unicode.Range16{{0x11b4, 0x11b4, 0x1}}, nil, 0}
	jamoShortNameM    = &unicode.RangeTable{[]unicode.Range16{{0x1106, 0x11b7, 0xb1}}, nil, 0}
	jamoShortNameN    = &unicode.RangeTable{[]unicode.Range16{{0x1102, 0x11ab, 0xa9}}, nil, 0}
	jamoShortNameNG   = &unicode.RangeTable{[]unicode.Range16{{0x11bc, 0x11bc, 0x1}}, nil, 0}
	jamoShortNameNH   = &unicode.RangeTable{[]unicode.Range16{{0x11ad, 0x11ad, 0x1}}, nil, 0}
	jamoShortNameNJ   = &unicode.RangeTable{[]unicode.Range16{{0x11ac, 0x11ac, 0x1}}, nil, 0}
	jamoShortNameO    = &unicode.RangeTable{[]unicode.Range16{{0x1169, 0x1169, 0x1}}, nil, 0}
	jamoShortNameOE   = &unicode.RangeTable{[]unicode.Range16{{0x116c, 0x116c, 0x1}}, nil, 0}
	jamoShortNameP    = &unicode.RangeTable{[]unicode.Range16{{0x1111, 0x11c1, 0xb0}}, nil, 0}
	jamoShortNameR    = &unicode.RangeTable{[]unicode.Range16{{0x1105, 0x1105, 0x1}}, nil, 0}
	jamoShortNameS    = &unicode.RangeTable{[]unicode.Range16{{0x1109, 0x11ba, 0xb1}}, nil, 0}
	jamoShortNameSS   = &unicode.RangeTable{[]unicode.Range16{{0x110a, 0x11bb, 0xb1}}, nil, 0}
	jamoShortNameT    = &unicode.RangeTable{[]unicode.Range16{{0x1110, 0x11c0, 0xb0}}, nil, 0}
	jamoShortNameU    = &unicode.RangeTable{[]unicode.Range16{{0x116e, 0x116e, 0x1}}, nil, 0}
	jamoShortNameWA   = &unicode.RangeTable{[]unicode.Range16{{0x116a, 0x116a, 0x1}}, nil, 0}
	jamoShortNameWAE  = &unicode.RangeTable{[]unicode.Range16{{0x116b, 0x116b, 0x1}}, nil, 0}
	jamoShortNameWE   = &unicode.RangeTable{[]unicode.Range16{{0x1170, 0x1170, 0x1}}, nil, 0}
	jamoShortNameWEO  = &unicode.RangeTable{[]unicode.Range16{{0x116f, 0x116f, 0x1}}, nil, 0}
	jamoShortNameWI   = &unicode.RangeTable{[]unicode.Range16{{0x1171, 0x1171, 0x1}}, nil, 0}
	jamoShortNameYA   = &unicode.RangeTable{[]unicode.Range16{{0x1163, 0x1163, 0x1}}, nil, 0}
	jamoShortNameYAE  = &unicode.RangeTable{[]unicode.Range16{{0x1164, 0x1164, 0x1}}, nil, 0}
	jamoShortNameYE   = &unicode.RangeTable{[]unicode.Range16{{0x1168, 0x1168, 0x1}}, nil, 0}
	jamoShortNameYEO  = &unicode.RangeTable{[]unicode.Range16{{0x1167, 0x1167, 0x1}}, nil, 0}
	jamoShortNameYI   = &unicode.RangeTable{[]unicode.Range16{{0x1174, 0x1174, 0x1}}, nil, 0}
	jamoShortNameYO   = &unicode.RangeTable{[]unicode.Range16{{0x116d, 0x116d, 0x1}}, nil, 0}
	jamoShortNameYU   = &unicode.RangeTable{[]unicode.Range16{{0x1172, 0x1172, 0x1}}, nil, 0}
	jamoShortNameNone = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x10ff, 0x1}, {0x110b, 0x1113, 0x8}, {0x1114, 0x1160, 0x1}, {0x1176, 0x11a7, 0x1}, {0x11c3, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
)
