package unicodeh

import "unicode"

// Unicode property "Vertical_Orientation" (known as "vo", "Vertical_Orientation").
// Kind of property: "Enumerated".
// Based on file "VerticalOrientation.txt".
var (
	VerticalOrientationRotated            = verticalOrientationRotated            // Value "Rotated" (known as "R", "Rotated").
	VerticalOrientationTransformedRotated = verticalOrientationTransformedRotated // Value "Transformed_Rotated" (known as "Tr", "Transformed_Rotated").
	VerticalOrientationTransformedUpright = verticalOrientationTransformedUpright // Value "Transformed_Upright" (known as "Tu", "Transformed_Upright").
	VerticalOrientationUpright            = verticalOrientationUpright            // Value "Upright" (known as "U", "Upright").
)

var (
	verticalOrientationRotated            = &unicode.RangeTable{[]unicode.Range16{{0x0, 0xa6, 0x1}, {0xa8, 0xaa, 0x2}, {0xab, 0xad, 0x1}, {0xaf, 0xb0, 0x1}, {0xb2, 0xbb, 0x1}, {0xbf, 0xd6, 0x1}, {0xd8, 0xf6, 0x1}, {0xf8, 0x2e9, 0x1}, {0x2ec, 0x10ff, 0x1}, {0x1200, 0x1400, 0x1}, {0x1680, 0x18af, 0x1}, {0x1900, 0x2015, 0x1}, {0x2017, 0x201f, 0x1}, {0x2022, 0x202f, 0x1}, {0x2032, 0x203a, 0x1}, {0x203d, 0x2041, 0x1}, {0x2043, 0x2046, 0x1}, {0x204a, 0x2050, 0x1}, {0x2052, 0x2064, 0x1}, {0x2066, 0x20dc, 0x1}, {0x20e1, 0x20e5, 0x4}, {0x20e6, 0x20ff, 0x1}, {0x2102, 0x210a, 0x8}, {0x210b, 0x210e, 0x1}, {0x2110, 0x2112, 0x1}, {0x2115, 0x2118, 0x3}, {0x2119, 0x211d, 0x1}, {0x2124, 0x212a, 0x2}, {0x212b, 0x212d, 0x1}, {0x212f, 0x2134, 0x1}, {0x2140, 0x2144, 0x1}, {0x214b, 0x214e, 0x3}, {0x218a, 0x218b, 0x1}, {0x2190, 0x221d, 0x1}, {0x221f, 0x2233, 0x1}, {0x2236, 0x22ff, 0x1}, {0x2308, 0x230b, 0x1}, {0x2320, 0x2323, 0x1}, {0x232c, 0x237c, 0x1}, {0x239b, 0x23bd, 0x1}, {0x23ce, 0x23d0, 0x2}, {0x23dc, 0x23e1, 0x1}, {0x2423, 0x2500, 0xdd}, {0x2501, 0x259f, 0x1}, {0x261a, 0x261f, 0x1}, {0x2768, 0x2775, 0x1}, {0x2794, 0x2b11, 0x1}, {0x2b30, 0x2b4f, 0x1}, {0x2b5a, 0x2bb7, 0x1}, {0x2bd2, 0x2bec, 0x1a}, {0x2bed, 0x2bef, 0x1}, {0x2c00, 0x2e7f, 0x1}, {0xa4d0, 0xa95f, 0x1}, {0xa980, 0xabff, 0x1}, {0xd800, 0xdfff, 0x1}, {0xfb00, 0xfe0f, 0x1}, {0xfe20, 0xfe2f, 0x1}, {0xfe49, 0xfe4f, 0x1}, {0xfe58, 0xfe63, 0xb}, {0xfe64, 0xfe66, 0x1}, {0xfe70, 0xff00, 0x1}, {0xff0d, 0xff1c, 0xf}, {0xff1d, 0xff1e, 0x1}, {0xff61, 0xffdf, 0x1}, {0xffe8, 0xffef, 0x1}, {0xfff9, 0xfffb, 0x1}, {0xfffe, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x1097f, 0x1}, {0x109a0, 0x1157f, 0x1}, {0x11600, 0x119ff, 0x1}, {0x11ab0, 0x12fff, 0x1}, {0x13430, 0x143ff, 0x1}, {0x14680, 0x16fdf, 0x1}, {0x18b00, 0x1afff, 0x1}, {0x1b130, 0x1b16f, 0x1}, {0x1b300, 0x1cfff, 0x1}, {0x1d200, 0x1d2ff, 0x1}, {0x1d380, 0x1d7ff, 0x1}, {0x1dab0, 0x1efff, 0x1}, {0x1f800, 0x1f8ff, 0x1}, {0x1fa00, 0x1ffff, 0x1}, {0x2fffe, 0x2ffff, 0x1}, {0x3fffe, 0xeffff, 0x1}, {0xffffe, 0xfffff, 0x1}, {0x10fffe, 0x10ffff, 0x1}}, 7}
	verticalOrientationTransformedRotated = &unicode.RangeTable{[]unicode.Range16{{0x2329, 0x232a, 0x1}, {0x3008, 0x3011, 0x1}, {0x3014, 0x301f, 0x1}, {0x3030, 0x30a0, 0x70}, {0x30fc, 0xfe59, 0xcd5d}, {0xfe5a, 0xfe5e, 0x1}, {0xff08, 0xff09, 0x1}, {0xff1a, 0xff1b, 0x1}, {0xff3b, 0xff3f, 0x2}, {0xff5b, 0xff60, 0x1}, {0xffe3, 0xffe3, 0x1}}, nil, 0}
	verticalOrientationTransformedUpright = &unicode.RangeTable{[]unicode.Range16{{0x3001, 0x3002, 0x1}, {0x3041, 0x3049, 0x2}, {0x3063, 0x3083, 0x20}, {0x3085, 0x3087, 0x2}, {0x308e, 0x3095, 0x7}, {0x3096, 0x309b, 0x5}, {0x309c, 0x30a1, 0x5}, {0x30a3, 0x30a9, 0x2}, {0x30c3, 0x30e3, 0x20}, {0x30e5, 0x30e7, 0x2}, {0x30ee, 0x30f5, 0x7}, {0x30f6, 0x3127, 0x31}, {0x31f0, 0x31ff, 0x1}, {0x3300, 0x3357, 0x1}, {0x337b, 0x337f, 0x1}, {0xfe50, 0xfe52, 0x1}, {0xff01, 0xff0c, 0xb}, {0xff0e, 0xff1f, 0x11}}, []unicode.Range32{{0x1f200, 0x1f201, 0x1}}, 0}
	verticalOrientationUpright            = &unicode.RangeTable{[]unicode.Range16{{0xa7, 0xa9, 0x2}, {0xae, 0xb1, 0x3}, {0xbc, 0xbe, 0x1}, {0xd7, 0xf7, 0x20}, {0x2ea, 0x2eb, 0x1}, {0x1100, 0x11ff, 0x1}, {0x1401, 0x167f, 0x1}, {0x18b0, 0x18ff, 0x1}, {0x2016, 0x2020, 0xa}, {0x2021, 0x2030, 0xf}, {0x2031, 0x203b, 0xa}, {0x203c, 0x2042, 0x6}, {0x2047, 0x2049, 0x1}, {0x2051, 0x2065, 0x14}, {0x20dd, 0x20e0, 0x1}, {0x20e2, 0x20e4, 0x1}, {0x2100, 0x2101, 0x1}, {0x2103, 0x2109, 0x1}, {0x210f, 0x2113, 0x4}, {0x2114, 0x2116, 0x2}, {0x2117, 0x211e, 0x7}, {0x211f, 0x2123, 0x1}, {0x2125, 0x2129, 0x2}, {0x212e, 0x2135, 0x7}, {0x2136, 0x213f, 0x1}, {0x2145, 0x214a, 0x1}, {0x214c, 0x214d, 0x1}, {0x214f, 0x2189, 0x1}, {0x218c, 0x218f, 0x1}, {0x221e, 0x2234, 0x16}, {0x2235, 0x2300, 0xcb}, {0x2301, 0x2307, 0x1}, {0x230c, 0x231f, 0x1}, {0x2324, 0x2328, 0x1}, {0x232b, 0x237d, 0x52}, {0x237e, 0x239a, 0x1}, {0x23be, 0x23cd, 0x1}, {0x23cf, 0x23d1, 0x2}, {0x23d2, 0x23db, 0x1}, {0x23e2, 0x2422, 0x1}, {0x2424, 0x24ff, 0x1}, {0x25a0, 0x2619, 0x1}, {0x2620, 0x2767, 0x1}, {0x2776, 0x2793, 0x1}, {0x2b12, 0x2b2f, 0x1}, {0x2b50, 0x2b59, 0x1}, {0x2bb8, 0x2bd1, 0x1}, {0x2bd3, 0x2beb, 0x1}, {0x2bf0, 0x2bff, 0x1}, {0x2e80, 0x3000, 0x1}, {0x3003, 0x3007, 0x1}, {0x3012, 0x3013, 0x1}, {0x3020, 0x302f, 0x1}, {0x3031, 0x3040, 0x1}, {0x3042, 0x304a, 0x2}, {0x304b, 0x3062, 0x1}, {0x3064, 0x3082, 0x1}, {0x3084, 0x3088, 0x2}, {0x3089, 0x308d, 0x1}, {0x308f, 0x3094, 0x1}, {0x3097, 0x309a, 0x1}, {0x309d, 0x309f, 0x1}, {0x30a2, 0x30aa, 0x2}, {0x30ab, 0x30c2, 0x1}, {0x30c4, 0x30e2, 0x1}, {0x30e4, 0x30e8, 0x2}, {0x30e9, 0x30ed, 0x1}, {0x30ef, 0x30f4, 0x1}, {0x30f7, 0x30fb, 0x1}, {0x30fd, 0x3126, 0x1}, {0x3128, 0x31ef, 0x1}, {0x3200, 0x32ff, 0x1}, {0x3358, 0x337a, 0x1}, {0x3380, 0xa4cf, 0x1}, {0xa960, 0xa97f, 0x1}, {0xac00, 0xd7ff, 0x1}, {0xe000, 0xfaff, 0x1}, {0xfe10, 0xfe1f, 0x1}, {0xfe30, 0xfe48, 0x1}, {0xfe53, 0xfe57, 0x1}, {0xfe5f, 0xfe62, 0x1}, {0xfe67, 0xfe6f, 0x1}, {0xff02, 0xff07, 0x1}, {0xff0a, 0xff0b, 0x1}, {0xff0f, 0xff19, 0x1}, {0xff20, 0xff3a, 0x1}, {0xff3c, 0xff40, 0x2}, {0xff41, 0xff5a, 0x1}, {0xffe0, 0xffe2, 0x1}, {0xffe4, 0xffe7, 0x1}, {0xfff0, 0xfff8, 0x1}, {0xfffc, 0xfffd, 0x1}}, []unicode.Range32{{0x10980, 0x1099f, 0x1}, {0x11580, 0x115ff, 0x1}, {0x11a00, 0x11aaf, 0x1}, {0x13000, 0x1342f, 0x1}, {0x14400, 0x1467f, 0x1}, {0x16fe0, 0x18aff, 0x1}, {0x1b000, 0x1b12f, 0x1}, {0x1b170, 0x1b2ff, 0x1}, {0x1d000, 0x1d1ff, 0x1}, {0x1d300, 0x1d37f, 0x1}, {0x1d800, 0x1daaf, 0x1}, {0x1f000, 0x1f1ff, 0x1}, {0x1f202, 0x1f7ff, 0x1}, {0x1f900, 0x1f9ff, 0x1}, {0x20000, 0x2fffd, 0x1}, {0x30000, 0x3fffd, 0x1}, {0xf0000, 0xffffd, 0x1}, {0x100000, 0x10fffd, 0x1}}, 4}
)
