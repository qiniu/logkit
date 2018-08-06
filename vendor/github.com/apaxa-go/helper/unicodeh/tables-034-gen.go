package unicodeh

import "unicode"

// Unicode property "Changes_When_Casemapped" (known as "CWCM", "Changes_When_Casemapped").
// Kind of property: "Binary".
// Based on file "DerivedCoreProperties.txt".
var (
	ChangesWhenCasemappedNo  = changesWhenCasemappedNo  // Value "No" (known as "N", "No", "F", "False").
	ChangesWhenCasemappedYes = changesWhenCasemappedYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	changesWhenCasemappedNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x40, 0x1}, {0x5b, 0x60, 0x1}, {0x7b, 0xb4, 0x1}, {0xb6, 0xbf, 0x1}, {0xd7, 0xf7, 0x20}, {0x138, 0x18d, 0x55}, {0x19b, 0x1aa, 0xf}, {0x1ab, 0x1ba, 0xf}, {0x1bb, 0x1be, 0x3}, {0x1c0, 0x1c3, 0x1}, {0x221, 0x234, 0x13}, {0x235, 0x239, 0x1}, {0x255, 0x258, 0x3}, {0x25a, 0x25d, 0x3}, {0x25e, 0x25f, 0x1}, {0x262, 0x264, 0x2}, {0x267, 0x26d, 0x6}, {0x26e, 0x270, 0x2}, {0x273, 0x274, 0x1}, {0x276, 0x27c, 0x1}, {0x27e, 0x27f, 0x1}, {0x281, 0x282, 0x1}, {0x284, 0x286, 0x1}, {0x28d, 0x291, 0x1}, {0x293, 0x29c, 0x1}, {0x29f, 0x344, 0x1}, {0x346, 0x36f, 0x1}, {0x374, 0x375, 0x1}, {0x378, 0x37a, 0x1}, {0x37e, 0x380, 0x2}, {0x381, 0x385, 0x1}, {0x387, 0x38b, 0x4}, {0x38d, 0x3a2, 0x15}, {0x3d2, 0x3d4, 0x1}, {0x3f6, 0x3fc, 0x6}, {0x482, 0x489, 0x1}, {0x530, 0x557, 0x27}, {0x558, 0x560, 0x1}, {0x588, 0x109f, 0x1}, {0x10c6, 0x10c8, 0x2}, {0x10c9, 0x10cc, 0x1}, {0x10ce, 0x139f, 0x1}, {0x13f6, 0x13f7, 0x1}, {0x13fe, 0x1c7f, 0x1}, {0x1c89, 0x1d78, 0x1}, {0x1d7a, 0x1d7c, 0x1}, {0x1d7e, 0x1dff, 0x1}, {0x1e9c, 0x1e9d, 0x1}, {0x1e9f, 0x1f16, 0x77}, {0x1f17, 0x1f1e, 0x7}, {0x1f1f, 0x1f46, 0x27}, {0x1f47, 0x1f4e, 0x7}, {0x1f4f, 0x1f58, 0x9}, {0x1f5a, 0x1f5e, 0x2}, {0x1f7e, 0x1f7f, 0x1}, {0x1fb5, 0x1fbd, 0x8}, {0x1fbf, 0x1fc1, 0x1}, {0x1fc5, 0x1fcd, 0x8}, {0x1fce, 0x1fcf, 0x1}, {0x1fd4, 0x1fd5, 0x1}, {0x1fdc, 0x1fdf, 0x1}, {0x1fed, 0x1ff1, 0x1}, {0x1ff5, 0x1ffd, 0x8}, {0x1ffe, 0x2125, 0x1}, {0x2127, 0x2129, 0x1}, {0x212c, 0x2131, 0x1}, {0x2133, 0x214d, 0x1}, {0x214f, 0x215f, 0x1}, {0x2180, 0x2182, 0x1}, {0x2185, 0x24b5, 0x1}, {0x24ea, 0x2bff, 0x1}, {0x2c2f, 0x2c5f, 0x30}, {0x2c71, 0x2c77, 0x3}, {0x2c78, 0x2c7d, 0x1}, {0x2ce4, 0x2cea, 0x1}, {0x2cef, 0x2cf1, 0x1}, {0x2cf4, 0x2cff, 0x1}, {0x2d26, 0x2d28, 0x2}, {0x2d29, 0x2d2c, 0x1}, {0x2d2e, 0xa63f, 0x1}, {0xa66e, 0xa67f, 0x1}, {0xa69c, 0xa721, 0x1}, {0xa730, 0xa731, 0x1}, {0xa770, 0xa778, 0x1}, {0xa788, 0xa78a, 0x1}, {0xa78e, 0xa78f, 0x1}, {0xa794, 0xa795, 0x1}, {0xa7af, 0xa7b8, 0x9}, {0xa7b9, 0xab52, 0x1}, {0xab54, 0xab6f, 0x1}, {0xabc0, 0xfaff, 0x1}, {0xfb07, 0xfb12, 0x1}, {0xfb18, 0xff20, 0x1}, {0xff3b, 0xff40, 0x1}, {0xff5b, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x103ff, 0x1}, {0x10450, 0x104af, 0x1}, {0x104d4, 0x104d7, 0x1}, {0x104fc, 0x10c7f, 0x1}, {0x10cb3, 0x10cbf, 0x1}, {0x10cf3, 0x1189f, 0x1}, {0x118e0, 0x1e8ff, 0x1}, {0x1e944, 0x10ffff, 0x1}}, 5}
	changesWhenCasemappedYes = &unicode.RangeTable{[]unicode.Range16{{0x41, 0x5a, 0x1}, {0x61, 0x7a, 0x1}, {0xb5, 0xc0, 0xb}, {0xc1, 0xd6, 0x1}, {0xd8, 0xf6, 0x1}, {0xf8, 0x137, 0x1}, {0x139, 0x18c, 0x1}, {0x18e, 0x19a, 0x1}, {0x19c, 0x1a9, 0x1}, {0x1ac, 0x1b9, 0x1}, {0x1bc, 0x1bd, 0x1}, {0x1bf, 0x1c4, 0x5}, {0x1c5, 0x220, 0x1}, {0x222, 0x233, 0x1}, {0x23a, 0x254, 0x1}, {0x256, 0x257, 0x1}, {0x259, 0x25b, 0x2}, {0x25c, 0x260, 0x4}, {0x261, 0x265, 0x2}, {0x266, 0x268, 0x2}, {0x269, 0x26c, 0x1}, {0x26f, 0x271, 0x2}, {0x272, 0x275, 0x3}, {0x27d, 0x283, 0x3}, {0x287, 0x28c, 0x1}, {0x292, 0x29d, 0xb}, {0x29e, 0x345, 0xa7}, {0x370, 0x373, 0x1}, {0x376, 0x377, 0x1}, {0x37b, 0x37d, 0x1}, {0x37f, 0x386, 0x7}, {0x388, 0x38a, 0x1}, {0x38c, 0x38e, 0x2}, {0x38f, 0x3a1, 0x1}, {0x3a3, 0x3d1, 0x1}, {0x3d5, 0x3f5, 0x1}, {0x3f7, 0x3fb, 0x1}, {0x3fd, 0x481, 0x1}, {0x48a, 0x52f, 0x1}, {0x531, 0x556, 0x1}, {0x561, 0x587, 0x1}, {0x10a0, 0x10c5, 0x1}, {0x10c7, 0x10cd, 0x6}, {0x13a0, 0x13f5, 0x1}, {0x13f8, 0x13fd, 0x1}, {0x1c80, 0x1c88, 0x1}, {0x1d79, 0x1d7d, 0x4}, {0x1e00, 0x1e9b, 0x1}, {0x1e9e, 0x1ea0, 0x2}, {0x1ea1, 0x1f15, 0x1}, {0x1f18, 0x1f1d, 0x1}, {0x1f20, 0x1f45, 0x1}, {0x1f48, 0x1f4d, 0x1}, {0x1f50, 0x1f57, 0x1}, {0x1f59, 0x1f5f, 0x2}, {0x1f60, 0x1f7d, 0x1}, {0x1f80, 0x1fb4, 0x1}, {0x1fb6, 0x1fbc, 0x1}, {0x1fbe, 0x1fc2, 0x4}, {0x1fc3, 0x1fc4, 0x1}, {0x1fc6, 0x1fcc, 0x1}, {0x1fd0, 0x1fd3, 0x1}, {0x1fd6, 0x1fdb, 0x1}, {0x1fe0, 0x1fec, 0x1}, {0x1ff2, 0x1ff4, 0x1}, {0x1ff6, 0x1ffc, 0x1}, {0x2126, 0x212a, 0x4}, {0x212b, 0x2132, 0x7}, {0x214e, 0x2160, 0x12}, {0x2161, 0x217f, 0x1}, {0x2183, 0x2184, 0x1}, {0x24b6, 0x24e9, 0x1}, {0x2c00, 0x2c2e, 0x1}, {0x2c30, 0x2c5e, 0x1}, {0x2c60, 0x2c70, 0x1}, {0x2c72, 0x2c73, 0x1}, {0x2c75, 0x2c76, 0x1}, {0x2c7e, 0x2ce3, 0x1}, {0x2ceb, 0x2cee, 0x1}, {0x2cf2, 0x2cf3, 0x1}, {0x2d00, 0x2d25, 0x1}, {0x2d27, 0x2d2d, 0x6}, {0xa640, 0xa66d, 0x1}, {0xa680, 0xa69b, 0x1}, {0xa722, 0xa72f, 0x1}, {0xa732, 0xa76f, 0x1}, {0xa779, 0xa787, 0x1}, {0xa78b, 0xa78d, 0x1}, {0xa790, 0xa793, 0x1}, {0xa796, 0xa7ae, 0x1}, {0xa7b0, 0xa7b7, 0x1}, {0xab53, 0xab70, 0x1d}, {0xab71, 0xabbf, 0x1}, {0xfb00, 0xfb06, 0x1}, {0xfb13, 0xfb17, 0x1}, {0xff21, 0xff3a, 0x1}, {0xff41, 0xff5a, 0x1}}, []unicode.Range32{{0x10400, 0x1044f, 0x1}, {0x104b0, 0x104d3, 0x1}, {0x104d8, 0x104fb, 0x1}, {0x10c80, 0x10cb2, 0x1}, {0x10cc0, 0x10cf2, 0x1}, {0x118a0, 0x118df, 0x1}, {0x1e900, 0x1e943, 0x1}}, 5}
)
