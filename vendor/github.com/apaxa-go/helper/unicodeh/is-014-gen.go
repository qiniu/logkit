package unicodeh

import "unicode"

// IsJoiningGroupAfricanFeh reports whether the rune has property "Joining_Group"="African_Feh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupAfricanFeh(r rune) bool { return r == 0x8bb }

// IsJoiningGroupAfricanNoon reports whether the rune has property "Joining_Group"="African_Noon".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupAfricanNoon(r rune) bool { return r == 0x8bd }

// IsJoiningGroupAfricanQaf reports whether the rune has property "Joining_Group"="African_Qaf".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupAfricanQaf(r rune) bool { return r == 0x8bc }

// IsJoiningGroupAin reports whether the rune has property "Joining_Group"="Ain".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupAin(r rune) bool {
	return r == 0x639 || r == 0x63a || r == 0x6a0 || r == 0x6fc || (r >= 0x75d && r <= 0x75f) || r == 0x8b3
}

// IsJoiningGroupAlaph reports whether the rune has property "Joining_Group"="Alaph".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupAlaph(r rune) bool { return r == 0x710 }

// IsJoiningGroupAlef reports whether the rune has property "Joining_Group"="Alef".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupAlef(r rune) bool { return unicode.Is(JoiningGroupAlef, r) }

// IsJoiningGroupBeh reports whether the rune has property "Joining_Group"="Beh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupBeh(r rune) bool { return unicode.Is(JoiningGroupBeh, r) }

// IsJoiningGroupBeth reports whether the rune has property "Joining_Group"="Beth".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupBeth(r rune) bool { return r == 0x712 || r == 0x72d }

// IsJoiningGroupBurushaskiYehBarree reports whether the rune has property "Joining_Group"="Burushaski_Yeh_Barree".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupBurushaskiYehBarree(r rune) bool { return r == 0x77a || r == 0x77b }

// IsJoiningGroupDal reports whether the rune has property "Joining_Group"="Dal".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupDal(r rune) bool {
	return r == 0x62f || r == 0x630 || (r >= 0x688 && r <= 0x690) || r == 0x6ee || r == 0x759 || r == 0x75a || r == 0x8ae
}

// IsJoiningGroupDalathRish reports whether the rune has property "Joining_Group"="Dalath_Rish".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupDalathRish(r rune) bool {
	return r == 0x715 || r == 0x716 || r == 0x72a || r == 0x72f
}

// IsJoiningGroupE reports whether the rune has property "Joining_Group"="E".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupE(r rune) bool { return r == 0x725 }

// IsJoiningGroupFarsiYeh reports whether the rune has property "Joining_Group"="Farsi_Yeh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupFarsiYeh(r rune) bool {
	return (r >= 0x63d && r <= 0x63f) || r == 0x6cc || r == 0x6ce || r == 0x775 || r == 0x776
}

// IsJoiningGroupFe reports whether the rune has property "Joining_Group"="Fe".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupFe(r rune) bool { return r == 0x74f }

// IsJoiningGroupFeh reports whether the rune has property "Joining_Group"="Feh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupFeh(r rune) bool {
	return r == 0x641 || r == 0x6a1 || (r >= 0x6a2 && r <= 0x6a6) || r == 0x760 || r == 0x761 || r == 0x8a4
}

// IsJoiningGroupFinalSemkath reports whether the rune has property "Joining_Group"="Final_Semkath".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupFinalSemkath(r rune) bool { return r == 0x724 }

// IsJoiningGroupGaf reports whether the rune has property "Joining_Group"="Gaf".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupGaf(r rune) bool { return unicode.Is(JoiningGroupGaf, r) }

// IsJoiningGroupGamal reports whether the rune has property "Joining_Group"="Gamal".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupGamal(r rune) bool { return r == 0x713 || r == 0x714 || r == 0x72e }

// IsJoiningGroupHah reports whether the rune has property "Joining_Group"="Hah".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupHah(r rune) bool { return unicode.Is(JoiningGroupHah, r) }

// IsJoiningGroupHe reports whether the rune has property "Joining_Group"="He".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupHe(r rune) bool { return r == 0x717 }

// IsJoiningGroupHeh reports whether the rune has property "Joining_Group"="Heh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupHeh(r rune) bool { return r == 0x647 }

// IsJoiningGroupHehGoal reports whether the rune has property "Joining_Group"="Heh_Goal".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupHehGoal(r rune) bool { return r == 0x6c1 || r == 0x6c2 }

// IsJoiningGroupHeth reports whether the rune has property "Joining_Group"="Heth".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupHeth(r rune) bool { return r == 0x71a }

// IsJoiningGroupKaf reports whether the rune has property "Joining_Group"="Kaf".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupKaf(r rune) bool {
	return r == 0x643 || r == 0x6ac || r == 0x6ad || r == 0x6ae || r == 0x77f || r == 0x8b4
}

// IsJoiningGroupKaph reports whether the rune has property "Joining_Group"="Kaph".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupKaph(r rune) bool { return r == 0x71f }

// IsJoiningGroupKhaph reports whether the rune has property "Joining_Group"="Khaph".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupKhaph(r rune) bool { return r == 0x74e }

// IsJoiningGroupKnottedHeh reports whether the rune has property "Joining_Group"="Knotted_Heh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupKnottedHeh(r rune) bool { return r == 0x6be || r == 0x6ff }

// IsJoiningGroupLam reports whether the rune has property "Joining_Group"="Lam".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupLam(r rune) bool {
	return r == 0x644 || r == 0x6b5 || (r >= 0x6b6 && r <= 0x6b8) || r == 0x76a || r == 0x8a6
}

// IsJoiningGroupLamadh reports whether the rune has property "Joining_Group"="Lamadh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupLamadh(r rune) bool { return r == 0x720 }

// IsJoiningGroupMalayalamBha reports whether the rune has property "Joining_Group"="Malayalam_Bha".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamBha(r rune) bool { return r == 0x866 }

// IsJoiningGroupMalayalamJa reports whether the rune has property "Joining_Group"="Malayalam_Ja".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamJa(r rune) bool { return r == 0x861 }

// IsJoiningGroupMalayalamLla reports whether the rune has property "Joining_Group"="Malayalam_Lla".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamLla(r rune) bool { return r == 0x868 }

// IsJoiningGroupMalayalamLlla reports whether the rune has property "Joining_Group"="Malayalam_Llla".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamLlla(r rune) bool { return r == 0x869 }

// IsJoiningGroupMalayalamNga reports whether the rune has property "Joining_Group"="Malayalam_Nga".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamNga(r rune) bool { return r == 0x860 }

// IsJoiningGroupMalayalamNna reports whether the rune has property "Joining_Group"="Malayalam_Nna".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamNna(r rune) bool { return r == 0x864 }

// IsJoiningGroupMalayalamNnna reports whether the rune has property "Joining_Group"="Malayalam_Nnna".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamNnna(r rune) bool { return r == 0x865 }

// IsJoiningGroupMalayalamNya reports whether the rune has property "Joining_Group"="Malayalam_Nya".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamNya(r rune) bool { return r == 0x862 }

// IsJoiningGroupMalayalamRa reports whether the rune has property "Joining_Group"="Malayalam_Ra".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamRa(r rune) bool { return r == 0x867 }

// IsJoiningGroupMalayalamSsa reports whether the rune has property "Joining_Group"="Malayalam_Ssa".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamSsa(r rune) bool { return r == 0x86a }

// IsJoiningGroupMalayalamTta reports whether the rune has property "Joining_Group"="Malayalam_Tta".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMalayalamTta(r rune) bool { return r == 0x863 }

// IsJoiningGroupManichaeanAleph reports whether the rune has property "Joining_Group"="Manichaean_Aleph".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanAleph(r rune) bool { return r == 0x10ac0 }

// IsJoiningGroupManichaeanAyin reports whether the rune has property "Joining_Group"="Manichaean_Ayin".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanAyin(r rune) bool { return r == 0x10ad9 || r == 0x10ada }

// IsJoiningGroupManichaeanBeth reports whether the rune has property "Joining_Group"="Manichaean_Beth".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanBeth(r rune) bool { return r == 0x10ac1 || r == 0x10ac2 }

// IsJoiningGroupManichaeanDaleth reports whether the rune has property "Joining_Group"="Manichaean_Daleth".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanDaleth(r rune) bool { return r == 0x10ac5 }

// IsJoiningGroupManichaeanDhamedh reports whether the rune has property "Joining_Group"="Manichaean_Dhamedh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanDhamedh(r rune) bool { return r == 0x10ad4 }

// IsJoiningGroupManichaeanFive reports whether the rune has property "Joining_Group"="Manichaean_Five".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanFive(r rune) bool { return r == 0x10aec }

// IsJoiningGroupManichaeanGimel reports whether the rune has property "Joining_Group"="Manichaean_Gimel".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanGimel(r rune) bool { return r == 0x10ac3 || r == 0x10ac4 }

// IsJoiningGroupManichaeanHeth reports whether the rune has property "Joining_Group"="Manichaean_Heth".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanHeth(r rune) bool { return r == 0x10acd }

// IsJoiningGroupManichaeanHundred reports whether the rune has property "Joining_Group"="Manichaean_Hundred".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanHundred(r rune) bool { return r == 0x10aef }

// IsJoiningGroupManichaeanKaph reports whether the rune has property "Joining_Group"="Manichaean_Kaph".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanKaph(r rune) bool { return (r >= 0x10ad0 && r <= 0x10ad2) }

// IsJoiningGroupManichaeanLamedh reports whether the rune has property "Joining_Group"="Manichaean_Lamedh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanLamedh(r rune) bool { return r == 0x10ad3 }

// IsJoiningGroupManichaeanMem reports whether the rune has property "Joining_Group"="Manichaean_Mem".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanMem(r rune) bool { return r == 0x10ad6 }

// IsJoiningGroupManichaeanNun reports whether the rune has property "Joining_Group"="Manichaean_Nun".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanNun(r rune) bool { return r == 0x10ad7 }

// IsJoiningGroupManichaeanOne reports whether the rune has property "Joining_Group"="Manichaean_One".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanOne(r rune) bool { return r == 0x10aeb }

// IsJoiningGroupManichaeanPe reports whether the rune has property "Joining_Group"="Manichaean_Pe".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanPe(r rune) bool { return r == 0x10adb || r == 0x10adc }

// IsJoiningGroupManichaeanQoph reports whether the rune has property "Joining_Group"="Manichaean_Qoph".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanQoph(r rune) bool { return (r >= 0x10ade && r <= 0x10ae0) }

// IsJoiningGroupManichaeanResh reports whether the rune has property "Joining_Group"="Manichaean_Resh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanResh(r rune) bool { return r == 0x10ae1 }

// IsJoiningGroupManichaeanSadhe reports whether the rune has property "Joining_Group"="Manichaean_Sadhe".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanSadhe(r rune) bool { return r == 0x10add }

// IsJoiningGroupManichaeanSamekh reports whether the rune has property "Joining_Group"="Manichaean_Samekh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanSamekh(r rune) bool { return r == 0x10ad8 }

// IsJoiningGroupManichaeanTaw reports whether the rune has property "Joining_Group"="Manichaean_Taw".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanTaw(r rune) bool { return r == 0x10ae4 }

// IsJoiningGroupManichaeanTen reports whether the rune has property "Joining_Group"="Manichaean_Ten".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanTen(r rune) bool { return r == 0x10aed }

// IsJoiningGroupManichaeanTeth reports whether the rune has property "Joining_Group"="Manichaean_Teth".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanTeth(r rune) bool { return r == 0x10ace }

// IsJoiningGroupManichaeanThamedh reports whether the rune has property "Joining_Group"="Manichaean_Thamedh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanThamedh(r rune) bool { return r == 0x10ad5 }

// IsJoiningGroupManichaeanTwenty reports whether the rune has property "Joining_Group"="Manichaean_Twenty".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanTwenty(r rune) bool { return r == 0x10aee }

// IsJoiningGroupManichaeanWaw reports whether the rune has property "Joining_Group"="Manichaean_Waw".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanWaw(r rune) bool { return r == 0x10ac7 }

// IsJoiningGroupManichaeanYodh reports whether the rune has property "Joining_Group"="Manichaean_Yodh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanYodh(r rune) bool { return r == 0x10acf }

// IsJoiningGroupManichaeanZayin reports whether the rune has property "Joining_Group"="Manichaean_Zayin".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupManichaeanZayin(r rune) bool { return r == 0x10ac9 || r == 0x10aca }

// IsJoiningGroupMeem reports whether the rune has property "Joining_Group"="Meem".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMeem(r rune) bool { return r == 0x645 || r == 0x765 || r == 0x766 || r == 0x8a7 }

// IsJoiningGroupMim reports whether the rune has property "Joining_Group"="Mim".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupMim(r rune) bool { return r == 0x721 }

// IsJoiningGroupNoJoiningGroup reports whether the rune has property "Joining_Group"="No_Joining_Group".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupNoJoiningGroup(r rune) bool {
	const lo = 0x620
	const hi = 0x10aef
	if r < lo || r > hi {
		return true
	}
	var i = (r - lo) / 64
	var shift = (r - lo) % 64
	return dataArrayIsJoiningGroupNoJoiningGroup[i]&(1<<uint(shift)) != 0
}

// IsJoiningGroupNoon reports whether the rune has property "Joining_Group"="Noon".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupNoon(r rune) bool {
	return r == 0x646 || r == 0x6b9 || (r >= 0x6ba && r <= 0x6bc) || (r >= 0x767 && r <= 0x769)
}

// IsJoiningGroupNun reports whether the rune has property "Joining_Group"="Nun".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupNun(r rune) bool { return r == 0x722 }

// IsJoiningGroupNya reports whether the rune has property "Joining_Group"="Nya".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupNya(r rune) bool { return r == 0x6bd }

// IsJoiningGroupPe reports whether the rune has property "Joining_Group"="Pe".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupPe(r rune) bool { return r == 0x726 }

// IsJoiningGroupQaf reports whether the rune has property "Joining_Group"="Qaf".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupQaf(r rune) bool {
	return r == 0x642 || r == 0x66f || r == 0x6a7 || r == 0x6a8 || r == 0x8a5
}

// IsJoiningGroupQaph reports whether the rune has property "Joining_Group"="Qaph".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupQaph(r rune) bool { return r == 0x729 }

// IsJoiningGroupReh reports whether the rune has property "Joining_Group"="Reh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupReh(r rune) bool { return unicode.Is(JoiningGroupReh, r) }

// IsJoiningGroupReversedPe reports whether the rune has property "Joining_Group"="Reversed_Pe".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupReversedPe(r rune) bool { return r == 0x727 }

// IsJoiningGroupRohingyaYeh reports whether the rune has property "Joining_Group"="Rohingya_Yeh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupRohingyaYeh(r rune) bool { return r == 0x8ac }

// IsJoiningGroupSad reports whether the rune has property "Joining_Group"="Sad".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupSad(r rune) bool {
	return r == 0x635 || r == 0x636 || r == 0x69d || r == 0x69e || r == 0x6fb || r == 0x8af
}

// IsJoiningGroupSadhe reports whether the rune has property "Joining_Group"="Sadhe".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupSadhe(r rune) bool { return r == 0x728 }

// IsJoiningGroupSeen reports whether the rune has property "Joining_Group"="Seen".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupSeen(r rune) bool { return unicode.Is(JoiningGroupSeen, r) }

// IsJoiningGroupSemkath reports whether the rune has property "Joining_Group"="Semkath".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupSemkath(r rune) bool { return r == 0x723 }

// IsJoiningGroupShin reports whether the rune has property "Joining_Group"="Shin".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupShin(r rune) bool { return r == 0x72b }

// IsJoiningGroupStraightWaw reports whether the rune has property "Joining_Group"="Straight_Waw".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupStraightWaw(r rune) bool { return r == 0x8b1 }

// IsJoiningGroupSwashKaf reports whether the rune has property "Joining_Group"="Swash_Kaf".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupSwashKaf(r rune) bool { return r == 0x6aa }

// IsJoiningGroupSyriacWaw reports whether the rune has property "Joining_Group"="Syriac_Waw".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupSyriacWaw(r rune) bool { return r == 0x718 }

// IsJoiningGroupTah reports whether the rune has property "Joining_Group"="Tah".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupTah(r rune) bool { return r == 0x637 || r == 0x638 || r == 0x69f || r == 0x8a3 }

// IsJoiningGroupTaw reports whether the rune has property "Joining_Group"="Taw".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupTaw(r rune) bool { return r == 0x72c }

// IsJoiningGroupTehMarbuta reports whether the rune has property "Joining_Group"="Teh_Marbuta".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupTehMarbuta(r rune) bool { return r == 0x629 || r == 0x6c0 || r == 0x6d5 }

// IsJoiningGroupHamzaOnHehGoal reports whether the rune has property "Joining_Group"="Hamza_On_Heh_Goal".
// Property "Joining_Group" known as "jg", "Joining_Group".
// Value "Hamza_On_Heh_Goal" known as "Teh_Marbuta_Goal", "Hamza_On_Heh_Goal".
func IsJoiningGroupHamzaOnHehGoal(r rune) bool { return r == 0x6c3 }

// IsJoiningGroupTeth reports whether the rune has property "Joining_Group"="Teth".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupTeth(r rune) bool { return r == 0x71b || r == 0x71c }

// IsJoiningGroupWaw reports whether the rune has property "Joining_Group"="Waw".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupWaw(r rune) bool { return unicode.Is(JoiningGroupWaw, r) }

// IsJoiningGroupYeh reports whether the rune has property "Joining_Group"="Yeh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupYeh(r rune) bool { return unicode.Is(JoiningGroupYeh, r) }

// IsJoiningGroupYehBarree reports whether the rune has property "Joining_Group"="Yeh_Barree".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupYehBarree(r rune) bool { return r == 0x6d2 || r == 0x6d3 }

// IsJoiningGroupYehWithTail reports whether the rune has property "Joining_Group"="Yeh_With_Tail".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupYehWithTail(r rune) bool { return r == 0x6cd }

// IsJoiningGroupYudh reports whether the rune has property "Joining_Group"="Yudh".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupYudh(r rune) bool { return r == 0x71d }

// IsJoiningGroupYudhHe reports whether the rune has property "Joining_Group"="Yudh_He".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupYudhHe(r rune) bool { return r == 0x71e }

// IsJoiningGroupZain reports whether the rune has property "Joining_Group"="Zain".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupZain(r rune) bool { return r == 0x719 }

// IsJoiningGroupZhain reports whether the rune has property "Joining_Group"="Zhain".
// Property "Joining_Group" known as "jg", "Joining_Group".
func IsJoiningGroupZhain(r rune) bool { return r == 0x74d }

var dataArrayIsJoiningGroupNoJoiningGroup = [...]uint64{0xfffff80100000002, 0x113fff, 0xffd0000000000000, 0x2ffff63ff3fff, 0x1fffffff0000, 0xffffffff00000000, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xfffffffffffff800, 0xffffffffc0202000, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0xffffffffffffffff, 0x1940ffffffff, 0x7ec}
