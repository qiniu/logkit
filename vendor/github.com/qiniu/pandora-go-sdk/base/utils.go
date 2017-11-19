package base

func FormExportName(repoName, exportType string) string {
	return repoName + "_export2_" + exportType
}

func FormExportTSDBName(repoName, seriesName, exportType string) string {
	return repoName + "export2" + exportType + "_" + seriesName
}
