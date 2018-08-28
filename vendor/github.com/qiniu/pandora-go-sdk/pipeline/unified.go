package pipeline

import (
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/logdb"
)

func (c *Pipeline) FormExportInput(repoName, exportType string, spec interface{}) *CreateExportInput {
	exportName := base.FormExportName(repoName, exportType)
	return &CreateExportInput{
		RepoName:   repoName,
		ExportName: exportName,
		Type:       exportType,
		Spec:       spec,
		Whence:     "oldest",
	}
}

func (c *Pipeline) FormLogDBSpec(input *CreateRepoForLogDBInput) *ExportLogDBSpec {
	doc := make(map[string]interface{})
	for _, v := range input.Schema {
		doc[v.Key] = "#" + v.Key
	}
	return &ExportLogDBSpec{
		DestRepoName: input.LogRepoName,
		Doc:          doc,
		OmitInvalid:  input.OmitInvalid,
		OmitEmpty:    input.OmitEmpty,
	}
}

func (c *Pipeline) FormKodoSpec(input *CreateRepoForKodoInput) *ExportKodoSpec {
	doc := make(map[string]string)
	for _, v := range input.Schema {
		doc[v.Key] = "#" + v.Key
	}
	if input.Prefix == "" {
		input.Prefix = "logkitauto/date=$(year)-$(mon)-$(day)/hour=$(hour)/min=$(min)/$(sec)"
	}
	if input.Format == "" {
		input.Format = "parquet"
	}
	return &ExportKodoSpec{
		Bucket:         input.Bucket,
		KeyPrefix:      input.Prefix,
		Fields:         doc,
		Email:          input.Email,
		AccessKey:      input.Ak,
		Format:         input.Format,
		Retention:      input.Retention,
		Compress:       input.Compress,
		RotateStrategy: input.RotateStrategy,
		RotateInterval: input.RotateInterval,
		RotateSize:     input.RotateSize,
		RotateSizeType: "KB",
		RotateNumber:   input.RotateSize / 1024,
		KodoFileType:   input.KodoFileType,
	}
}

func (c *Pipeline) FormTSDBSpec(input *CreateRepoForTSDBInput) *ExportTsdbSpec {
	tags := make(map[string]string)
	fields := make(map[string]string)
	for _, v := range input.Schema {
		if IsTag(v.Key, input.Tags) {
			tags[v.Key] = "#" + v.Key
		} else {
			fields[v.Key] = "#" + v.Key
		}
	}
	timestamp := input.Timestamp
	if timestamp != "" {
		timestamp = "#" + timestamp
	}
	return &ExportTsdbSpec{
		Tags:         tags,
		Fields:       fields,
		Timestamp:    timestamp,
		DestRepoName: input.TSDBRepoName,
		SeriesName:   input.SeriesName,
		OmitEmpty:    input.OmitEmpty,
		OmitInvalid:  input.OmitInvalid,
	}
}

func (c *Pipeline) FormMutiSeriesTSDBSpec(input *CreateRepoForTSDBInput) *ExportTsdbSpec {
	tags := make(map[string]string)
	fields := make(map[string]string)
	for _, v := range input.Schema {
		var key string
		// 重命名 cpu__time_user --> cpu_time_user
		// 当字段名称有 __ 分割的前缀，且该前缀为 seriesName 时，导出时将字段去掉一个下划线
		if strings.HasPrefix(v.Key, input.SeriesName+"__") {
			key = strings.Replace(v.Key, "__", "_", 1)
		} else {
			key = v.Key
		}
		if IsTag(key, input.Tags) {
			tags[key] = "#" + v.Key
		} else {
			fields[key] = "#" + v.Key
		}
	}
	timestamp := input.Timestamp
	if timestamp != "" {
		timestamp = "#" + timestamp
	}
	return &ExportTsdbSpec{
		Tags:         tags,
		Fields:       fields,
		Timestamp:    timestamp,
		DestRepoName: input.TSDBRepoName,
		SeriesName:   input.SeriesName,
		OmitEmpty:    input.OmitEmpty,
		OmitInvalid:  input.OmitInvalid,
	}
}

func formPipelineRepoInput(repoName, region string, schemas []RepoSchemaEntry) *CreateRepoInput {
	return &CreateRepoInput{
		Region:   region,
		RepoName: repoName,
		Schema:   schemas,
	}
}

func convertCreate2LogDB(input *CreateRepoForLogDBInput) *logdb.CreateRepoInput {
	if input.LogRepoName == "" {
		input.LogRepoName = input.RepoName
	}
	linput := &logdb.CreateRepoInput{
		Region:    input.Region,
		RepoName:  input.LogRepoName,
		Schema:    convertSchema2LogDB(input.Schema, input.AnalyzerInfo, nil),
		Retention: input.Retention,
	}
	if input.AnalyzerInfo.FullText {
		linput.FullText = logdb.NewFullText(logdb.StandardAnalyzer)
	}
	return linput
}

func convertSchema2LogDB(scs []RepoSchemaEntry, analyzer AnalyzerInfo, prefix []string) (ret []logdb.RepoSchemaEntry) {
	ret = make([]logdb.RepoSchemaEntry, 0)
	for _, v := range scs {
		rp := logdb.RepoSchemaEntry{
			Key:       v.Key,
			ValueType: v.ValueType,
		}
		if v.ValueType == PandoraTypeMap {
			rp.Schemas = convertSchema2LogDB(v.Schema, analyzer, append(prefix, v.Key))
			rp.ValueType = logdb.TypeObject
		}
		if v.ValueType == PandoraTypeJsonString {
			rp.ValueType = logdb.TypeObject
		}
		if v.ValueType == PandoraTypeArray {
			rp.ValueType = v.ElemType
		}
		if v.ValueType == PandoraTypeString && !analyzer.FullText {
			// 当 analyzer.Analyzer 这个 map 中有明确的字段分词类型时，按照 map 中的分词类型设置
			// 否则当 analyzer.Default 不为空时，按照 default 值设置分词类型
			// 上述两个条件都不符合时，按照标准分词设置
			exist := false
			var ana string
			if analyzer.Analyzer != nil {
				ana, exist = analyzer.Analyzer[strings.Join(append(prefix, v.Key), ".")]
			}
			if exist && logdb.Analyzers[ana] {
				rp.Analyzer = ana
			} else if logdb.Analyzers[analyzer.Default] {
				rp.Analyzer = analyzer.Default
			} else {
				rp.Analyzer = logdb.StandardAnalyzer
			}
		}
		ret = append(ret, rp)
	}
	return ret
}

func getSeriesName(seriesTag map[string][]string, schemaKey string) string {
	tmpSeries := strings.Split(schemaKey, "__")
	if len(tmpSeries) < 2 {
		return ""
	}
	seriesName := tmpSeries[0]
	if _, ok := seriesTag[seriesName]; ok {
		return seriesName
	}
	return ""
}

func isInExpandAttr(key string, expandAttr []string) bool {
	for _, val := range expandAttr {
		if key == val {
			return true
		}
	}
	return false
}

func (c *Pipeline) AutoExportToTSDB(input *AutoExportToTSDBInput) error {
	if input.TSDBRepoName == "" {
		input.TSDBRepoName = input.RepoName
	}
	if input.SeriesName == "" {
		input.SeriesName = input.RepoName
	}
	if input.Retention == "" {
		input.Retention = "30d"
	}
	repoInfo, err := c.GetRepo(&GetRepoInput{
		RepoName:     input.RepoName,
		PandoraToken: input.PipelineGetRepoToken,
	})
	if err != nil {
		log.Error("AutoExportToTSDB get repo from pipeline error", err)
		return err
	}
	tags := make([]string, 0)
	if val, ok := input.SeriesTags[input.SeriesName]; ok {
		tags = val
	}

	if !input.IsMetric {
		return c.CreateForTSDB(&CreateRepoForTSDBInput{
			Tags:                 tags,
			RepoName:             input.RepoName,
			TSDBRepoName:         input.TSDBRepoName,
			Region:               repoInfo.Region,
			Schema:               repoInfo.Schema,
			Retention:            input.Retention,
			SeriesName:           input.SeriesName,
			OmitInvalid:          input.OmitInvalid,
			OmitEmpty:            input.OmitEmpty,
			Timestamp:            input.Timestamp,
			AutoExportTSDBTokens: input.AutoExportTSDBTokens,
		})
	}

	// 获取字段，并根据 seriesTag 中的 key 拿到series name
	seriesMap := make(map[string]SeriesInfo)
	expandAttr := make([]RepoSchemaEntry, 0)
	for _, val := range repoInfo.Schema {
		seriesName := getSeriesName(input.SeriesTags, val.Key)
		if seriesName == "" {
			if isInExpandAttr(val.Key, input.ExpandAttr) {
				expandAttr = append(expandAttr, val)
			}
			continue
		}
		series, exist := seriesMap[seriesName]
		if !exist {
			series = SeriesInfo{
				SeriesName: seriesName,
				TimeStamp:  input.Timestamp,
				Schema:     make([]RepoSchemaEntry, 0),
				Tags:       input.SeriesTags[seriesName],
			}
		}
		series.Schema = append(series.Schema, val)
		seriesMap[seriesName] = series
	}

	// 将调用方传递过来的 expand attr 也加入到每个 series 中
	for k, val := range seriesMap {
		val.Schema = append(val.Schema, expandAttr...)
		seriesMap[k] = val
	}

	err = c.CreateForMutiExportTSDB(&CreateRepoForMutiExportTSDBInput{
		RepoName:             input.RepoName,
		TSDBRepoName:         input.TSDBRepoName,
		Region:               repoInfo.Region,
		Retention:            input.Retention,
		OmitInvalid:          input.OmitInvalid,
		OmitEmpty:            input.OmitEmpty,
		SeriesMap:            seriesMap,
		AutoExportTSDBTokens: input.AutoExportTSDBTokens,
	})
	return err
}

func (c *Pipeline) AutoExportToLogDB(input *AutoExportToLogDBInput) error {
	if input.LogRepoName == "" {
		input.LogRepoName = input.RepoName
	}
	input.LogRepoName = strings.ToLower(input.LogRepoName)
	if input.Retention == "" {
		input.Retention = "30d"
	}
	repoInfo, err := c.GetRepo(&GetRepoInput{
		RepoName:     input.RepoName,
		PandoraToken: input.PipelineGetRepoToken,
	})
	if err != nil {
		log.Error("AutoExportToLogDB get pipeline repo error", err)
		return err
	}

	logdbapi, err := c.GetLogDBAPI()
	if err != nil {
		return err
	}
	logdbschemas := convertSchema2LogDB(repoInfo.Schema, input.AnalyzerInfo, nil)
	logdbrepoinfo, err := logdbapi.GetRepo(&logdb.GetRepoInput{
		RepoName:     input.LogRepoName,
		PandoraToken: input.GetLogDBRepoToken,
	})
	if reqerr.IsNoSuchResourceError(err) {
		linput := &logdb.CreateRepoInput{
			RepoName:     input.LogRepoName,
			Region:       repoInfo.Region,
			Retention:    input.Retention,
			Schema:       logdbschemas,
			PandoraToken: input.CreateLogDBRepoToken,
			Description:  input.Description,
		}
		if input.AnalyzerInfo.FullText {
			linput.FullText = logdb.NewFullText(logdb.StandardAnalyzer)
		}
		err = logdbapi.CreateRepo(linput)
		if err != nil && !reqerr.IsExistError(err) {
			log.Error("AutoExportToLogDB create logdb repo error", err)
			return err
		}
	} else if err != nil {
		log.Error("AutoExportToLogDB get logdb repo error", err)
		return err
	} else {
		//repo 存在，检查是否需要更新
		needupdate := false
		for _, v := range logdbschemas {
			//暂不考虑嵌套类型里面的不同
			if schemaNotIn(v.Key, logdbrepoinfo.Schema) {
				logdbrepoinfo.Schema = append(logdbrepoinfo.Schema, v)
				needupdate = true
			}
		}
		if needupdate {
			if err = logdbapi.UpdateRepo(&logdb.UpdateRepoInput{
				RepoName:     input.LogRepoName,
				Retention:    logdbrepoinfo.Retention,
				Schema:       logdbrepoinfo.Schema,
				PandoraToken: input.UpdateLogDBRepoToken,
			}); err != nil {
				log.Error("AutoExportToLogDB update logdb repo error", err)
				return err
			}
		}
	}

	_, err = c.GetExport(&GetExportInput{
		RepoName:     input.RepoName,
		ExportName:   base.FormExportName(input.RepoName, ExportTypeLogDB),
		PandoraToken: input.GetExportToken,
	})
	if reqerr.IsNoSuchResourceError(err) {
		logDBSpec := c.FormLogDBSpec(&CreateRepoForLogDBInput{
			RepoName:    input.RepoName,
			LogRepoName: input.LogRepoName,
			Schema:      repoInfo.Schema,
			OmitEmpty:   input.OmitEmpty,
			OmitInvalid: input.OmitInvalid,
		})
		exportInput := c.FormExportInput(input.RepoName, ExportTypeLogDB, logDBSpec)
		exportInput.PandoraToken = input.CreateExportToken
		if err = c.CreateExport(exportInput); err != nil && reqerr.IsExistError(err) {
			err = nil
		} else if err != nil {
			log.Error("AutoExportToLogDB get export error", err)
		}
	}
	if err != nil {
		log.Error("AutoExportToLogDB get export error", err)
	}
	return err
}

//自动导出到KODO需要提前创建bucket，不会自动创建
func (c *Pipeline) AutoExportToKODO(input *AutoExportToKODOInput) error {
	if input.BucketName == "" {
		input.BucketName = input.RepoName
	}
	if input.Retention <= 0 {
		input.Retention = 0
	}
	input.BucketName = strings.Replace(input.BucketName, "_", "-", -1)

	repoInfo, err := c.GetRepo(&GetRepoInput{
		RepoName:     input.RepoName,
		PandoraToken: input.PipelineGetRepoToken,
	})
	if err != nil {
		log.Error("AutoExportToKodo GetRepo from pipeline error", err)
		return err
	}

	_, err = c.GetExport(&GetExportInput{
		RepoName:     input.RepoName,
		ExportName:   base.FormExportName(input.RepoName, ExportTypeKODO),
		PandoraToken: input.GetExportToken,
	})
	if reqerr.IsNoSuchResourceError(err) {
		var ak string
		if c.Config.Ak != "" {
			ak = c.Config.Ak
		}
		if ak == "" && input.CreateExportToken.Token != "" {
			tks := strings.Split(strings.TrimSpace(strings.TrimPrefix(input.CreateExportToken.Token, "Pandora")), ":")
			if len(tks) > 0 {
				ak = tks[0]
			}
		}
		kodoSpec := c.FormKodoSpec(&CreateRepoForKodoInput{
			Retention:      input.Retention,
			Ak:             ak,
			Email:          input.Email,
			Bucket:         input.BucketName,
			RepoName:       input.RepoName,
			Schema:         repoInfo.Schema,
			Prefix:         input.Prefix,
			Format:         input.Format,
			Compress:       input.Compress,
			RotateStrategy: input.RotateStrategy,
			RotateInterval: input.RotateInterval,
			RotateSize:     input.RotateSize,
			KodoFileType:   input.KodoFileType,
		})
		exportInput := c.FormExportInput(input.RepoName, ExportTypeKODO, kodoSpec)
		exportInput.PandoraToken = input.CreateExportToken
		if err = c.CreateExport(exportInput); err != nil && reqerr.IsExistError(err) {
			err = nil
		} else if err != nil {
			log.Error("AutoExportToKodo create export error", err)
			return err
		}
	}
	if err != nil {
		log.Error("AutoExportToKodo get export error", err)
	}
	return err
}
