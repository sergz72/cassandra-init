package main

import (
	"database-init/src/main/model"
	"encoding/xml"
	"os"
	"path"
	"sort"
	"strings"
)

func buildFileList(initScriptsFolder string, result []string) ([]string, error) {
	files, err := os.ReadDir(initScriptsFolder)
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".xml") {
			return buildFileListFromXML(initScriptsFolder, file.Name(), result)
		}
	}

	for _, file := range files {
		pathName := path.Join(initScriptsFolder, file.Name())
		if file.IsDir() {
			result, err = buildFileList(pathName, result)
			if err != nil {
				return nil, err
			}
		} else {
			result = append(result, pathName)
		}
	}

	return result, nil
}

func buildFileListFromXML(folderName, fileName string, result []string) ([]string, error) {
	data, err := os.ReadFile(path.Join(folderName, fileName))
	if err != nil {
		return nil, err
	}
	var changeLog model.DatabaseChangeLog
	err = xml.Unmarshal(data, &changeLog)
	if err != nil {
		return nil, err
	}
	for _, include := range changeLog.Includes {
		result = append(result, path.Join(folderName, include.File))
	}

	return result, nil
}
