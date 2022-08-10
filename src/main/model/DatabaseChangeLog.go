package model

import "encoding/xml"

type DatabaseChangeLog struct {
	XMLName  xml.Name  `xml:"databaseChangeLog"`
	Includes []Include `xml:"include"`
}

type Include struct {
	File string `xml:"file,attr"`
}
