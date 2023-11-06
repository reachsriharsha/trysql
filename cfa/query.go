package cfa

import "strings"

type GenQuery struct {
	InputTable    string `json:"InputTable,omitempty"`
	DisplayFields string `json:"DisplayFields,omitempty"`
	Condition     string `json: "Condition,omitempty"`
	ChildLink     string `json: "ChildLink,omitempty"`
	SqlQueryStr   string `json: "SQLQuery,omitempty"`
	Sequence      int    `json: "Sequence, omitempty"`
}

type ChainQuery struct {
	ParentQuery GenQuery   `json:"ParentQuery,omitempty"`
	ChildQuery  []GenQuery `json: ChildQuery,omitempty"`
}

func (cq *ChainQuery) Reorder() {

	if cq != nil {
		ncq := len(cq.ChildQuery)

		//newCQ := [ncq]GenQuery
		newCQ := make([]GenQuery, ncq)

		for i, gq := range cq.ChildQuery {
			if i < ncq {
				newCQ[cq.ChildQuery[i].Sequence] = gq
			}
		}

		cq.ChildQuery = newCQ
	}

}

func (cq *ChainQuery) GetDisplayFileds() []string {
	var df []string
	if cq != nil && len(cq.ParentQuery.DisplayFields) > 0 {
		df = strings.Split(cq.ParentQuery.DisplayFields, ",")
		for _, nq := range cq.ChildQuery {
			df = append(df, nq.GetDisplayFields()...)
		}
	}
	return df
}
func (gq *GenQuery) GetDisplayFields() []string {
	var df []string
	if gq != nil && len(gq.DisplayFields) > 0 {

		df = strings.Split(gq.DisplayFields, ",")

	}
	return df
}
