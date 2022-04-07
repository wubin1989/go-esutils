package esutils

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/unionj-cloud/go-doudou/toolkit/stringutils"
	"time"
)

// Random if paging is nil, randomly return 10 pcs of documents as default
func (es *Es) Random(ctx context.Context, paging *Paging) ([]map[string]interface{}, error) {
	var (
		err       error
		boolQuery *elastic.BoolQuery
		sr        *elastic.SearchResult
		rets      []map[string]interface{}
	)
	if paging == nil {
		paging = &Paging{
			Limit: 10,
		}
	}
	var zone *time.Location
	if stringutils.IsNotEmpty(paging.Zone) {
		zone, err = time.LoadLocation(paging.Zone)
		if err != nil {
			return nil, errors.Wrap(err, "call LoadLocation() error")
		}
	}
	boolQuery = query(paging.StartDate, paging.EndDate, paging.DateField, paging.QueryConds, zone)
	fsq := elastic.NewFunctionScoreQuery().Query(boolQuery).AddScoreFunc(elastic.NewScriptFunction(elastic.NewScriptInline("Math.random()")))
	if sr, err = es.client.Search().Index(es.esIndex).Type(es.esType).Query(fsq).From(paging.Skip).Size(paging.Limit).Do(ctx); err != nil {
		return nil, errors.Wrap(err, "call Search() error")
	}
	for _, hit := range sr.Hits.Hits {
		var ret map[string]interface{}
		json.Unmarshal(*hit.Source, &ret)
		rets = append(rets, ret)
	}
	return rets, nil
}
