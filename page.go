package esutils

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/unionj-cloud/go-doudou/toolkit/stringutils"
	"time"
)

// PageResult represents result of pagination
type PageResult struct {
	Page        int           `json:"page"` // from 1
	PageSize    int           `json:"page_size"`
	Total       int           `json:"total"`
	Docs        []interface{} `json:"docs"`
	HasNextPage bool          `json:"has_next_page"`
}

// Page fetch pagination result
func (es *Es) Page(ctx context.Context, paging *Paging) (PageResult, error) {
	var (
		err       error
		boolQuery *elastic.BoolQuery
		pr        PageResult
	)
	if paging == nil {
		paging = &Paging{
			Limit: -1,
		}
	}
	if paging.Limit < 0 || paging.Limit > 10000 {
		docs, err := es.List(ctx, paging, nil)
		if err != nil {
			return pr, errors.Wrap(err, "call List() error")
		}
		pr.Total = len(docs)
		pr.Docs = docs
		return pr, nil
	}
	var zone *time.Location
	if stringutils.IsNotEmpty(paging.Zone) {
		zone, err = time.LoadLocation(paging.Zone)
		if err != nil {
			return PageResult{}, errors.Wrap(err, "call LoadLocation() error")
		}
	}
	boolQuery = query(paging.StartDate, paging.EndDate, paging.DateField, paging.QueryConds, zone)
	var rets []interface{}

	var searchResult *elastic.SearchResult
	fsc := elastic.NewFetchSourceContext(true)
	if len(paging.Includes) > 0 {
		fsc = fsc.Include(paging.Includes...)
	}
	if len(paging.Excludes) > 0 {
		fsc = fsc.Exclude(paging.Excludes...)
	}
	ss := es.client.Search().Index(es.esIndex).Type(es.esType).Query(boolQuery).FetchSourceContext(fsc)
	if paging.Sortby != nil && len(paging.Sortby) > 0 {
		for _, v := range paging.Sortby {
			ss = ss.Sort(v.Field, v.Ascending)
		}
	}
	if searchResult, err = ss.From(paging.Skip).Size(paging.Limit).Do(ctx); err != nil {
		return pr, errors.Wrap(err, "call Search() error")
	}
	for _, hit := range searchResult.Hits.Hits {
		var p map[string]interface{}
		json.Unmarshal(*hit.Source, &p)
		p["_id"] = hit.Id
		rets = append(rets, p)
	}

	pr.Docs = rets
	pr.Total = int(searchResult.TotalHits())
	pr.PageSize = paging.Limit
	if paging.Limit > 0 {
		pr.Page = paging.Skip/paging.Limit + 1
	}

	var totalPage int
	if pr.PageSize > 0 {
		if pr.Total%pr.PageSize > 0 {
			totalPage = pr.Total/pr.PageSize + 1
		} else {
			totalPage = pr.Total / pr.PageSize
		}
	}

	if pr.Page < totalPage {
		pr.HasNextPage = true
	}
	return pr, err
}
