package esutils

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/unionj-cloud/go-doudou/toolkit/stringutils"
	"time"
)

// List fetch docs by paging
func (es *Es) List(ctx context.Context, paging *Paging, callback func(message json.RawMessage) (interface{}, error)) ([]interface{}, error) {
	var (
		err       error
		boolQuery *elastic.BoolQuery
	)
	if paging == nil {
		paging = &Paging{
			Limit:      -1,
			ScrollSize: 1000,
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
	fsc := elastic.NewFetchSourceContext(true)
	if len(paging.Includes) > 0 {
		fsc = fsc.Include(paging.Includes...)
	}
	if len(paging.Excludes) > 0 {
		fsc = fsc.Exclude(paging.Excludes...)
	}
	var rets []interface{}
	if paging.Limit < 0 || paging.Limit > 10000 {
		scrollSize := paging.ScrollSize
		if scrollSize <= 0 {
			scrollSize = 1000
		}
		if rets, err = es.fetchAll(fsc, boolQuery, scrollSize, callback); err != nil {
			return nil, errors.Wrap(err, "call es.fetchAll error")
		}
	} else {
		if rets, err = es.doPaging(ctx, fsc, paging, boolQuery, callback); err != nil {
			return nil, errors.Wrap(err, "call es.fetchAll error")
		}
	}
	return rets, nil
}
