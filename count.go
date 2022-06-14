package esutils

import (
	"context"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/unionj-cloud/go-doudou/toolkit/stringutils"
	"time"
)

// Count counts docs by paging
func (es *Es) Count(ctx context.Context, paging *Paging) (int64, error) {
	var (
		err       error
		boolQuery *elastic.BoolQuery
	)
	if paging == nil {
		paging = &Paging{}
	}
	var zone *time.Location
	if stringutils.IsNotEmpty(paging.Zone) {
		zone, err = time.LoadLocation(paging.Zone)
		if err != nil {
			return 0, errors.Wrap(err, "call LoadLocation() error")
		}
	}
	boolQuery = query(paging.StartDate, paging.EndDate, paging.DateField, paging.QueryConds, zone)
	es.client.Refresh().Index(es.esIndex).Do(ctx)
	es.client.Flush().Index(es.esIndex).Do(ctx)
	var total int64
	if total, err = es.client.Count().Index(es.esIndex).Type(es.esType).Query(boolQuery).Do(ctx); err != nil {
		return 0, errors.Wrap(err, "call Count() error")
	}
	return total, nil
}
