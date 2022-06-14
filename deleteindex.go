package esutils

import (
	"context"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// DeleteIndex removes the index
func (es *Es) DeleteIndex(ctx context.Context) error {
	var (
		err error
		res *elastic.IndicesDeleteResponse
	)
	if res, err = es.client.DeleteIndex(es.esIndex).Do(ctx); err != nil {
		if elastic.IsNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "call DeleteIndex() error")
	}
	if !res.Acknowledged {
		return errors.New("failed to delete index" + es.esIndex)
	}
	return nil
}
