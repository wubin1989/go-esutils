package esutils

import (
	"context"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
)

// NewIndex creates a new index
func (es *Es) NewIndex(ctx context.Context, mapping string) (exists bool, err error) {
	var (
		res *elastic.IndicesCreateResult
	)
	if exists, err = es.client.IndexExists(es.esIndex).Do(ctx); err != nil {
		return false, errors.Wrap(err, "call IndexExists() error")
	}
	if !exists {
		// Create a new index.
		if res, err = es.client.CreateIndex(es.esIndex).BodyString(mapping).Do(ctx); err != nil {
			return false, errors.Wrap(err, "call CreateIndex() error")
		}
		if !res.Acknowledged {
			return false, errors.Wrap(err, "call CreateIndex() failed")
		}
	}
	return
}

// NewIndexOnly creates a new index without settings and mappings
func (es *Es) NewIndexOnly(ctx context.Context) (exists bool, err error) {
	var (
		res *elastic.IndicesCreateResult
	)
	if exists, err = es.client.IndexExists(es.esIndex).Do(ctx); err != nil {
		return false, errors.Wrap(err, "call IndexExists() error")
	}
	if !exists {
		// Create a new index.
		if res, err = es.client.CreateIndex(es.esIndex).Do(ctx); err != nil {
			return false, errors.Wrap(err, "call CreateIndex() error")
		}
		if !res.Acknowledged {
			return false, errors.Wrap(err, "call CreateIndex() failed")
		}
	}
	return
}
