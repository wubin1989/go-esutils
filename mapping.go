package esutils

import (
	"context"
	"github.com/Jeffail/gabs/v2"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/unionj-cloud/go-doudou/toolkit/stringutils"
)

// MappingPayload defines request payload for es index mapping
type MappingPayload struct {
	Base
	Fields []Field `json:"fields"`
}

// NewMapping return es mapping json string from mp
func NewMapping(mp MappingPayload) string {
	var (
		mapping    *gabs.Container
		properties *gabs.Container
	)

	mapping = gabs.New()
	mapping.SetP("60s", "settings.refresh_interval")
	mapping.SetP("1", "settings.number_of_replicas")
	mapping.SetP("15", "settings.number_of_shards")

	properties = gabs.New()
	for _, f := range mp.Fields {
		properties.Set(f.Type, f.Name, "type")
	}

	esType := mp.Type
	if stringutils.IsEmpty(esType) {
		esType = "_doc"
	}
	mapping.Set(properties, "mappings", esType, "properties")

	return mapping.String()
}

// PutMapping updates mapping
func (es *Es) PutMapping(ctx context.Context, mp MappingPayload) error {
	var (
		mapping    *gabs.Container
		properties *gabs.Container
		res        *elastic.PutMappingResponse
		err        error
	)
	mapping = gabs.New()
	properties = gabs.New()
	for _, f := range mp.Fields {
		properties.Set(f.Type, f.Name, "type")
	}
	mapping.Set(properties, "properties")
	if res, err = es.client.PutMapping().Index(mp.Index).IncludeTypeName(false).BodyString(mapping.String()).Do(ctx); err != nil {
		return errors.Wrap(err, "call PutMapping() error")
	}
	if !res.Acknowledged {
		return errors.New("putmapping failed!!!")
	}
	return nil
}

// GetMapping get mapping
func (es *Es) GetMapping(ctx context.Context) (map[string]interface{}, error) {
	var (
		res map[string]interface{}
		err error
	)
	if res, err = es.client.GetMapping().Index(es.esIndex).Type(es.esType).IncludeTypeName(true).Do(ctx); err != nil {
		return nil, errors.Wrap(err, "call GetMapping() error")
	}
	return res, nil
}

// PutMappingJson updates mapping with json data
func (es *Es) PutMappingJson(ctx context.Context, mapping string) error {
	var (
		res *elastic.PutMappingResponse
		err error
	)
	if res, err = es.client.PutMapping().Index(es.esIndex).IncludeTypeName(false).BodyString(mapping).Do(ctx); err != nil {
		return errors.Wrap(err, "call PutMappingJson() error")
	}
	if !res.Acknowledged {
		return errors.New("putmapping failed!!!")
	}
	return nil
}
