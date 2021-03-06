package esutils

import (
	"context"
	"github.com/Jeffail/gabs/v2"
	"testing"
)

func TestNewMapping(t *testing.T) {
	es := setupSubTest("test_newmapping")

	type args struct {
		mp MappingPayload
	}

	parsed, err := gabs.ParseJSON([]byte(`{
    "settings": {
        "refresh_interval": "60s",
        "number_of_replicas": "1",
        "number_of_shards": "15"
    },
    "mappings": {
        "` + es.esType + `": {
            "properties": {
				"createAt": {
					"type": "date"
				}
			}
        }
    }
}`))
	if err != nil {
		panic(err)
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "1",
			args: args{
				mp: MappingPayload{
					Base{
						Index: es.esIndex,
						Type:  es.esType,
					},
					[]Field{
						{
							Name: "createAt",
							Type: DATE,
						},
					},
				},
			},
			want: parsed.String(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMapping(tt.args.mp); got != tt.want {
				t.Errorf("NewMapping() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPutMapping(t *testing.T) {
	es := setupSubTest("test_putmapping")

	type args struct {
		mp MappingPayload
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "",
			args: args{
				mp: MappingPayload{
					Base{
						Index: es.esIndex,
						Type:  es.esType,
					},
					[]Field{
						{
							Name: "orderPhrase",
							Type: SHORT,
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := es.PutMapping(context.Background(), tt.args.mp); (err != nil) != tt.wantErr {
				t.Errorf("PutMapping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
