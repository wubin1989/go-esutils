package esutils

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList(t *testing.T) {
	es := setupSubTest("test_list")
	type args struct {
		paging   *Paging
		esIndex  string
		esType   string
		callback func(message json.RawMessage) (interface{}, error)
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "",
			args: args{
				paging: &Paging{
					StartDate: "2020-06-01",
					EndDate:   "2020-07-10",
					DateField: "createAt",
					Skip:      0,
					Limit:     1,
					Sortby: []Sort{
						{
							Field:     "createAt",
							Ascending: false,
						},
					},
					QueryConds: []QueryCond{
						{
							Pair: map[string][]interface{}{
								"text": {"考生"},
							},
							QueryLogic: SHOULD,
							QueryType:  MATCHPHRASE,
						},
					},
				},
				callback: nil,
			},
			want:    "考场两位监考教师及时制止，并稳定了考场秩序，市一中考点按程序启用备用答题卡，按规定补足答题卡被损毁的两位考生耽误的考试时间，两位考生将损毁卡的内容誊写在新答题卡上。",
			wantErr: false,
		},
		{
			name: "",
			args: args{
				paging: nil,
				callback: func(message json.RawMessage) (interface{}, error) {
					var p map[string]interface{}
					json.Unmarshal(message, &p)
					p["text"] = "fixed content"
					return p, nil
				},
			},
			want:    "fixed content",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := es.List(context.Background(), tt.args.paging, tt.args.callback)
			if (err != nil) != tt.wantErr {
				t.Errorf("List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) == 0 {
				t.Error("got's length shouldn't be zero")
				return
			}
			assert.Equal(t, tt.want, got[0].(map[string]interface{})["text"])
		})
	}
}
