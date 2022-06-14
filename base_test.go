package esutils

import (
	"context"
	"fmt"
	"github.com/Jeffail/gabs/v2"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/unionj-cloud/go-doudou/toolkit/constants"
	"os"
	"testing"
	"time"
)

// PrepareTestEnvironment prepares test environment
func PrepareTestEnvironment() (func(), string, int) {
	var terminateContainer func() // variable to store function to terminate container
	var host string
	var port int
	var err error
	terminateContainer, host, port, err = SetupEs7Container(logrus.New())
	if err != nil {
		panic("failed to setup Elasticsearch container")
	}
	return terminateContainer, host, port
}

// SetupEs7Container starts elasticsearch 7.17.4 docker container
func SetupEs7Container(logger *logrus.Logger) (func(), string, int, error) {
	logger.Info("setup Elasticsearch v6 Container")
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "elasticsearch:7.17.4",
		ExposedPorts: []string{"9200/tcp", "9300/tcp"},
		Env: map[string]string{
			"discovery.type": "single-node",
		},
		WaitingFor: wait.ForLog("started"),
	}

	esC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		logger.Errorf("error starting Elasticsearch container: %s", err)
		panic(fmt.Sprintf("%v", err))
	}

	closeContainer := func() {
		logger.Info("terminating container")
		err := esC.Terminate(ctx)
		if err != nil {
			logger.Errorf("error terminating Elasticsearch container: %s", err)
			panic(fmt.Sprintf("%v", err))
		}
	}

	host, _ := esC.Host(ctx)
	p, _ := esC.MappedPort(ctx, "9200/tcp")
	port := p.Int()

	return closeContainer, host, port, nil
}

var esHost string
var esPort int

func TestMain(m *testing.M) {
	os.Setenv("TZ", "Asia/Shanghai")
	var terminator func()
	terminator, esHost, esPort = PrepareTestEnvironment()
	code := m.Run()
	terminator()
	os.Exit(code)
}

func setupSubTest(esindex string) *Es {
	es := NewEs(esindex, esindex, WithLogger(logrus.StandardLogger()), WithUrls([]string{fmt.Sprintf("http://%s:%d", esHost, esPort)}))
	prepareTestIndex(es)
	prepareTestData(es)
	return es
}

func prepareTestIndex(es *Es) {
	mapping := NewMapping(MappingPayload{
		Base{
			Index: es.esIndex,
			Type:  es.esType,
		},
		[]Field{
			{
				Name: "createAt",
				Type: DATE,
			},
			{
				Name: "text",
				Type: TEXT,
			},
		},
	})
	_, err := es.NewIndex(context.Background(), mapping)
	if err != nil {
		panic(err)
	}
}

func prepareTestData(es *Es) {
	data1 := "2020-06-01"
	data2 := "2020-06-20"
	data3 := "2020-07-10"

	createAt1, _ := time.Parse(constants.FORMAT2, data1)
	createAt2, _ := time.Parse(constants.FORMAT2, data2)
	createAt3, _ := time.Parse(constants.FORMAT2, data3)

	err := es.BulkSaveOrUpdate(context.Background(), []interface{}{
		map[string]interface{}{
			"id":       "9seTXHoBNx091WJ2QCh5",
			"createAt": createAt1.UTC().Format(constants.FORMATES),
			"type":     "education",
			"text":     "2020年7月8日11时25分，高考文科综合/理科综合科目考试将要结束时，平顶山市一中考点一考生突然情绪失控，先后抓其右边、后边考生答题卡，造成两位考生答题卡损毁。",
		},
		map[string]interface{}{
			"id":       "9seTXHoBNx091WJ2QCh6",
			"createAt": createAt2.UTC().Format(constants.FORMATES),
			"type":     "sport",
			"text":     "考场两位监考教师及时制止，并稳定了考场秩序，市一中考点按程序启用备用答题卡，按规定补足答题卡被损毁的两位考生耽误的考试时间，两位考生将损毁卡的内容誊写在新答题卡上。",
		},
		map[string]interface{}{
			"id":       "9seTXHoBNx091WJ2QCh7",
			"createAt": createAt3.UTC().Format(constants.FORMATES),
			"type":     "culture",
			"text":     "目前，我办已将损毁其他考生答题卡的考生违规情况上报河南省招生办公室，将依规对该考生进行处理。平顶山市招生考试委员会办公室",
		},
	})
	if err != nil {
		panic(err)
	}
}

func Test_query(t *testing.T) {
	type args struct {
		startDate  string
		endDate    string
		dateField  string
		queryConds []QueryCond
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				startDate: "2020-06-01",
				endDate:   "2020-07-10",
				dateField: "createAt",
				queryConds: []QueryCond{
					{
						Pair: map[string][]interface{}{
							"text":    {"考生"},
							"school":  {"西安理工+西安交大"},
							"address": {"北京+-西安"},
							"company": {"-unionj"},
						},
						QueryLogic: SHOULD,
						QueryType:  MATCHPHRASE,
					},
					{
						Pair: map[string][]interface{}{
							"text": {"高考"},
						},
						QueryLogic: MUST,
						QueryType:  MATCHPHRASE,
					},
					{
						Pair: map[string][]interface{}{
							"text": {"北京高考"},
						},
						QueryLogic: MUSTNOT,
						QueryType:  MATCHPHRASE,
					},
					{
						Pair: map[string][]interface{}{
							"content":      {"北京"},
							"content_full": {"unionj"},
						},
						QueryLogic: MUST,
						QueryType:  TERMS,
					},
				},
			},
			want: `{"bool":{"minimum_should_match":"1","must":[{"range":{"createAt":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","from":"2020-06-01","include_lower":true,"include_upper":false,"time_zone":"Asia/Shanghai","to":"2020-07-10"}}},{"bool":{"should":{"match_phrase":{"text":{"query":"高考"}}}}},{"terms":{"content":["北京"]}},{"terms":{"content_full":["unionj"]}}],"must_not":{"bool":{"should":{"match_phrase":{"text":{"query":"北京高考"}}}}},"should":[{"bool":{"should":{"match_phrase":{"text":{"query":"考生"}}}}},{"bool":{"should":{"bool":{"must":[{"match_phrase":{"school":{"query":"西安理工"}}},{"match_phrase":{"school":{"query":"西安交大"}}}]}}}},{"bool":{"should":{"bool":{"must":{"match_phrase":{"address":{"query":"北京"}}},"must_not":{"match_phrase":{"address":{"query":"西安"}}}}}}},{"bool":{"should":{"bool":{"must_not":{"match_phrase":{"company":{"query":"unionj"}}}}}}}]}}`,
		},
		{
			name: "",
			args: args{
				startDate: "2020-06-01",
				endDate:   "2020-07-10",
				dateField: "createAt",
				queryConds: []QueryCond{
					{
						Pair: map[string][]interface{}{
							"type.keyword": {"education"},
							"status":       {float64(200)},
						},
						QueryLogic: MUST,
						QueryType:  TERMS,
					},
					{
						Pair: map[string][]interface{}{
							"dept.keyword": {"unionj*"},
						},
						QueryLogic: SHOULD,
						QueryType:  WILDCARD,
					},
					{
						Pair: map[string][]interface{}{
							"position.keyword": {"dev*"},
						},
						QueryLogic: MUST,
						QueryType:  WILDCARD,
					},
					{
						Pair: map[string][]interface{}{
							"city.keyword": {"四川*"},
						},
						QueryLogic: MUSTNOT,
						QueryType:  WILDCARD,
					},
					{
						Pair: map[string][]interface{}{
							"project.keyword": {"unionj"},
						},
						QueryLogic: SHOULD,
						QueryType:  PREFIX,
					},
					{
						Pair: map[string][]interface{}{
							"name.keyword": {"unionj"},
						},
						QueryLogic: MUSTNOT,
						QueryType:  PREFIX,
					},
					{
						Pair: map[string][]interface{}{
							"book.keyword": {"go"},
						},
						QueryLogic: MUST,
						QueryType:  PREFIX,
					},
				},
			},
			want: `{"bool":{"minimum_should_match":"1","must":[{"range":{"createAt":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","from":"2020-06-01","include_lower":true,"include_upper":false,"time_zone":"Asia/Shanghai","to":"2020-07-10"}}},{"terms":{"type.keyword":["education"]}},{"terms":{"status":[200]}},{"wildcard":{"position.keyword":{"wildcard":"dev*"}}},{"prefix":{"book.keyword":"go"}}],"must_not":[{"wildcard":{"city.keyword":{"wildcard":"四川*"}}},{"prefix":{"name.keyword":"unionj"}}],"should":[{"wildcard":{"dept.keyword":{"wildcard":"unionj*"}}},{"prefix":{"project.keyword":"unionj"}}]}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := query(tt.args.startDate, tt.args.endDate, tt.args.dateField, tt.args.queryConds, nil)
			var src interface{}
			var err error
			if src, err = bq.Source(); err != nil {
				panic(err)
			}
			want, _ := gabs.ParseJSON([]byte(tt.want))
			_src := gabs.Wrap(src)
			fmt.Println(_src.String())
			if !assert.ElementsMatch(t, _src.Path("bool.must").Data(), want.Path("bool.must").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.must").Data(), want.Path("bool.must").Data())
			}
			if !assert.ElementsMatch(t, _src.Path("bool.should").Data(), want.Path("bool.should").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.should").Data(), want.Path("bool.should").Data())
			}
		})
	}
}

func Test_range_query(t *testing.T) {
	param1 := make(map[string]interface{})
	param1["to"] = 0.4
	param1["include_upper"] = false
	param1["include_lower"] = true

	param2 := make(map[string]interface{})
	param2["from"] = 0.6
	param2["include_upper"] = false
	param2["include_lower"] = true

	type args struct {
		startDate  string
		endDate    string
		dateField  string
		queryConds []QueryCond
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "1",
			args: args{
				startDate: "2020-06-01",
				endDate:   "2020-07-01",
				dateField: "acceptDate",
				queryConds: []QueryCond{
					{
						Pair: map[string][]interface{}{
							"senseResult": {param1},
						},
						QueryLogic: MUST,
						QueryType:  RANGE,
					},
					{
						Pair: map[string][]interface{}{
							"visitSenseResult": {param2},
						},
						QueryLogic: SHOULD,
						QueryType:  RANGE,
					},
					{
						Pair: map[string][]interface{}{
							"commonSenseResult": {param2},
						},
						QueryLogic: MUSTNOT,
						QueryType:  RANGE,
					},
					{
						Pair: map[string][]interface{}{
							"orderPhrase": {float64(300)},
						},
						QueryLogic: MUST,
						QueryType:  TERMS,
					},
				},
			},
			want: `{"bool":{"minimum_should_match":"1","must":[{"range":{"acceptDate":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","from":"2020-06-01","include_lower":true,"include_upper":false,"time_zone":"Asia/Shanghai","to":"2020-07-01"}}},{"range":{"senseResult":{"from":null,"include_lower":true,"include_upper":false,"to":0.4}}},{"terms":{"orderPhrase":[300]}}],"must_not":{"range":{"commonSenseResult":{"from":0.6,"include_lower":true,"include_upper":false,"to":null}}},"should":{"range":{"visitSenseResult":{"from":0.6,"include_lower":true,"include_upper":false,"to":null}}}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := query(tt.args.startDate, tt.args.endDate, tt.args.dateField, tt.args.queryConds, nil)
			var src interface{}
			var err error
			if src, err = bq.Source(); err != nil {
				panic(err)
			}
			want, _ := gabs.ParseJSON([]byte(tt.want))
			_src := gabs.Wrap(src)
			fmt.Println(_src.String())
			if !assert.ElementsMatch(t, _src.Path("bool.must").Data(), want.Path("bool.must").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.must").Data(), want.Path("bool.must").Data())
			}
			if !assert.Equal(t, _src.Path("bool.should").Data(), want.Path("bool.should").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.should").Data(), want.Path("bool.should").Data())
			}
		})
	}
}

func Test_exists_query(t *testing.T) {
	type args struct {
		startDate  string
		endDate    string
		dateField  string
		queryConds []QueryCond
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "1",
			args: args{
				startDate: "2020-06-01",
				endDate:   "2020-07-01",
				dateField: "acceptDate",
				queryConds: []QueryCond{
					{
						Pair: map[string][]interface{}{
							"delete_at": {},
						},
						QueryLogic: MUSTNOT,
						QueryType:  EXISTS,
					},
					{
						Pair: map[string][]interface{}{
							"flag": {},
						},
						QueryLogic: MUST,
						QueryType:  EXISTS,
					},
					{
						Pair: map[string][]interface{}{
							"status": {},
						},
						QueryLogic: SHOULD,
						QueryType:  EXISTS,
					},
					{
						Pair: map[string][]interface{}{
							"orderPhrase": {float64(300)},
						},
						QueryLogic: MUST,
						QueryType:  TERMS,
					},
				},
			},
			want: `{"bool":{"minimum_should_match":"1","must":[{"range":{"acceptDate":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","from":"2020-06-01","include_lower":true,"include_upper":false,"time_zone":"Asia/Shanghai","to":"2020-07-01"}}},{"exists":{"field":"flag"}},{"terms":{"orderPhrase":[300]}}],"must_not":{"exists":{"field":"delete_at"}},"should":{"exists":{"field":"status"}}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := query(tt.args.startDate, tt.args.endDate, tt.args.dateField, tt.args.queryConds, nil)
			var src interface{}
			var err error
			if src, err = bq.Source(); err != nil {
				panic(err)
			}
			want, _ := gabs.ParseJSON([]byte(tt.want))
			_src := gabs.Wrap(src)
			fmt.Println(_src.String())
			if !assert.ElementsMatch(t, _src.Path("bool.must").Data(), want.Path("bool.must").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.must").Data(), want.Path("bool.must").Data())
			}
			if !assert.Equal(t, _src.Path("bool.should").Data(), want.Path("bool.should").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.should").Data(), want.Path("bool.should").Data())
			}
		})
	}
}

func Test_children_query(t *testing.T) {
	type args struct {
		startDate  string
		endDate    string
		dateField  string
		queryConds []QueryCond
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "1",
			args: args{
				startDate: "2020-06-01",
				endDate:   "2020-07-01",
				dateField: "acceptDate",
				queryConds: []QueryCond{
					{
						Pair: map[string][]interface{}{
							"delete_at": {},
						},
						QueryLogic: MUSTNOT,
						QueryType:  EXISTS,
					},
					{
						Pair: map[string][]interface{}{
							"status": {float64(100), float64(300)},
						},
						QueryLogic: MUSTNOT,
						QueryType:  TERMS,
					},
					{
						QueryLogic: MUSTNOT,
						Children: []QueryCond{
							{
								Pair: map[string][]interface{}{
									"type": {"网络调查"},
								},
								QueryLogic: MUST,
								QueryType:  TERMS,
							},
							{
								Pair: map[string][]interface{}{
									"price": {float64(0)},
								},
								QueryLogic: MUST,
								QueryType:  TERMS,
							},
						},
					},
				},
			},
			want: `{"bool":{"must":{"range":{"acceptDate":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","from":"2020-06-01","include_lower":true,"include_upper":false,"time_zone":"Asia/Shanghai","to":"2020-07-01"}}},"must_not":[{"exists":{"field":"delete_at"}},{"terms":{"status":[100,300]}},{"bool":{"must":[{"terms":{"type":["网络调查"]}},{"terms":{"price":[0]}}]}}]}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := query(tt.args.startDate, tt.args.endDate, tt.args.dateField, tt.args.queryConds, nil)
			var src interface{}
			var err error
			if src, err = bq.Source(); err != nil {
				panic(err)
			}
			want, _ := gabs.ParseJSON([]byte(tt.want))
			_src := gabs.Wrap(src)
			fmt.Println(_src.String())
			if !assert.Equal(t, _src.Path("bool.must").Data(), want.Path("bool.must").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.must").Data(), want.Path("bool.must").Data())
			}
			if !assert.ElementsMatch(t, _src.Path("bool.must_not").Data(), want.Path("bool.must_not").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.must_not").Data(), want.Path("bool.must_not").Data())
			}
			if !assert.ElementsMatch(t, _src.Path("bool.should").Data(), want.Path("bool.should").Data()) {
				t.Errorf("query() = %v, want %v", _src.Path("bool.should").Data(), want.Path("bool.should").Data())
			}
		})
	}
}

func TestNewEs(t *testing.T) {
	url := "http://test.com"
	username := "unionj"
	password := "unionj"
	client, err := elastic.NewSimpleClient(
		elastic.SetErrorLog(logrus.New()),
		elastic.SetURL(url),
		elastic.SetBasicAuth(username, password),
		elastic.SetGzip(true),
	)
	if err != nil {
		panic(fmt.Errorf("NewSimpleClient() error: %+v\n", err))
	}
	type args struct {
		esIndex string
		esType  string
		opts    []EsOption
	}
	tests := []struct {
		name string
		args args
		want *Es
	}{
		{
			name: "",
			args: args{
				esIndex: "test1",
				opts: []EsOption{
					WithUsername(username),
					WithPassword(password),
					WithUrls([]string{url}),
				},
			},
			want: nil,
		},
		{
			name: "",
			args: args{
				esIndex: "test2",
				opts: []EsOption{
					WithClient(client),
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		assert.NotPanics(t, func() {
			got := NewEs(tt.args.esIndex, tt.args.esType, tt.args.opts...)
			got.SetType(got.esIndex)
		})
	}
}

func TestEs_newDefaultClient(t *testing.T) {
	assert.Panics(t, func() {
		NewEs("test", "test", WithUrls([]string{"wrongurl"}))
	})
}

func TestPaging_String(t *testing.T) {
	p := Paging{
		StartDate: "2021-01-01",
		EndDate:   "2021-10-31",
		DateField: "submitTime",
		QueryConds: []QueryCond{
			{
				Pair: map[string][]interface{}{
					"user.keyword":     {"wubin1989"},
					"phone.keyword":    {"123456"},
					"district.keyword": {"Beijing"},
				},
				QueryLogic: MUST,
				QueryType:  TERMS,
			},
			{
				Pair: map[string][]interface{}{
					"score": {-1},
				},
				QueryLogic: MUSTNOT,
				QueryType:  TERMS,
			},
		},
	}
	fmt.Println(p.String())
}
