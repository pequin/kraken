package kraken

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pequin/xlog"
)

// Copyright 2024 Vasiliy Vdovin

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var ErrorNotFound = errors.New("the requested endpoint is not configured on KrakenD")
var ErrorBadRequest = errors.New("client made a malformed request, i.e. json-schema validation failed")
var ErrorUnauthorized = errors.New("client sent an invalid JWT token or its claims")
var ErrorForbidden = errors.New("the user is allowed to use the API, but not the resource, e.g.: Insufficient JWT role, or bot detector banned it")
var ErrorTooManyRequests = errors.New("the client reached the rate limit for the endpoint")
var ErrorServiceUnavailable = errors.New("all clients together reached the configured global rate limit for the endpoint")
var ErrorInternalServer = errors.New("default error code, and in general, when backends return any status above 400")

type SpotData struct {
}

func NewSpotData() *SpotData {

	api := SpotData{}

	return &api
}

func responseStatusCode(response *http.Response) error {

	if response.StatusCode == 404 {
		return ErrorNotFound
	} else if response.StatusCode == 400 {
		return ErrorBadRequest
	} else if response.StatusCode == 401 {
		return ErrorUnauthorized
	} else if response.StatusCode == 403 {
		return ErrorForbidden
	} else if response.StatusCode == 429 {
		return ErrorTooManyRequests
	} else if response.StatusCode == 503 {
		return ErrorServiceUnavailable
	} else if response.StatusCode == 500 {
		return ErrorInternalServer
	}
	return nil
}

func responseToJSON(response *http.Response, data any) error {

	// Body from response.
	bdy, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	// Parse json.
	return json.Unmarshal(bdy, data)
}

type Trade struct {
	Id       uint64
	Price    float64
	Volume   float64
	IsBuy    bool
	IsMarket bool
	Time     time.Time
}

func (s *SpotData) trades(pair string, since time.Time) ([]Trade, time.Time, error) {

	tds := make([]Trade, 0)

	last := time.Time{}

	var data struct {
		Result any `json:"result"`
	}

	// Create request.
	rqt, err := http.NewRequest("GET", "https://api.kraken.com/0/public/Trades", nil)
	xlog.Fatalln(err)

	// Adding headers.
	rqt.Header.Add("Content-Type", "application/json")

	// Adding queries.
	qey := rqt.URL.Query()
	qey.Add("pair", strings.ToUpper(pair))
	qey.Add("since", strconv.FormatInt(int64(since.UTC().UnixNano()), 10))
	qey.Add("count", "10")
	rqt.URL.RawQuery = qey.Encode()

	// Execute request.
	rpe, err := http.DefaultClient.Do(rqt)
	xlog.Fatalln(err)

	if err := responseStatusCode(rpe); err != nil {
		return tds, last, err
	}

	xlog.Fatalln(responseToJSON(rpe, &data))

	for k, v := range data.Result.(map[string]any) {

		// Trades.
		if k == strings.ToUpper(pair) {

			for _, v := range v.([]any) {

				t := Trade{}

				v := v.([]any)

				// Parse price.
				price, err := strconv.ParseFloat(v[0].(string), 64)
				xlog.Fatalln(err)
				t.Price = price

				// Parse volume.
				volume, err := strconv.ParseFloat(v[1].(string), 64)
				xlog.Fatalln(err)
				t.Volume = volume

				// Parse time.
				tsp, err := json.Marshal(v[2])
				xlog.Fatalln(err)

				sec, err := strconv.ParseInt(string(tsp[:10]), 10, 64)
				xlog.Fatalln(err)

				nsec, err := strconv.ParseInt(string(tsp[11:]), 10, 64)
				xlog.Fatalln(err)

				t.Time = time.Unix(sec, nsec).UTC()

				// Parse buy/sell.
				t.IsBuy = v[3].(string) == "b"

				// Parse market/limit.
				t.IsMarket = v[4].(string) == "m"

				// Parse trade id.
				t.Id = uint64(v[6].(float64))

				tds = append(tds, t)
			}

		} else if k == "last" {

			lst, err := strconv.ParseInt(v.(string), 10, 64)
			xlog.Fatalln(err)

			last = time.Unix(0, lst).UTC()
		}

	}

	return tds, last, nil
}

func (s *SpotData) Trades(pair string, from time.Time, duration time.Duration, cluster func(trades []Trade, last time.Time)) error {

	// Cluster.
	csr := make([]Trade, 0)

	// Next timestamp.
	pts := time.Time{}

	isContinue := true

	for getID := 0; isContinue; getID++ {

		if getID != 0 {
			time.Sleep(time.Second)
		}
		tds, lst, err := s.trades(pair, from)

		if err != nil {
			return err
		}

		sort.Slice(tds, func(i, j int) bool {
			return tds[i].Id < tds[j].Id
		})

		for i := 0; i < len(tds) && isContinue; i++ {

			// Prev timestamp.
			if getID == 0 && i == 0 {
				pts = tds[i].Time.Truncate(duration)
			}

			// Separation.
			if pts != tds[i].Time.Truncate(duration) {

				cluster(csr, lst)

				csr = csr[:0]
			}

			pts = tds[i].Time.Truncate(duration)

			csr = append(csr, tds[i])

			isContinue = time.Now().UTC().Truncate(duration) != tds[i].Time.Truncate(duration)
		}

		from = lst.Add(time.Nanosecond)
	}

	return nil
}
