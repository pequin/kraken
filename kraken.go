package kraken

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
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

func (s *SpotData) Trades(pair string, since time.Time) ([]Trade, uint64, error) {

	tds := make([]Trade, 0)

	last := uint64(0)

	var data struct {
		Result any `json:"result"`
	}

	// Create request.
	rqt, err := http.NewRequest("GET", "https://api.kraken.com/0/public/Trades", nil)
	if err != nil {
		return tds, last, err
	}

	// Adding headers.
	rqt.Header.Add("Content-Type", "application/json")

	// Adding queries.
	qey := rqt.URL.Query()
	qey.Add("pair", pair)
	qey.Add("since", strconv.FormatInt(since.UTC().UnixNano(), 10))
	qey.Add("count", "1000")
	rqt.URL.RawQuery = qey.Encode()

	// Execute request.
	rpe, err := http.DefaultClient.Do(rqt)
	if err != nil {
		return tds, last, err
	}

	if err := responseStatusCode(rpe); err != nil {
		return tds, last, err
	}

	if err := responseToJSON(rpe, &data); err != nil {
		return tds, last, err
	}

	for k, v := range data.Result.(map[string]any) {

		// Trades.
		if k == strings.ToUpper(pair) {

			for _, v := range v.([]any) {

				t := Trade{}

				v := v.([]any)

				// Parse price.
				price, err := strconv.ParseFloat(v[0].(string), 64)
				if err != nil {
					return tds, last, err
				}
				t.Price = price

				// Parse volume.
				volume, err := strconv.ParseFloat(v[1].(string), 64)
				if err != nil {
					return tds, last, err
				}
				t.Volume = volume

				// Parse time.
				tsp, err := json.Marshal(v[2])
				if err != nil {
					return tds, last, err
				}

				sec, err := strconv.ParseInt(string(tsp[:10]), 10, 64)
				if err != nil {
					return tds, last, err
				}

				nsec, err := strconv.ParseInt(string(tsp[11:]), 10, 64)
				if err != nil {
					return tds, last, err
				}

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

			lst, err := strconv.ParseUint(v.(string), 10, 64)
			if err != nil {
				return tds, last, err
			}

			last = lst

		}

	}

	return tds, last, nil
}
