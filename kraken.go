package kraken

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
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

func Trades(pair string, from time.Time, trade func(id uint64, tme, nte time.Time, price, volume float64, buy bool)) time.Time {

	var data struct {
		Result any `json:"result"`
	}

	// Create request.
	rqt, err := http.NewRequest("GET", "https://api.kraken.com/0/public/Trades", nil)
	xlog.Fatalln(err)

	// Adding headers.
	rqt.Header.Add("User-Agent", "github.com/pequin/kraken")
	rqt.Header.Add("Content-Type", "application/json")

	// Adding queries.
	qey := rqt.URL.Query()
	qey.Add("pair", strings.ToUpper(pair))
	qey.Add("since", strconv.FormatInt(int64(from.UTC().UnixNano()), 10))
	qey.Add("count", "10")
	rqt.URL.RawQuery = qey.Encode()

	// Execute request.
	rpe, err := http.DefaultClient.Do(rqt)
	xlog.Fatalln(err)

	// Checking response of a server.
	xlog.Fatalln(responseStatusCode(rpe))

	// Body from response.
	bdy, err := io.ReadAll(rpe.Body)
	xlog.Fatalln(err)

	// Parse json.
	xlog.Fatalln(json.Unmarshal(bdy, &data))

	// Result.
	rst := data.Result.(map[string]any)

	// Timestamp for last trade.
	lst, err := strconv.ParseInt(rst["last"].(string), 10, 64)
	xlog.Fatalln(err)

	// Trades.
	tds := rst[strings.ToUpper(pair)].([]any)

	for i := 0; i < len(tds)-1; i++ {

		t := tds[i].([]any)

		// Parse price.
		pce, err := strconv.ParseFloat(t[0].(string), 64)
		xlog.Fatalln(err)

		// Parse volume.
		vle, err := strconv.ParseFloat(t[1].(string), 64)
		xlog.Fatalln(err)

		// Parse time.
		tsp, err := json.Marshal(t[2])
		xlog.Fatalln(err)

		csc, err := strconv.ParseInt(string(tsp[:10]), 10, 64)
		xlog.Fatalln(err)

		cnc, err := strconv.ParseInt(string(tsp[11:]), 10, 64)
		xlog.Fatalln(err)

		// Parse next time.
		nts, err := json.Marshal(tds[i+1].([]any)[2])
		xlog.Fatalln(err)

		nsc, err := strconv.ParseInt(string(nts[:10]), 10, 64)
		xlog.Fatalln(err)

		nns, err := strconv.ParseInt(string(nts[11:]), 10, 64)
		xlog.Fatalln(err)

		// Parse buy/sell.
		buy := t[3].(string) == "b"

		// Parse trade id.
		id := uint64(t[6].(float64))

		trade(id, time.Unix(csc, cnc).UTC(), time.Unix(nsc, nns).UTC(), pce, vle, buy)
	}

	from = time.Unix(0, lst).UTC()

	if len(tds) > 0 {

		time.Sleep(time.Second)

		Trades(pair, from, trade)
	}

	return from
}
