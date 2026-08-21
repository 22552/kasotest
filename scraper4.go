package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	apiBase            = "https://api.scratch.mit.edu"
	pageLimit          = 40
	pageConcurrency    = 16
	pageLookahead      = pageConcurrency * 4
	replyConcurrency   = 24
	maxAttempts        = 6
	maxSafetyPages     = 100000
	progressEvery      = 100
	incrementalOverlap = 400
)

type apiAuthor struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type apiComment struct {
	ID              int64     `json:"id"`
	Content         string    `json:"content"`
	DatetimeCreated string    `json:"datetime_created"`
	Author          apiAuthor `json:"author"`
	ReplyCount      int       `json:"reply_count"`
}

type outComment struct {
	ID       int64        `json:"id"`
	User     string       `json:"user"`
	UserID   int64        `json:"user_id"`
	Datetime string       `json:"datetime"`
	Content  string       `json:"content"`
	Replies  []outComment `json:"replies"`
}

type pageResult struct {
	page    int
	data    []apiComment
	err     error
	elapsed time.Duration
}

var directClient = &http.Client{
	Timeout: 35 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   64,
		MaxConnsPerHost:       64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   8 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		ForceAttemptHTTP2:     true,
	},
}

func backoff(resp *http.Response, attempt int) time.Duration {
	if resp != nil {
		if v := strings.TrimSpace(resp.Header.Get("Retry-After")); v != "" {
			if sec, err := strconv.Atoi(v); err == nil && sec > 0 && sec <= 60 {
				return time.Duration(sec) * time.Second
			}
		}
	}
	d := 400 * time.Millisecond * time.Duration(1<<attempt)
	if d > 12*time.Second {
		d = 12 * time.Second
	}
	return d
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func getJSON(rawURL string, dst any) error {
	return getJSONContext(context.Background(), rawURL, dst)
}

func getJSONContext(ctx context.Context, rawURL string, dst any) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "kasotest-genko-scraper/10")

		resp, err := directClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
			if attempt+1 >= maxAttempts {
				break
			}
			wait := backoff(nil, attempt)
			fmt.Printf("[http] direct attempt=%d/%d error=%v; retry in %s\n", attempt+1, maxAttempts, err, wait)
			if err := sleepContext(ctx, wait); err != nil {
				return err
			}
			continue
		}

		if resp.StatusCode == http.StatusOK {
			err = json.NewDecoder(resp.Body).Decode(dst)
			resp.Body.Close()
			if err == nil {
				return nil
			}
			lastErr = fmt.Errorf("decode: %w", err)
			if attempt+1 >= maxAttempts {
				break
			}
			wait := backoff(resp, attempt)
			fmt.Printf("[http] direct invalid JSON attempt=%d/%d; retry in %s\n", attempt+1, maxAttempts, wait)
			if err := sleepContext(ctx, wait); err != nil {
				return err
			}
			continue
		}

		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("GET %s: HTTP %d", rawURL, resp.StatusCode)
			if attempt+1 >= maxAttempts {
				break
			}
			wait := backoff(resp, attempt)
			fmt.Printf("[http] direct HTTP %d attempt=%d/%d; retry in %s\n", resp.StatusCode, attempt+1, maxAttempts, wait)
			if err := sleepContext(ctx, wait); err != nil {
				return err
			}
			continue
		}

		return fmt.Errorf("GET %s: HTTP %d", rawURL, resp.StatusCode)
	}
	if lastErr == nil {
		lastErr = errors.New("request failed")
	}
	return lastErr
}

func commentsURL(studio string, offset int) string {
	return fmt.Sprintf("%s/studios/%s/comments?limit=%d&offset=%d", apiBase, url.PathEscape(studio), pageLimit, offset)
}

func repliesURL(studio string, parentID int64, offset int) string {
	return fmt.Sprintf("%s/studios/%s/comments/%d/replies?limit=%d&offset=%d", apiBase, url.PathEscape(studio), parentID, pageLimit, offset)
}

func parseCommentTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func fetchTopLevel(studio string, cutoff time.Time, old map[int64]outComment, incremental bool) ([]apiComment, bool, time.Time, error) {
	all := make([]apiComment, 0, 8192)
	seen := make(map[int64]struct{}, 8192)
	started := time.Now()
	consecutiveKnown := 0
	cacheBoundary := false
	var oldestFetched time.Time

	mode := "full"
	if incremental {
		mode = "incremental"
	}
	fmt.Printf("[pages] start: route=direct mode=%s concurrency=%d lookahead=%d limit=%d cutoff=%s\n",
		mode, pageConcurrency, pageLookahead, pageLimit, cutoff.Format(time.RFC3339))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int, pageConcurrency)
	results := make(chan pageResult, pageLookahead)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for page := range jobs {
			t0 := time.Now()
			var data []apiComment
			err := getJSONContext(ctx, commentsURL(studio, page*pageLimit), &data)
			results <- pageResult{page: page, data: data, err: err, elapsed: time.Since(t0)}
		}
	}
	for i := 0; i < pageConcurrency; i++ {
		wg.Add(1)
		go worker()
	}

	nextSchedule := 0
	nextProcess := 0
	inFlight := 0
	pending := make(map[int]pageResult, pageLookahead)
	stopping := false
	var firstErr error
	var requestCount int64
	var totalRequestTime time.Duration

	fill := func() {
		for !stopping && inFlight < pageConcurrency && nextSchedule < maxSafetyPages && nextSchedule < nextProcess+pageLookahead {
			jobs <- nextSchedule
			nextSchedule++
			inFlight++
		}
	}
	fill()

	lastLoggedPages := 0
	oldest := ""
	actualPages := 0

	for inFlight > 0 {
		r := <-results
		inFlight--
		requestCount++
		totalRequestTime += r.elapsed

		if stopping {
			fill()
			continue
		}
		if r.err != nil {
			firstErr = fmt.Errorf("comments page %d: %w", r.page, r.err)
			stopping = true
			cancel()
			continue
		}
		pending[r.page] = r

		for !stopping {
			cur, ok := pending[nextProcess]
			if !ok {
				break
			}
			delete(pending, nextProcess)
			actualPages = nextProcess + 1
			data := cur.data
			nextProcess++

			if len(data) == 0 {
				stopping = true
				cancel()
				break
			}

			for _, c := range data {
				created, err := parseCommentTime(c.DatetimeCreated)
				if err != nil {
					firstErr = fmt.Errorf("comment %d time %q: %w", c.ID, c.DatetimeCreated, err)
					stopping = true
					cancel()
					break
				}
				oldestFetched = created
				oldest = created.UTC().Format(time.RFC3339)
				if created.Before(cutoff) {
					stopping = true
					cancel()
					break
				}
				if _, ok := seen[c.ID]; ok {
					continue
				}
				seen[c.ID] = struct{}{}
				all = append(all, c)

				if incremental {
					if _, ok := old[c.ID]; ok {
						consecutiveKnown++
					} else {
						consecutiveKnown = 0
					}
					if consecutiveKnown >= incrementalOverlap {
						cacheBoundary = true
						stopping = true
						cancel()
						break
					}
				}
			}
			if stopping || len(data) < pageLimit {
				if len(data) < pageLimit && !stopping {
					stopping = true
					cancel()
				}
				break
			}
		}

		if actualPages-lastLoggedPages >= pageConcurrency || stopping {
			secs := time.Since(started).Seconds()
			rate := 0.0
			if secs > 0 {
				rate = float64(actualPages) / secs
			}
			avgReq := time.Duration(0)
			if requestCount > 0 {
				avgReq = totalRequestTime / time.Duration(requestCount)
			}
			fmt.Printf("[pages] fetched=%d pages comments=%d oldest=%s cache_streak=%d rate=%.2f pages/s avg_req=%s elapsed=%s\n",
				actualPages, len(all), oldest, consecutiveKnown, rate, avgReq.Round(time.Millisecond), time.Since(started).Round(time.Second))
			lastLoggedPages = actualPages
		}
		fill()
	}

	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return nil, false, time.Time{}, firstErr
	}
	if cacheBoundary {
		fmt.Printf("[cache] incremental boundary: %d consecutive cached IDs; stopping deep pagination\n", incrementalOverlap)
	} else {
		fmt.Printf("[pages] stop: reached cutoff/end after %d pages\n", actualPages)
	}
	return all, cacheBoundary, oldestFetched, nil
}

func asOut(c apiComment) outComment {
	return outComment{
		ID:       c.ID,
		User:     c.Author.Username,
		UserID:   c.Author.ID,
		Datetime: c.DatetimeCreated,
		Content:  c.Content,
		Replies:  []outComment{},
	}
}

func fetchReplies(studio string, parent apiComment) ([]outComment, error) {
	if parent.ReplyCount <= 0 {
		return []outComment{}, nil
	}
	out := make([]outComment, 0, parent.ReplyCount)
	seen := make(map[int64]struct{}, parent.ReplyCount)
	for offset := 0; offset < parent.ReplyCount; offset += pageLimit {
		var data []apiComment
		if err := getJSON(repliesURL(studio, parent.ID, offset), &data); err != nil {
			return nil, err
		}
		for _, r := range data {
			if _, ok := seen[r.ID]; ok {
				continue
			}
			seen[r.ID] = struct{}{}
			out = append(out, asOut(r))
		}
		if len(data) < pageLimit {
			break
		}
	}
	return out, nil
}

func commentsToMap(old []outComment) map[int64]outComment {
	m := make(map[int64]outComment, len(old))
	for _, c := range old {
		m[c.ID] = c
	}
	return m
}

func loadOld(path string) ([]outComment, map[int64]outComment, string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, map[int64]outComment{}, "none", nil
		}
		return nil, map[int64]outComment{}, "none", err
	}
	defer f.Close()

	var asArray []outComment
	arrayErr := json.NewDecoder(f).Decode(&asArray)
	if arrayErr == nil {
		return asArray, commentsToMap(asArray), "array", nil
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, map[int64]outComment{}, "none", err
	}
	var asMap map[string]outComment
	mapErr := json.NewDecoder(f).Decode(&asMap)
	if mapErr == nil {
		items := make([]outComment, 0, len(asMap))
		for _, c := range asMap {
			items = append(items, c)
		}
		sortOutComments(items)
		return items, commentsToMap(items), "object", nil
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, map[int64]outComment{}, "none", err
	}
	var wrapper struct {
		Comments []outComment `json:"comments"`
	}
	wrapperErr := json.NewDecoder(f).Decode(&wrapper)
	if wrapperErr == nil && wrapper.Comments != nil {
		return wrapper.Comments, commentsToMap(wrapper.Comments), "comments-wrapper", nil
	}

	return nil, map[int64]outComment{}, "none", fmt.Errorf("cache decode failed: array=%v; object=%v; wrapper=%v", arrayErr, mapErr, wrapperErr)
}

func repliesHaveIDs(replies []outComment) bool {
	for _, r := range replies {
		if r.User != "" && r.UserID == 0 {
			return false
		}
	}
	return true
}

func cacheHasIDs(items []outComment) bool {
	for _, c := range items {
		if c.User != "" && c.UserID == 0 {
			return false
		}
		if !repliesHaveIDs(c.Replies) {
			return false
		}
	}
	return true
}

func buildOutput(studio string, top []apiComment, old map[int64]outComment) ([]outComment, int64, int64, error) {
	out := make([]outComment, len(top))
	jobs := make(chan int, replyConcurrency*4)
	errCh := make(chan error, 1)
	var reused, fetched, processed atomic.Int64
	var wg sync.WaitGroup
	started := time.Now()
	total := int64(len(top))

	logDone := func() {
		done := processed.Add(1)
		if done%progressEvery == 0 || done == total {
			fmt.Printf("[replies] processed=%d/%d reused=%d refreshed=%d elapsed=%s\n",
				done, total, reused.Load(), fetched.Load(), time.Since(started).Round(time.Second))
		}
	}

	fmt.Printf("[replies] start: route=direct comments=%d workers=%d\n", total, replyConcurrency)
	worker := func() {
		defer wg.Done()
		for i := range jobs {
			c := top[i]
			item := asOut(c)
			replies, err := fetchReplies(studio, c)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("replies for comment %d: %w", c.ID, err):
				default:
				}
				logDone()
				continue
			}
			item.Replies = replies
			out[i] = item
			fetched.Add(1)
			logDone()
		}
	}

	for i := 0; i < replyConcurrency; i++ {
		wg.Add(1)
		go worker()
	}

	for i, c := range top {
		item := asOut(c)
		if prev, ok := old[c.ID]; ok && len(prev.Replies) == c.ReplyCount && repliesHaveIDs(prev.Replies) {
			item.Replies = prev.Replies
			out[i] = item
			reused.Add(1)
			logDone()
			continue
		}
		if c.ReplyCount <= 0 {
			out[i] = item
			fetched.Add(1)
			logDone()
			continue
		}
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return nil, reused.Load(), fetched.Load(), err
	default:
	}
	return out, reused.Load(), fetched.Load(), nil
}

func sortOutComments(items []outComment) {
	sort.SliceStable(items, func(i, j int) bool {
		ti, errI := parseCommentTime(items[i].Datetime)
		tj, errJ := parseCommentTime(items[j].Datetime)
		if errI != nil || errJ != nil {
			return items[i].Datetime > items[j].Datetime
		}
		return ti.After(tj)
	})
}

func mergeCachedTail(fresh, old []outComment, cutoff, boundary time.Time) ([]outComment, int, error) {
	merged := make([]outComment, 0, len(old)+len(fresh))
	seen := make(map[int64]struct{}, len(old)+len(fresh))
	merged = append(merged, fresh...)
	for _, c := range fresh {
		seen[c.ID] = struct{}{}
	}

	added := 0
	for _, c := range old {
		if _, ok := seen[c.ID]; ok {
			continue
		}
		created, err := parseCommentTime(c.Datetime)
		if err != nil {
			return nil, 0, fmt.Errorf("cached comment %d time %q: %w", c.ID, c.Datetime, err)
		}
		if created.Before(cutoff) {
			continue
		}
		if created.After(boundary) {
			continue
		}
		seen[c.ID] = struct{}{}
		merged = append(merged, c)
		added++
	}

	sortOutComments(merged)
	return merged, added, nil
}

func writeJSON(path string, data []outComment) error {
	fmt.Printf("[write] writing %d comments to %s...\n", len(data), path)
	started := time.Now()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	fmt.Printf("[write] done in %s\n", time.Since(started).Round(time.Millisecond))
	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: %s <studio-id> <days> <output.json>\n", os.Args[0])
		os.Exit(2)
	}
	studio := os.Args[1]
	days, err := strconv.Atoi(os.Args[2])
	if err != nil || days <= 0 {
		fmt.Fprintln(os.Stderr, "days must be a positive integer")
		os.Exit(2)
	}
	output := os.Args[3]

	started := time.Now()
	fmt.Printf("[start] studio=%s days=%d output=%s\n", studio, days, output)
	fmt.Printf("[network] direct only; page_concurrency=%d page_lookahead=%d reply_workers=%d\n", pageConcurrency, pageLookahead, replyConcurrency)
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	oldItems, old, cacheFormat, cacheErr := loadOld(output)
	if cacheErr != nil {
		fmt.Printf("[cache] unusable: %v\n", cacheErr)
	} else {
		fmt.Printf("[cache] format=%s existing comments=%d\n", cacheFormat, len(old))
	}

	forceFull := os.Getenv("FULL_SCAN") == "1"
	cacheIDsOK := cacheErr == nil && cacheHasIDs(oldItems)
	incremental := cacheErr == nil && len(old) >= incrementalOverlap && cacheIDsOK && !forceFull
	if forceFull {
		fmt.Println("[cache] FULL_SCAN=1; incremental mode disabled")
	} else if len(old) < incrementalOverlap {
		fmt.Printf("[cache] too small for incremental mode (<%d comments); full scan\n", incrementalOverlap)
	} else if cacheErr == nil && !cacheIDsOK {
		fmt.Println("[cache] missing user_id in cache; full scan required before incremental mode")
	} else if incremental {
		fmt.Printf("[cache] incremental mode enabled; overlap=%d consecutive cached IDs\n", incrementalOverlap)
	}

	top, cacheBoundary, boundaryTime, err := fetchTopLevel(studio, cutoff, old, incremental)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("[pages] complete: fetched top-level comments=%d\n", len(top))

	data, reused, fetched, err := buildOutput(studio, top, old)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	cachedTail := 0
	if cacheBoundary {
		data, cachedTail, err = mergeCachedTail(data, oldItems, cutoff, boundaryTime)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("[cache] merged cached tail=%d final_comments=%d\n", cachedTail, len(data))
	}

	if err := writeJSON(output, data); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("[done] reused=%d refreshed=%d cached_tail=%d total_elapsed=%s\n",
		reused, fetched, cachedTail, time.Since(started).Round(time.Millisecond))
}
