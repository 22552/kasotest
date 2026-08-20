package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	apiBase          = "https://api.scratch.mit.edu"
	pageLimit        = 40
	pageConcurrency  = 9
	replyConcurrency = 24
	maxAttempts      = 6
	maxSafetyPages   = 100000
	progressEvery    = 100
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

func getJSON(rawURL string, dst any) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, err := http.NewRequest(http.MethodGet, rawURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "kasotest-genko-scraper/8")

		resp, err := directClient.Do(req)
		if err != nil {
			lastErr = err
			wait := backoff(nil, attempt)
			fmt.Printf("[http] direct attempt=%d/%d error=%v; retry in %s\n", attempt+1, maxAttempts, err, wait)
			time.Sleep(wait)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			err = json.NewDecoder(resp.Body).Decode(dst)
			resp.Body.Close()
			if err == nil {
				return nil
			}
			lastErr = fmt.Errorf("decode: %w", err)
			wait := backoff(resp, attempt)
			fmt.Printf("[http] direct invalid JSON attempt=%d/%d; retry in %s\n", attempt+1, maxAttempts, wait)
			time.Sleep(wait)
			continue
		}

		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("GET %s: HTTP %d", rawURL, resp.StatusCode)
			wait := backoff(resp, attempt)
			fmt.Printf("[http] direct HTTP %d attempt=%d/%d; retry in %s\n", resp.StatusCode, attempt+1, maxAttempts, wait)
			time.Sleep(wait)
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

type pageResult struct {
	page int
	data []apiComment
	err  error
}

func fetchTopLevel(studio string, cutoff time.Time) ([]apiComment, error) {
	all := make([]apiComment, 0, 8192)
	seen := make(map[int64]struct{}, 8192)
	started := time.Now()

	fmt.Printf("[pages] start: route=direct concurrency=%d limit=%d cutoff=%s\n", pageConcurrency, pageLimit, cutoff.Format(time.RFC3339))
	for base := 0; base < maxSafetyPages; base += pageConcurrency {
		end := base + pageConcurrency
		if end > maxSafetyPages {
			end = maxSafetyPages
		}

		ch := make(chan pageResult, end-base)
		var wg sync.WaitGroup
		for p := base; p < end; p++ {
			wg.Add(1)
			go func(page int) {
				defer wg.Done()
				var data []apiComment
				err := getJSON(commentsURL(studio, page*pageLimit), &data)
				ch <- pageResult{page: page, data: data, err: err}
			}(p)
		}
		wg.Wait()
		close(ch)

		pages := make(map[int][]apiComment, end-base)
		for r := range ch {
			if r.err != nil {
				return nil, fmt.Errorf("comments page %d: %w", r.page, r.err)
			}
			pages[r.page] = r.data
		}

		stop := false
		oldest := ""
		actualPages := base
		for p := base; p < end; p++ {
			data := pages[p]
			actualPages = p + 1
			if len(data) == 0 {
				stop = true
				break
			}
			for _, c := range data {
				created, err := parseCommentTime(c.DatetimeCreated)
				if err != nil {
					return nil, fmt.Errorf("comment %d time %q: %w", c.ID, c.DatetimeCreated, err)
				}
				oldest = created.UTC().Format(time.RFC3339)
				if created.Before(cutoff) {
					stop = true
					break
				}
				if _, ok := seen[c.ID]; ok {
					continue
				}
				seen[c.ID] = struct{}{}
				all = append(all, c)
			}
			if stop || len(data) < pageLimit {
				stop = true
				break
			}
		}

		fmt.Printf("[pages] fetched=%d pages comments=%d oldest=%s elapsed=%s\n", actualPages, len(all), oldest, time.Since(started).Round(time.Second))
		if stop {
			fmt.Printf("[pages] stop: reached cutoff/end after %d pages\n", actualPages)
			break
		}
	}
	return all, nil
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

func loadOld(path string) (map[int64]outComment, string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[int64]outComment{}, "none", nil
		}
		return map[int64]outComment{}, "none", err
	}
	defer f.Close()

	var asArray []outComment
	arrayErr := json.NewDecoder(f).Decode(&asArray)
	if arrayErr == nil {
		return commentsToMap(asArray), "array", nil
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return map[int64]outComment{}, "none", err
	}
	var asMap map[string]outComment
	mapErr := json.NewDecoder(f).Decode(&asMap)
	if mapErr == nil {
		m := make(map[int64]outComment, len(asMap))
		for _, c := range asMap {
			m[c.ID] = c
		}
		return m, "object", nil
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return map[int64]outComment{}, "none", err
	}
	var wrapper struct {
		Comments []outComment `json:"comments"`
	}
	wrapperErr := json.NewDecoder(f).Decode(&wrapper)
	if wrapperErr == nil && wrapper.Comments != nil {
		return commentsToMap(wrapper.Comments), "comments-wrapper", nil
	}

	return map[int64]outComment{}, "none", fmt.Errorf("cache decode failed: array=%v; object=%v; wrapper=%v", arrayErr, mapErr, wrapperErr)
}

func repliesHaveIDs(replies []outComment) bool {
	for _, r := range replies {
		if r.User != "" && r.UserID == 0 {
			return false
		}
	}
	return true
}

func buildOutput(studio string, top []apiComment, old map[int64]outComment) ([]outComment, int64, int64, error) {
	out := make([]outComment, len(top))
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var reused, fetched, processed atomic.Int64
	var wg sync.WaitGroup
	started := time.Now()
	total := int64(len(top))

	fmt.Printf("[replies] start: route=direct comments=%d workers=%d\n", total, replyConcurrency)
	worker := func() {
		defer wg.Done()
		for i := range jobs {
			c := top[i]
			item := asOut(c)
			if prev, ok := old[c.ID]; ok && len(prev.Replies) == c.ReplyCount && repliesHaveIDs(prev.Replies) {
				item.Replies = prev.Replies
				out[i] = item
				reused.Add(1)
			} else {
				replies, err := fetchReplies(studio, c)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("replies for comment %d: %w", c.ID, err):
					default:
					}
					continue
				}
				item.Replies = replies
				out[i] = item
				fetched.Add(1)
			}

			done := processed.Add(1)
			if done%progressEvery == 0 || done == total {
				fmt.Printf("[replies] processed=%d/%d reused=%d refreshed=%d elapsed=%s\n", done, total, reused.Load(), fetched.Load(), time.Since(started).Round(time.Second))
			}
		}
	}

	for i := 0; i < replyConcurrency; i++ {
		wg.Add(1)
		go worker()
	}
	for i := range top {
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
	fmt.Printf("[network] direct only; page_concurrency=%d reply_workers=%d\n", pageConcurrency, replyConcurrency)
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	old, cacheFormat, cacheErr := loadOld(output)
	if cacheErr != nil {
		fmt.Printf("[cache] unusable: %v\n", cacheErr)
	} else {
		fmt.Printf("[cache] format=%s existing comments=%d\n", cacheFormat, len(old))
	}

	top, err := fetchTopLevel(studio, cutoff)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("[pages] complete: top-level comments in range=%d\n", len(top))

	data, reused, fetched, err := buildOutput(studio, top, old)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeJSON(output, data); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("[done] reused=%d refreshed=%d total_elapsed=%s\n", reused, fetched, time.Since(started).Round(time.Millisecond))
}
