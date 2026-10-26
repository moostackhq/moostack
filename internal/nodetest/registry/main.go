// Command registry is a read-only OCI registry for nodetest's tests
// only, served with TLS on :443 from -dir: each blob as
// blobs/sha256/<hex>, and each tag of a repository as
// <repository>/tags/<tag>, which holds the media type and the digest of
// its manifest or index, on one line. A repository with a file
// <repository>/fault answers every request for it as the file says:
// "404" as a registry that has no such manifest, "401" as Docker Hub
// does for a private or missing repository, with a challenge to fetch a
// token from /token and then insufficient_scope, and "429" as a
// registry that rate limits the caller. It prints each request as
// "<method> <path> <status>".
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// route is a request of the registry API for a repository's manifest or
// blob.
var route = regexp.MustCompile(`^/v2/(.+)/(manifests|blobs)/([^/]+)$`)

// digest is a reference that names a blob by its digest.
var digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func main() {
	dir := flag.String("dir", "", "the directory of the blobs and tags")
	cert := flag.String("cert", "", "the server's certificate, PEM")
	key := flag.String("key", "", "the server's key, PEM")
	flag.Parse()
	r := registry{dir: *dir}
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	log.Fatal(http.ListenAndServeTLS(":443", *cert, *key, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		r.serve(rec, req)
		log.Printf("%s %s %d", req.Method, req.URL.Path, rec.status)
	})))
}

// recorder keeps the status of the response it writes.
type recorder struct {
	http.ResponseWriter
	status int
}

func (r *recorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

type registry struct{ dir string }

func (r registry) serve(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/token":
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"token":"test-token","access_token":"test-token","expires_in":300}`)
		return
	case "/v2/":
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		fmt.Fprint(w, "{}")
		return
	}
	m := route.FindStringSubmatch(req.URL.Path)
	if m == nil {
		answer(w, http.StatusNotFound, "NAME_UNKNOWN", "no such route")
		return
	}
	repo, kind, ref := m[1], m[2], m[3]
	if fault, err := os.ReadFile(filepath.Join(r.dir, repo, "fault")); err == nil {
		r.fail(w, req, repo, strings.TrimSpace(string(fault)))
		return
	}
	mediaType, dgst := "application/octet-stream", ref
	if kind == "manifests" && !digest.MatchString(ref) {
		tag, err := os.ReadFile(filepath.Join(r.dir, repo, "tags", ref))
		if err != nil {
			answer(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "unknown tag="+ref)
			return
		}
		mediaType, dgst, _ = strings.Cut(strings.TrimSpace(string(tag)), " ")
	}
	if !digest.MatchString(dgst) {
		answer(w, http.StatusBadRequest, "DIGEST_INVALID", "invalid digest "+dgst)
		return
	}
	data, err := os.ReadFile(filepath.Join(r.dir, "blobs", "sha256", strings.TrimPrefix(dgst, "sha256:")))
	if err != nil {
		answer(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob "+dgst)
		return
	}
	if kind == "manifests" && digest.MatchString(ref) {
		var manifest struct{ MediaType string }
		if json.Unmarshal(data, &manifest) == nil && manifest.MediaType != "" {
			mediaType = manifest.MediaType
		}
	}
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", dgst)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	if req.Method != http.MethodHead {
		w.Write(data)
	}
}

// fail answers a request for repo as fault says.
func (r registry) fail(w http.ResponseWriter, req *http.Request, repo, fault string) {
	switch fault {
	case "404":
		answer(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown")
	case "401":
		challenge := fmt.Sprintf(`Bearer realm="https://%s/token",service="%s",scope="repository:%s:pull"`, req.Host, req.Host, repo)
		if req.Header.Get("Authorization") != "" {
			challenge += `,error="insufficient_scope"`
		}
		w.Header().Set("Www-Authenticate", challenge)
		answer(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
	case "429":
		answer(w, http.StatusTooManyRequests, "TOOMANYREQUESTS", "You have reached your unauthenticated pull rate limit.")
	default:
		answer(w, http.StatusInternalServerError, "UNKNOWN", "unknown fault "+fault)
	}
}

// answer answers with status and a registry error of code.
func answer(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"errors":[{"code":%q,"message":%q}]}`, code, message)
}
