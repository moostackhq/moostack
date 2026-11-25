package deployfile_test

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/deployfile"
)

// appDir is an app directory with a Dockerfile, a deploy/ of manifests,
// a chart/ and the deploy file content.
func appDir(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range map[string]string{
		"Dockerfile":        "FROM scratch\n",
		"deploy/app.yaml":   "kind: ConfigMap\n",
		"chart/Chart.yaml":  "name: web\n",
		deployfile.FileName: content,
	} {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestReadContainers(t *testing.T) {
	dir := appDir(t, `version: 1
name: shop
cluster: acme/prod
containers:
  web:
    build: {dockerfile: .}
    port: 8080
    host: &host shop.example.com
    replicas: 2
    env:
      HOST: *host
      WORKERS: 4
      DEBUG: true
      EMPTY: ""
      DB_URL: {secret: DB_URL}
      DATABASE: {secret: DB_URL}
  jobs:
    build: {railpack: .}
    command: [bin/jobs, --queue, mail]
  cache:
    image: valkey/valkey:8
    port: 6379
secrets: [GREETING, DB_URL]
`)
	app, err := deployfile.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if app.Cluster != "acme/prod" || app.Name != "shop" || app.Manifests != "" || app.Chart != "" || app.Dir != dir {
		t.Errorf("app = %+v", app)
	}
	if strings.Join(app.Secrets, ",") != "GREETING,DB_URL" {
		t.Errorf("secrets = %v", app.Secrets)
	}
	if len(app.Containers) != 3 {
		t.Fatalf("containers = %+v", app.Containers)
	}
	cache, jobs, web := app.Containers[0], app.Containers[1], app.Containers[2]
	if cache.Name != "cache" || cache.Image != "valkey/valkey:8" || cache.Build != nil || cache.Port != 6379 || cache.Host != "" || cache.Replicas != 1 || cache.Command != nil {
		t.Errorf("cache = %+v", cache)
	}
	if jobs.Name != "jobs" || jobs.Image != "" || jobs.Build == nil || jobs.Build.Railpack != dir || jobs.Port != 0 || jobs.Replicas != 1 || strings.Join(jobs.Command, " ") != "bin/jobs --queue mail" {
		t.Errorf("jobs = %+v", jobs)
	}
	if web.Name != "web" || web.Build == nil || web.Build.Dockerfile != dir || web.Port != 8080 || web.Host != "shop.example.com" || web.Replicas != 2 {
		t.Errorf("web = %+v", web)
	}
	env := map[string]deployfile.Value{
		"HOST": {Value: "shop.example.com"}, "WORKERS": {Value: "4"}, "DEBUG": {Value: "true"}, "EMPTY": {},
		"DB_URL": {Secret: "DB_URL"}, "DATABASE": {Secret: "DB_URL"},
	}
	if !maps.Equal(web.Env, env) || cache.Env != nil {
		t.Errorf("env = %+v and %+v, want %+v and none", web.Env, cache.Env, env)
	}
}

// drupal is the deploy file of Drupal with MariaDB.
const drupal = `version: 1
name: drupal
cluster: playground
secrets:
  - DB_PASSWORD
  - HASH_SALT
  - ADMIN_PASSWORD
containers:
  web:
    build:
      dockerfile: .
    port: 80
    host: &host drupal.example.com
    rolling: true
    healthcheck:
      path: /user/login
    env:
      DRUPAL_HOST: *host
      DB_PASSWORD: {secret: DB_PASSWORD}
      HASH_SALT: {secret: HASH_SALT}
      ADMIN_PASSWORD: {secret: ADMIN_PASSWORD}
    volumes:
      files: /opt/drupal/web/sites/default/files
    pre_start:
      command: [drupal-install]
      user: "33:33"
    cron:
      drupal:
        schedule: "*/5  * * * *"
        command: [drush, cron]
        user: 33
  db:
    image: mariadb:11.4
    port: 3306
    stop_delay: 0
    healthcheck:
      command: [healthcheck.sh, --connect, --innodb_initialized]
    env:
      MARIADB_DATABASE: drupal
      MARIADB_USER: drupal
      MARIADB_PASSWORD: {secret: DB_PASSWORD}
      MARIADB_RANDOM_ROOT_PASSWORD: "1"
    volumes:
      data: /var/lib/mysql
      logs: /var/log/mysql
`

func TestReadDrupal(t *testing.T) {
	dir := appDir(t, drupal)
	app, err := deployfile.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []deployfile.Container{
		{
			Name: "db", Image: "mariadb:11.4", Port: 3306, Replicas: 1,
			Env: map[string]deployfile.Value{
				"MARIADB_DATABASE": {Value: "drupal"}, "MARIADB_USER": {Value: "drupal"},
				"MARIADB_PASSWORD": {Secret: "DB_PASSWORD"}, "MARIADB_RANDOM_ROOT_PASSWORD": {Value: "1"},
			},
			Volumes:     []deployfile.Volume{{Name: "data", Path: "/var/lib/mysql"}, {Name: "logs", Path: "/var/log/mysql"}},
			Healthcheck: &deployfile.Healthcheck{Command: []string{"healthcheck.sh", "--connect", "--innodb_initialized"}},
		},
		{
			Name: "web", Build: &deployfile.Build{Dockerfile: dir}, Port: 80, Host: "drupal.example.com", Replicas: 1,
			Env: map[string]deployfile.Value{
				"DRUPAL_HOST": {Value: "drupal.example.com"}, "DB_PASSWORD": {Secret: "DB_PASSWORD"},
				"HASH_SALT": {Secret: "HASH_SALT"}, "ADMIN_PASSWORD": {Secret: "ADMIN_PASSWORD"},
			},
			Volumes:     []deployfile.Volume{{Name: "files", Path: "/opt/drupal/web/sites/default/files"}},
			PreStart:    &deployfile.Command{Command: []string{"drupal-install"}, User: "33:33"},
			Cron:        []deployfile.Cron{{Name: "drupal", Schedule: "*/5 * * * *", Command: deployfile.Command{Command: []string{"drush", "cron"}, User: "33"}}},
			Healthcheck: &deployfile.Healthcheck{Path: "/user/login"},
			Rolling:     true,
			StopDelay:   5,
		},
	}
	if !reflect.DeepEqual(app.Containers, want) {
		t.Errorf("containers =\n%+v, want\n%+v", app.Containers, want)
	}
}

func TestAStopDelayIsFiveSecondsWithAPortAndNoneWithoutUnlessGiven(t *testing.T) {
	for container, want := range map[string]int{
		"{image: i, port: 80}":                5,
		"{image: i}":                          0,
		"{image: i, port: 80, stop_delay: 0}": 0,
		"{image: i, port: 80, stop_delay: 9}": 9,
		"{image: i, stop_delay: 3}":           3,
	} {
		app, err := deployfile.Read(appDir(t, "name: shop\ncontainers: {web: "+container+"}\n"))
		if err != nil || app.Containers[0].StopDelay != want {
			t.Errorf("%s: %v, a stop delay of %d, want %d", container, err, app.Containers[0].StopDelay, want)
		}
	}
}

func TestReadManifestsAndChart(t *testing.T) {
	dir := appDir(t, "name: web\nmanifests: deploy\ncontainers: {web: {build: {railpack: .}}, jobs: {build: {dockerfile: .}}}\nsecrets: [TOKEN]\n")
	app, err := deployfile.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []deployfile.Container{{Name: "jobs", Build: &deployfile.Build{Dockerfile: dir}}, {Name: "web", Build: &deployfile.Build{Railpack: dir}}}
	if app.Cluster != "" || app.Manifests != filepath.Join(dir, "deploy") || !reflect.DeepEqual(app.Containers, want) || !slices.Equal(app.Secrets, []string{"TOKEN"}) {
		t.Errorf("app = %+v", app)
	}

	dir = appDir(t, "name: web\nchart: chart\n")
	app, err = deployfile.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if app.Chart != filepath.Join(dir, "chart") || app.Containers != nil {
		t.Errorf("app = %+v", app)
	}

	dir = appDir(t, "name: web\nchart: chart\ncontainers: {web: {build: {dockerfile: .}}}\n")
	if app, err = deployfile.Read(dir); err != nil || len(app.Containers) != 1 || app.Containers[0].Build.Dockerfile != dir {
		t.Errorf("a chart with a build: %+v, %v", app, err)
	}
}

func TestAFileWithoutAVersionIsReadAsTheLatest(t *testing.T) {
	const rest = "name: web\nchart: chart\n"
	stated, err := deployfile.Read(appDir(t, fmt.Sprintf("version: %d\n", deployfile.Version)+rest))
	if err != nil {
		t.Fatal(err)
	}
	without, err := deployfile.Read(appDir(t, rest))
	if err != nil {
		t.Fatal(err)
	}
	stated.Dir, stated.Chart, without.Dir, without.Chart = "", "", "", ""
	if !reflect.DeepEqual(stated, without) {
		t.Errorf("with the version %+v, without %+v", stated, without)
	}
}

func TestAVersionThatIsntTheLatestIsRefusedBeforeAnythingElse(t *testing.T) {
	for version, want := range map[string]string{
		"2":      "version: 2 isn't a version this mooctl reads",
		"0":      "version: 0 isn't a version this mooctl reads",
		"-1":     "version: -1 isn't a version this mooctl reads",
		`"1"`:    "version: give a whole number, such as 1",
		"1.0":    "version: give a whole number, such as 1",
		"one":    "version: give a whole number, such as 1",
		"[1]":    "version: give a whole number, such as 1",
		"{a: 1}": "version: give a whole number, such as 1",
	} {
		dir := appDir(t, "name: Not A Name\nservices: {of: another version}\nversion: "+version+"\n")
		if _, err := deployfile.Read(dir); err == nil || !strings.HasSuffix(err.Error(), ": "+want) {
			t.Errorf("Read of version %s: %v, want %q", version, err, want)
		}
		if _, _, err := deployfile.ReadRef(dir); err == nil || !strings.HasSuffix(err.Error(), ": "+want) {
			t.Errorf("ReadRef of version %s: %v, want %q", version, err, want)
		}
	}
}

func TestReadRefuses(t *testing.T) {
	const web = "name: shop\ncontainers: {web: {build: {dockerfile: .}, port: 80"
	const only = "name: shop\ncontainers: {web: {build: {dockerfile: .}"
	for name, tc := range map[string]struct{ content, want string }{
		"no name":               {"chart: chart\n", "name:"},
		"bad name":              {"name: Web\nchart: chart\n", "name:"},
		"bad cluster":           {"cluster: a/b/c\nname: web\nchart: chart\n", "cluster:"},
		"a chart's build":       {"name: web\nbuild: {dockerfile: .}\nchart: chart\n", "build: each container gives its own"},
		"an empty build":        {"name: web\nbuild: {}\nchart: chart\n", "build: each container gives its own"},
		"manifests' build":      {"name: web\nbuild: {railpack: .}\nmanifests: deploy\n", "build: each container gives its own"},
		"none of the three":     {"name: web\n", "give containers, manifests or a chart"},
		"only secrets":          {"name: web\nsecrets: [A]\n", "give containers, manifests or a chart"},
		"chart and manifests":   {"name: web\nchart: chart\nmanifests: deploy\n", "give one of manifests and chart"},
		"all three":             {only + "}}\nchart: chart\nmanifests: deploy\n", "give one of manifests and chart"},
		"no containers, chart":  {"name: shop\ncontainers: {}\nchart: chart\n", "containers: give at least one"},
		"only built: empty":     {"name: shop\nchart: chart\ncontainers: {web: }\n", "containers: web: give its build"},
		"only built: no build":  {"name: shop\nmanifests: deploy\ncontainers: {web: {}}\n", "containers: web: give its build"},
		"only built: image":     {"name: shop\nchart: chart\ncontainers: {web: {image: nginx}}\n", "containers: web: give its build"},
		"only built: and image": {only + ", image: nginx}}\nchart: chart\n", "containers: web: image: a container beside manifests or a chart is only built"},
		"only built: command":   {only + ", command: [a]}}\nmanifests: deploy\n", "containers: web: command: a container beside manifests or a chart is only built"},
		"only built: port":      {only + ", port: 80}}\nmanifests: deploy\n", "containers: web: port: a container beside manifests or a chart is only built"},
		"only built: host":      {only + ", host: a.example.com}}\nchart: chart\n", "containers: web: host: a container beside manifests or a chart is only built"},
		"only built: replicas":  {only + ", replicas: 1}}\nchart: chart\n", "containers: web: replicas: a container beside manifests or a chart is only built"},
		"only built: env":       {only + ", env: {A: b}}}\nchart: chart\n", "containers: web: env: a container beside manifests or a chart is only built"},
		"only built: builds":    {"name: shop\nchart: chart\ncontainers: {web: {build: {}}}\n", "containers: web: build: give one of dockerfile and railpack"},
		"only built: its name":  {"name: shop\nchart: chart\ncontainers: {Web: {build: {dockerfile: .}}}\n", "containers:"},
		"env: a name":           {web + ", env: {A-B: c}}}\n", `containers: web: env: "A-B" isn't a name an environment variable can have`},
		"env: no value":         {web + ", env: {A: }}}\n", "containers: web: env: A: give a value, or {secret: NAME}"},
		"env: a list":           {web + ", env: {A: [b]}}}\n", "containers: web: env: A: give a value, or {secret: NAME}"},
		"env: another key":      {web + ", env: {A: {value: b}}}}\n", "containers: web: env: A: give a value, or {secret: NAME}"},
		"env: two keys":         {web + ", env: {A: {secret: B, name: C}}}}\nsecrets: [B]\n", "containers: web: env: A: give a value, or {secret: NAME}"},
		"env: an empty secret":  {web + ", env: {A: {secret: ''}}}}\n", "containers: web: env: A: give a value, or {secret: NAME}"},
		"env: a secret's list":  {web + ", env: {A: {secret: [B]}}}}\nsecrets: [B]\n", "containers: web: env: A: give a value, or {secret: NAME}"},
		"env: an unlisted one":  {web + ", env: {A: {secret: B}}}}\nsecrets: [C]\n", "containers: web: env: A: the secret B isn't in secrets"},
		"env: no secrets":       {web + ", env: {A: {secret: B}}}}\n", "containers: web: env: A: the secret B isn't in secrets"},
		"only built: volumes":   {only + ", volumes: {a: /a}}}\nchart: chart\n", "containers: web: volumes: a container beside manifests or a chart is only built"},
		"only built: pre_start": {only + ", pre_start: {command: [a]}}}\nchart: chart\n", "containers: web: pre_start: a container beside manifests or a chart is only built"},
		"only built: cron":      {only + ", cron: {a: {schedule: '* * * * *', command: [a]}}}}\nchart: chart\n", "containers: web: cron: a container beside manifests or a chart is only built"},
		"only built: a check":   {only + ", healthcheck: {command: [a]}}}\nchart: chart\n", "containers: web: healthcheck: a container beside manifests or a chart is only built"},
		"only built: rolling":   {only + ", rolling: false}}\nchart: chart\n", "containers: web: rolling: a container beside manifests or a chart is only built"},
		"only built: the delay": {only + ", stop_delay: 0}}\nchart: chart\n", "containers: web: stop_delay: a container beside manifests or a chart is only built"},
		"volume: its name":      {web + ", volumes: {Data: /data}}}\n", "containers: web: volumes:"},
		"volume: relative":      {web + ", volumes: {data: data}}}\n", "containers: web: volumes: data: give the directory's absolute path in the container"},
		"volume: the root":      {web + ", volumes: {data: /}}}\n", "containers: web: volumes: data: give the directory's absolute path in the container"},
		"volume: not clean":     {web + ", volumes: {data: /a/../b}}}\n", "containers: web: volumes: data: give the directory's absolute path in the container"},
		"volume: a slash after": {web + ", volumes: {data: /a/}}}\n", "containers: web: volumes: data: give the directory's absolute path in the container"},
		"volume: no path":       {web + ", volumes: {data: }}}\n", "containers: web: volumes: data: give the directory's absolute path in the container"},
		"volume: one path":      {web + ", volumes: {a: /data, b: /data}}}\n", "containers: web: volumes: b: /data is a's already"},
		"volume: a size":        {web + ", volumes: {data: {path: /data, size: 5Gi}}}}\n", "cannot unmarshal !!map into string"},
		"volume: two replicas":  {web + ", volumes: {data: /data}, replicas: 2}}\n", "containers: web: replicas: a container with volumes runs as one replica"},
		"rolling: no volumes":   {web + ", rolling: true}}\n", "containers: web: rolling: only a container with volumes takes it"},
		"rolling: false, none":  {web + ", rolling: false}}\n", "containers: web: rolling: only a container with volumes takes it"},
		"rolling: not a bool":   {web + ", volumes: {data: /data}, rolling: sometimes}}\n", "cannot unmarshal !!str `sometimes` into bool"},
		"pre_start: a list":     {web + ", pre_start: [a]}}\n", "cannot unmarshal !!seq into deployfile.command"},
		"pre_start: no command": {web + ", pre_start: {user: '33'}}}\n", "containers: web: pre_start: command: give the program to run, and its arguments"},
		"pre_start: empty":      {web + ", pre_start: {command: []}}}\n", "containers: web: pre_start: command: give the program to run, and its arguments"},
		"pre_start: a name":     {web + ", pre_start: {command: [a], user: www-data}}}\n", `containers: web: pre_start: user: "www-data" isn't a user's ID`},
		"pre_start: two colons": {web + ", pre_start: {command: [a], user: '33:33:33'}}}\n", "containers: web: pre_start: user:"},
		"pre_start: negative":   {web + ", pre_start: {command: [a], user: -1}}}\n", "containers: web: pre_start: user:"},
		"pre_start: a field":    {web + ", pre_start: {command: [a], timeout: 5}}}\n", "timeout"},
		"cron: its name":        {web + ", cron: {Nightly: {schedule: '0 3 * * *', command: [a]}}}}\n", "containers: web: cron:"},
		"cron: empty":           {web + ", cron: {nightly: }}}\n", "containers: web: cron: nightly: give its schedule and its command"},
		"cron: no schedule":     {web + ", cron: {nightly: {command: [a]}}}}\n", "containers: web: cron: nightly: schedule: give a crontab expression of five fields"},
		"cron: four fields":     {web + ", cron: {nightly: {schedule: '0 3 * *', command: [a]}}}}\n", "containers: web: cron: nightly: schedule: give a crontab expression of five fields"},
		"cron: six fields":      {web + ", cron: {nightly: {schedule: '0 0 3 * * *', command: [a]}}}}\n", "containers: web: cron: nightly: schedule: give a crontab expression of five fields"},
		"cron: a macro":         {web + ", cron: {nightly: {schedule: '@daily', command: [a]}}}}\n", "containers: web: cron: nightly: schedule: give a crontab expression of five fields"},
		"cron: a field's sign":  {web + ", cron: {nightly: {schedule: '0 3 * * $', command: [a]}}}}\n", "containers: web: cron: nightly: schedule: give a crontab expression of five fields"},
		"cron: no command":      {web + ", cron: {nightly: {schedule: '0 3 * * *'}}}}\n", "containers: web: cron: nightly: command: give the program to run, and its arguments"},
		"cron: a user's name":   {web + ", cron: {nightly: {schedule: '0 3 * * *', command: [a], user: root}}}}\n", "containers: web: cron: nightly: user:"},
		"cron: a field":         {web + ", cron: {nightly: {schedule: '0 3 * * *', command: [a], timezone: UTC}}}}\n", "timezone"},
		"check: a bare path":    {web + ", healthcheck: /up}}\n", "cannot unmarshal !!str `/up` into struct"},
		"check: neither":        {web + ", healthcheck: {}}}\n", "containers: web: healthcheck: give one of path and command"},
		"check: both":           {web + ", healthcheck: {path: /up, command: [a]}}}\n", "containers: web: healthcheck: give one of path and command"},
		"check: no slash":       {web + ", healthcheck: {path: up}}}\n", `containers: web: healthcheck: path: "up" doesn't start with a slash`},
		"check: no port":        {"name: shop\ncontainers: {web: {image: i, healthcheck: {path: /up}}}\n", "containers: web: healthcheck: path: give the port the container serves HTTP on too"},
		"check: empty command":  {web + ", healthcheck: {command: []}}}\n", "containers: web: healthcheck: command: give the program to run, and its arguments"},
		"check: a field":        {web + ", healthcheck: {path: /up, interval: 5}}}\n", "interval"},
		"stop_delay: negative":  {web + ", stop_delay: -1}}\n", "containers: web: stop_delay: can't be negative"},
		"env: not a map":        {web + ", env: [A]}}\n", "env"},
		"the old objects key":   {"name: web\nobjects: {chart: chart}\n", "objects"},
		"no containers":         {"name: shop\ncontainers: {}\n", "containers: give at least one"},
		"a top-level build":     {web + "}}\nbuild: {dockerfile: .}\n", "build: each container gives its own"},
		"an empty container":    {"name: shop\ncontainers: {web: }\n", "containers: web: give one of build and image"},
		"no build nor image":    {"name: shop\ncontainers: {web: {port: 80}}\n", "containers: web: give one of build and image"},
		"a build and an image":  {web + ", image: nginx}}\n", "containers: web: give one of build and image"},
		"a container's builds":  {"name: shop\ncontainers: {web: {build: {dockerfile: ., railpack: .}}}\n", "containers: web: build: give one of dockerfile and railpack"},
		"a build's directory":   {"name: shop\ncontainers: {web: {build: {dockerfile: deploy}}}\n", "containers: web: build: dockerfile:"},
		"an image with a space": {"name: shop\ncontainers: {web: {image: 'nginx latest'}}\n", "containers: web: image:"},
		"a container's name":    {"name: shop\ncontainers: {Web: {image: nginx}}\n", "containers:"},
		"a name with a digit":   {"name: shop\ncontainers: {1web: {image: nginx}}\n", "must start with a letter"},
		"an empty command":      {web + ", command: []}}\n", "containers: web: command:"},
		"an empty program":      {web + ", command: ['', -v]}}\n", "containers: web: command:"},
		"port zero":             {"name: shop\ncontainers: {web: {image: nginx, port: 0}}\n", "containers: web: port:"},
		"a port too large":      {"name: shop\ncontainers: {web: {image: nginx, port: 65536}}\n", "containers: web: port:"},
		"bad host":              {web + ", host: 'a b'}}\n", "containers: web: host:"},
		"a host without a port": {"name: shop\ncontainers: {web: {image: nginx, host: a.example.com}}\n", "containers: web: host: give the port"},
		"one host twice":        {"name: shop\ncontainers: {api: {image: a, port: 80, host: a.example.com}, web: {image: b, port: 80, host: a.example.com}}\n", "containers: web: host: a.example.com is api's already"},
		"negative replicas":     {web + ", replicas: -1}}\n", "containers: web: replicas:"},
		"no Chart.yaml":         {"name: web\nchart: deploy\n", "has no Chart.yaml"},
		"missing manifests":     {"name: web\nmanifests: nowhere\n", "manifests:"},
		"secret name":           {"name: web\nchart: chart\nsecrets: [db-url]\n", "secrets:"},
		"secret twice":          {"name: web\nchart: chart\nsecrets: [A, A]\n", "secrets:"},
		"unknown field":         {"name: web\nchart: chart\nport: 80\n", "port"},
		"a container's field":   {web + ", ports: [80]}}\n", "ports"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := deployfile.Read(appDir(t, tc.content))
			if err == nil {
				t.Fatal("taken")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%v, which doesn't say %q", err, tc.want)
			}
		})
	}
}

func TestANameIsCheckedOnlyAsADNSLabelThatStartsWithALetter(t *testing.T) {
	long := strings.Repeat("a", 63)
	for _, name := range []string{"a", "web", "web-2", long, "default", "kube-system", "kube-web", "cert-manager", "web-tls"} {
		if err := deployfile.CheckName(name); err != nil {
			t.Errorf("CheckName(%q): %v", name, err)
		}
		for file, content := range map[string]string{
			"a chart":               "name: " + name + "\nchart: chart\n",
			"a host and secrets":    "name: " + name + "\ncontainers: {web: {image: nginx, port: 80, host: a.example.com}}\nsecrets: [A]\n",
			"a container so named":  "name: shop\ncontainers: {" + name + ": {image: nginx}}\n",
			"manifests, in ReadRef": "name: " + name + "\nmanifests: deploy\n",
		} {
			dir := appDir(t, content)
			if _, err := deployfile.Read(dir); err != nil {
				t.Errorf("%s, named %q: %v", file, name, err)
			}
			if _, got, err := deployfile.ReadRef(dir); err != nil || file != "a container so named" && got != name {
				t.Errorf("ReadRef of %s, named %q: %q, %v", file, name, got, err)
			}
		}
	}
	for _, name := range []string{"", "1web", "-web", "web-", "Web", "we_b", "we.b", "web/api", long + "a"} {
		if err := deployfile.CheckName(name); err == nil {
			t.Errorf("CheckName took %q", name)
		}
	}
}

func TestReadTakes(t *testing.T) {
	for name, content := range map[string]string{
		"an app named as its certificate's secret, without secrets": "name: web-tls\ncontainers: {web: {image: nginx, port: 80, host: a.example.com}}\n",
		"a container without a port":                                "name: shop\ncontainers: {jobs: {image: nginx}}\n",
		"two hosts":                                                 "name: shop\ncontainers: {api: {image: a, port: 80, host: a.example.com}, web: {image: b, port: 80, host: b.example.com}}\n",
		"replicas of zero as one":                                   "name: shop\ncontainers: {web: {image: nginx, replicas: 0}}\n",
		"a listed secret that no container names":                   "name: shop\ncontainers: {web: {image: nginx}}\nsecrets: [A]\n",
		"a command as the check, without a port":                    "name: shop\ncontainers: {web: {image: nginx, healthcheck: {command: [a]}}}\n",
		"rolling: false with volumes":                               "name: shop\ncontainers: {web: {image: nginx, volumes: {a: /a}, rolling: false}}\n",
		"one replica with volumes":                                  "name: shop\ncontainers: {web: {image: nginx, volumes: {a: /a}, replicas: 1}}\n",
		"an empty env":                                              "name: shop\ncontainers: {web: {image: nginx, env: {}}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			app, err := deployfile.Read(appDir(t, content))
			if err != nil {
				t.Fatal(err)
			}
			if app.Containers[len(app.Containers)-1].Replicas != 1 {
				t.Errorf("replicas = %d", app.Containers[0].Replicas)
			}
		})
	}
}

func TestNamesAndAHostsLabelsAreDNSLabels(t *testing.T) {
	for name, content := range map[string]string{
		"leading digit":           "name: 1web\nchart: chart\n",
		"host label of 64":        "name: web\ncontainers: {web: {image: nginx, port: 80, host: " + strings.Repeat("a", 64) + ".example.com}}\n",
		"container name of 64":    "name: web\ncontainers: {" + strings.Repeat("a", 64) + ": {image: nginx}}\n",
		"container name's hyphen": "name: web\ncontainers: {web-: {image: nginx}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := deployfile.Read(appDir(t, content)); err == nil {
				t.Error("taken")
			}
		})
	}
	for name, content := range map[string]string{
		"app name of 63":       "name: " + strings.Repeat("a", 63) + "\ncontainers: {web: {image: nginx, port: 80}}\n",
		"container name of 63": "name: web\ncontainers: {" + strings.Repeat("a", 63) + ": {image: nginx}}\n",
		"host label of 63":     "name: web\ncontainers: {web: {image: nginx, port: 80, host: " + strings.Repeat("a", 63) + ".example.com}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := deployfile.Read(appDir(t, content)); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestReadRefReadsOnlyTheClusterAndName(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, deployfile.FileName), []byte("cluster: acme/prod\nname: web\nmanifests: gone\nunknown: 1\n"), 0o644)
	cluster, name, err := deployfile.ReadRef(dir)
	if err != nil || cluster != "acme/prod" || name != "web" {
		t.Errorf("ReadRef = %q, %q, %v", cluster, name, err)
	}
	os.WriteFile(filepath.Join(dir, deployfile.FileName), []byte("name: Web\n"), 0o644)
	if _, _, err := deployfile.ReadRef(dir); err == nil {
		t.Error("a bad name was taken")
	}
	os.WriteFile(filepath.Join(dir, deployfile.FileName), []byte("cluster: a/b/c\nname: web\n"), 0o644)
	if _, _, err := deployfile.ReadRef(dir); err == nil {
		t.Error("a bad cluster was taken")
	}
}
