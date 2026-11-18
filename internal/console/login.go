package console

import (
	"errors"
	"net/http"
	"time"
)

// maxLoginBytes is how long the body of a login may be.
const maxLoginBytes = 4 << 10

type loginPage struct {
	Problem string
}

func (c *Console) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	render(w, http.StatusOK, "login.html", loginPage{})
}

// handleLogin starts a session for the login token in the form.
func (c *Console) handleLogin(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxLoginBytes)
	if err := r.ParseForm(); err != nil {
		if _, tooLong := errors.AsType[*http.MaxBytesError](err); tooLong {
			http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if !c.tokens.redeem(r.PostForm.Get("token")) {
		render(w, http.StatusUnauthorized, "login.html", loginPage{Problem: "The token wasn't accepted."})
		return
	}
	setSessionCookie(w, c.sessions.start(), int(sessionLife/time.Second))
	http.Redirect(w, r, "/console/", http.StatusSeeOther)
}

// handleLogout ends the sessions of the request's cookies and removes
// the cookie.
func (c *Console) handleLogout(w http.ResponseWriter, r *http.Request) {
	cookies := r.CookiesNamed(sessionCookie)
	for _, cookie := range cookies {
		c.sessions.end(cookie.Value)
	}
	if len(cookies) > 0 {
		setSessionCookie(w, "", -1)
	}
	http.Redirect(w, r, "/console/login", http.StatusSeeOther)
}
