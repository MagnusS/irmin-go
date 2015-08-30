/*
 Copyright (c) 2015 Magnus Skjegstad <magnus@skjegstad.com>

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.

 THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

package irmin

import (
	"bytes"
	"net/url"
	"strings"
)

type IrminPath []IrminString

// Path delimiter. Always '/'
func (path *IrminPath) Delim() rune {
	return '/'
}

// Parse a path string by splitting into segments separated by '/'. Each segment may be PCT encoded to escape '/' in the name. (see url.QueryEscape)
func ParseEncodedPath(p string) (IrminPath, error) {
	segs := strings.Split(strings.Trim(p, " /"), "/")
	is := make([]IrminString, len(segs))
	for i := range segs {
		s, err := url.QueryUnescape(segs[i])
		if err != nil {
			return IrminPath{}, err
		}
		is[i] = []byte(s)
	}

	return is, nil
}

// Parse a path string by splitting into segments separated by '/'. See also ParseEncoded.
func ParsePath(p string) IrminPath {
	segs := strings.Split(strings.Trim(p, " /"), "/")
	is := make([]IrminString, len(segs))
	for i := range segs {
		is[i] = []byte(segs[i])
	}
	return is
}

// String representation of path
func (path *IrminPath) String() string {
	if len(*path) > 0 {
		var buf bytes.Buffer
		for _, v := range *path {
			buf.WriteRune(path.Delim())
			buf.Write(v)
		}
		return buf.String()
	} else {
		return ""
	}

}

// Relative URL representation of path
func (path *IrminPath) URL() *url.URL {
	if len(*path) > 0 {
		var buf bytes.Buffer
		for _, v := range *path {
			buf.WriteRune(path.Delim())
			buf.WriteString(url.QueryEscape(v.String()))
		}
		if u, err := url.Parse(buf.String()); err != nil {
			panic(err) // this should never happen
		} else {
			return u
		}
	} else {
		return new(url.URL)
	}

}
