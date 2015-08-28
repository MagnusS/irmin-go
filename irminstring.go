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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

type IrminString []byte

func (i *IrminString) String() string {
	return string(*i)
}

func (i *IrminString) MarshalJSON() ([]byte, error) {
	if utf8.Valid(*i) {
		return *i, nil /* output as string if valid utf8 */
	} else {
		return []byte(fmt.Sprintf("{ hex: \"%x\" }", *i)), nil /* if not valid, output in hex format */
	}
}

func (i *IrminString) UnmarshalJSON(b []byte) error {
	type IrminHex struct { /* only used internally */
		Hex string
	}
	var h IrminHex
	var s string
	var err error
	if err = json.Unmarshal(b, &s); err == nil { /* data as string */
		if utf8.ValidString(s) {
			*i = []byte(s)
		} else {
			err = fmt.Errorf("string not valid utf8: %s", s)
		}
	} else {
		if err = json.Unmarshal(b, &h); err == nil { /* try to parse as hex */
			*i, err = hex.DecodeString(h.Hex)
		}
	}
	return err
}
