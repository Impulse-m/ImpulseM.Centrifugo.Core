# Third-party notices

This file lists third-party code that is copied into or bundled with this repository. Go module dependencies (`go.mod`) are not listed here: they keep their own licenses and are fetched by the Go toolchain.

The copyright and license notices inside each file are kept as their authors wrote them.

## Gorilla WebSocket (modified copy)

* Location: `internal/websocket`
* Source: https://github.com/gorilla/websocket
* License: BSD 2-Clause. The full text is in [internal/websocket/LICENSE](internal/websocket/LICENSE), the authors are in [internal/websocket/AUTHORS](internal/websocket/AUTHORS).
* Changes made by this fork are listed in [internal/websocket/README.md](internal/websocket/README.md).

## UTF-8 decoder by Bjoern Hoehrmann (copied table)

* Location: `internal/websocket/examples/autobahn/server.go`
* Source: http://bjoern.hoehrmann.de/utf-8/decoder/dfa/
* Copyright (c) 2008-2009 Bjoern Hoehrmann
* License: MIT. The license notice is in the same file.

## d3 v7.9.0 (bundled)

* Location: `_examples/recovery_mode_cache/d3.v7.min.js`
* Source: https://github.com/d3/d3
* License: ISC. The license text is below.

```
Copyright 2010-2023 Mike Bostock

Permission to use, copy, modify, and/or distribute this software for any purpose
with or without fee is hereby granted, provided that the above copyright notice
and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
THIS SOFTWARE.
```

## centrifuge-js browser build (bundled)

* Location: `_examples/recovery_mode_cache/centrifuge.js`, a browser build of the centrifuge-js client
* Source: https://github.com/centrifugal/centrifuge-js (fork: https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.JS)
* Copyright (c) 2023 Centrifugal Labs LTD
* License: MIT. The license text is in the MIT section below.
* The build also contains an EventEmitter module with the Node.js `events` API. Source: https://github.com/browserify/events (npm package `events` 3.x, a dependency of centrifuge-js). The notice for it is in the MIT section below, under Joyent, Inc. and other Node contributors.

## MIT License text

The text below applies to the centrifuge-js build and to the EventEmitter module in it.

```
MIT License

Copyright (c) 2023 Centrifugal Labs LTD
Copyright Joyent, Inc. and other Node contributors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
