# rmapi-kt

[![build](https://github.com/hafacc/rmapi-kt/actions/workflows/build.yml/badge.svg)](https://github.com/hafacc/rmapi-kt/actions/workflows/build.yml)
[![jitpack](https://jitpack.io/v/hafacc/rmapi-kt.svg)](https://jitpack.io/#hafacc/rmapi-kt)
[![license](https://img.shields.io/github/license/hafacc/rmapi-kt)](LICENSE)

Kotlin/JVM client for the reMarkable cloud API — documents, folders, sync, and the `.rm`
stroke format. Plain Kotlin/JVM with no Android dependencies, and Android-compatible down
to API 21.

## Model

Everything is stored by the SHA-256 hash of its contents — documents, folders, and the
indexes that list them. A hash therefore names both a thing *and* its exact state, which
is how simultaneous edits are detected. Every item also has an `id`, a uuid4 that is
stable for the item's lifetime, so reads take an `ItemRef` pairing the two rather than
two separate strings that can be transposed.

Editing is read-modify-write: there is no "rename" call, so renaming means fetching the
`.metadata` file, changing one field, uploading it, and moving the account's root index
to point at the result. Two clients doing that at once will race; the loser retries
automatically.

## Usage

Register once with the eight-letter code from
<https://my.remarkable.com/device/desktop/connect>, then persist the device token — it
does not expire.

```kotlin
val deviceToken = register("abcdefgh")   // a DeviceToken; persist it
val api = remarkable(deviceToken)

for (ref in api.listRefs()) {
    val metadata = api.getMetadata(ref)
    println("${metadata.visibleName} (${metadata.type}) ${metadata.modifiedAt}")
}
```

`listRefs` costs one request for the root index; each `getMetadata` costs two more, so fetch
them concurrently with `async` if you want the whole account at once. `Metadata` is the
wire's own item-level type and already carries the name, parent, kind, and timestamps. A
document's `.content` holds only render settings and tags, so it is never fetched for a
listing; call `getContent(ref)` for the items you want it for. The wire stores timestamps as
strings, and `modifiedAt` / `openedAt` / `createdAt` read them as numbers.

`DeviceToken` and `SessionToken` are distinct types rather than both being `String`, so
handing the wrong one to `session` is a compile error instead of a confusing runtime
authentication failure.

In stateless environments, exchange the device token for a session once and reuse it:

```kotlin
val sessionToken = auth(deviceToken)     // short-lived
val api = session(sessionToken)          // no network call
```

### Reading

```kotlin
val ref = api.listRefs().first { api.getMetadata(it).type == EntryType.Document }

val metadata = api.getMetadata(ref)
val template = api.getTemplate(ref)       // a template item's .template definition
val pages = api.getPages(ref)             // parsed .rm pages, by page id
```

A pdf or epub can be larger than the heap has room for twice over, so it is written into an
okio sink a piece at a time rather than returned — a `Buffer` for memory, a file's `sink()`
for disk. `raw.getBlob` takes one the same way for any file.

```kotlin
File("book.pdf").sink().use { api.getPdf(ref, it) }   // ComponentNotFoundException if it has none
```

Strokes come back the same way whichever firmware wrote the page:

```kotlin
for ((pageId, page) in api.getPages(ref)) {
    for (layer in page.layers) {
        for (stroke in layer.strokes) {
            println("$pageId ${layer.name} ${stroke.pen} ${stroke.points.size} points")
        }
    }
}
```

Format versions 3 and 5 are entirely layers and strokes. Version 6 — what current firmware
writes — is a block-structured document, so it arrives as `RmFile.Scene`, which exposes the
same `layers` plus every raw `block`: a v6 page also carries text, glyphs, and editing
history that this library frames but does not interpret, and keeps rather than discards.

Parsing is strict throughout — a truncated stroke or a malformed block raises
`ValidationException` rather than yielding partial garbage, and one bad page fails the whole
`getPages` call. Erased strokes are dropped, as the device records them as tombstones with no
stroke data. If a document does have a page this library can't read, its bytes are still
reachable through `api.raw` — undecoded bytes are that tier's currency, not this one's.

Pages are written back the same way they are read, so an edit is a `mapValues`:

```kotlin
val edited = api.getPages(ref).mapValues { (_, page) -> recolour(page) }
val updated = api.setPages(ref, edited)
```

Only the pages that actually changed are uploaded: an unedited page serialises back to the
bytes it was read from, so it re-stages to the hash reading it already cached. Pages the
document doesn't already have are refused rather than written — adding one means listing it in
the `.content` too, which is `setDocumentContent`.

A single page has its own pair, and `getPage` is the one that matters: it fetches one blob
rather than the whole document, and fails only if *that* page is malformed.

```kotlin
val page = api.getPage(ref, pageId)             // null if nothing is drawn on it yet
api.setPage(ref, pageId, recolour(page ?: blankPage))
```

A page exists because the `.content` lists it, not because it has a `.rm` file — the device
writes that file the first time something is drawn. So `getPage` returns null for a real but
empty page and only errors on a page id the document doesn't have, and writing to an empty
page is how it gets its first strokes.

A version 6 page is written from its `blocks`; its `layers` are a decoded view of those, so
editing `layers` and writing the result back changes nothing.

### Writing

Every edit takes an `ItemRef` and returns one, so the result of an edit is directly usable
as the input to the next — an item's hash changes on every write, and reassembling the ref
by hand is exactly the transposition hazard `ItemRef` exists to prevent:

```kotlin
val renamed = api.rename(ref, "a better name")
val starred = api.star(renamed, true)
val moved = api.move(starred, Parent.Folder(folder.id))
api.trash(moved)
```

`trash` is a move, so the device can still restore the item. `purge` is not: it drops the
entry from the root index and nothing brings it back. `purgeTrash` does the same to
everything the trash holds, including whatever sits inside a trashed folder — the device
leaves those pointing at the folder rather than moving them:

```kotlin
api.purge(ref)                   // gone from the account, not moved
val removed = api.purgeTrash()   // a ref to each item removed
```

`.pagedata` names the template behind each page, positionally:

```kotlin
api.setPagedata(ref, listOf("Grid", "", "Dots"))   // middle page has no template
```

Layer names live beside each page, and their order matches `RmFile.layers`:

```kotlin
api.getPageMetadata(ref, pageId)?.layers?.map { it.name }
```

Highlights on a pdf or epub are stored per page, and come back as a list of lists — one
fragment per line a passage spans:

```kotlin
for ((pageId, passages) in api.getHighlights(ref)) {
    for (fragment in passages.flatten()) println("$pageId ${fragment.text}")
}
api.setHighlights(ref, pageId, passages)
```

A template's definition is read and written the same way, and is the whole of what the
device renders it from — a template's `.content` is empty:

```kotlin
api.setTemplate(ref, api.getTemplate(ref).copy(name = "dots"))
```

`move`, `rename` and `star` are the named cases of `setMetadata`, which reaches the rest of
`Metadata` — `lastOpenedPage`, `deleted`, `source`. Every write takes a value, so a change is
a read, a `copy`, and a write:

```kotlin
api.setMetadata(ref, api.getMetadata(ref).copy(lastOpenedPage = 12))
```

Bulk edits take one root write and return only what they changed. A ref the root index no
longer lists is passed over, so subtract the result from what you handed in to find those:

```kotlin
val refs = listOf(firstRef, secondRef)
val moved = api.bulkTrash(refs)     // Map<ItemRef, ItemRef>, old to new
val removed = api.bulkPurge(refs)   // Set<ItemRef>, the refs that are gone
val missing = refs - moved.keys     // the ones someone else had already written past
```

Content works the same way, with a typed getter for each shape so nothing needs casting:

```kotlin
val content = api.getDocumentContent(ref)
api.setDocumentContent(ref, content.copy(textScale = 1.5, lineHeight = 200))
```

Keys the wire carries but this library doesn't model are preserved from what's already
stored, so writing back a value that never saw them can't drop them.

`zoom` reads the flat `zoomMode` and `customZoom*` fields back as the sealed `Zoom`, and
`withZoom` writes all seven together so a stale custom number can't outlive its mode:

```kotlin
val content = api.getDocumentContent(ref)
api.setDocumentContent(ref, content.withZoom(Zoom.BestFit))
```

### Two ways to add a document

These are different mechanisms, not synonyms:

- `upload*` hands the file to the server's ingestion endpoint. The server builds the
  document. Robust, but no options.
- `put*` builds the document's component files locally and commits them through the sync
  protocol. Full control via `PutOptions`.

Both take an okio source and read it into memory once. `put*` must, because the sync protocol
names a file by its hash, so the whole file has to be read before any of it is sent;
`upload*` does so because a request that fails transiently is sent again.

```kotlin
File("simple.pdf").source().use { api.uploadPdf("simple", it) }

val epub = File("controlled.epub").source()
api.putEpub("controlled", epub, PutOptions(
    lineHeight = 180,                           // a value reMarkable's own apps don't expose
    margins = 50,
    parent = Parent.Folder(folder.id),
    zoom = Zoom.Custom(                         // the custom fit needs all of its numbers,
        scale = 1.5,                            // so they travel together rather than as
        centerX = 0.0, centerY = 0.0,           // six optional fields beside a mode enum
        pageWidth = 1404.0, pageHeight = 1872.0,
        orientation = Orientation.Portrait,
    ),
))
```

A document can also be archived and restored, which round-trips every component file. Note
this is a transfer format, not the document itself — for the pdf use `getPdf`:

```kotlin
File("doc.zip").sink().use { api.exportArchive(ref, it) }              // a zip of every component file
val restored = File("doc.zip").source().use { api.importArchive(it) }  // always under a fresh id
```

### Hearing about changes

reMarkable pushes a notification whenever a device finishes syncing, over a websocket the
client holds open:

```kotlin
api.notifications()
    .filter { it.attributes.sourceDeviceID != api.deviceId }   // skip your own syncs
    .collect { show(api.refreshRoot()) }
```

A notification names the device that synced and says nothing about what changed, so reading
is still how you find out. `api.deviceId` is the uuid this client registered under, which
reMarkable stamps on its work, so comparing the two is how you ignore your own. Other kinds
of event share the socket — screen sharing among them — and never reach the flow. The socket is reopened whenever the account drops it, which it does every couple
of minutes; leaving the flow closes it. It is not a queue — what happens between two sockets
is not replayed — so anything that must not miss a change still reads on its own schedule.
A session token lasts a few hours, and a flow collected past that eventually fails to
reconnect: build a new client with a fresh token.

## Configuration

```kotlin
val api = session(sessionToken, SessionOptions(
    httpClient = OkHttpClient.Builder().build(), // timeouts, interceptors, proxies, pinning
    maxTransientRetries = 3,                     // network errors, 5xx, 429
    maxGenerationRetries = 10,                   // lost races against another client
    maxCacheBytes = 8 * 1024 * 1024,             // a growth bound, not a memory budget
    maxCachedBlobBytes = 512 * 1024,             // above this, only the hash is remembered
))
```

The default `OkHttpClient` is shared across option sets rather than constructed per
instance, so a defaulted `SessionOptions()` does not create its own connection pool.

The three hosts are overridable too, which is how you point the client at
[rmfakecloud](https://github.com/ddvk/rmfakecloud).

The cache lives in memory. To start a later session warm, dump it into a sink and load it
back from a source; both go an entry at a time, since a full cache as one string is several
times its own size in heap. `cacheRevision` says whether there is anything new to write:

```kotlin
var written = file.source().use { api.loadCache(it) }   // ValidationException if unreadable
// ...
if (api.cacheRevision != written) {
    written = file.sink().use { api.dumpCache(it) }
}
```

An okio `Buffer` is an in-memory sink, so `Buffer().also { api.dumpCache(it) }.readUtf8()` is
the dump as a string.

## Errors

Everything the library raises itself extends `RemarkableException`:

| | when |
|---|---|
| `GenerationException` | another client wrote the root first, and the retries ran out |
| `ResponseException` | the server answered outside 2xx; carries status and body |
| `ValidationException` | a payload didn't match what was expected; carries the raw text, except from `loadCache`, which never holds the dump whole |
| `HashNotFoundException` | the hash isn't in the current root index |
| `ComponentNotFoundException` | the item exists but has no such file, e.g. no epub |

`IOException` from OkHttp is deliberately not wrapped: the network being down is a
different problem with a different handler than the API refusing a request.

Decoding is **strict** — a key this library doesn't model is an error rather than
something silently dropped, because a read-modify-write that dropped it would destroy
whatever the device stored there. The cost is that a field added by new firmware fails
the parse until this library is updated; `ValidationException.rawText` and the low-level
`api.raw` client both hand you the payload so you are never locked out of your data.

## Installation

Via [JitPack](https://jitpack.io/#hafacc/rmapi-kt):

```kotlin
repositories {
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.hafacc:rmapi-kt:<version>")
}
```

Maven users note that OkHttp 5 is published as a Kotlin Multiplatform project — the
`okhttp` artifact is empty for Maven, so select `okhttp-jvm` or `okhttp-android`
explicitly. Gradle handles this automatically.

## Android

Requires **minSdk 21**, with no core-library desugaring. This is enforced rather than
claimed: the build runs [animalsniffer](https://github.com/xvik/gradle-animalsniffer-plugin)
against Android API-21 signatures, so a call to anything newer fails CI.

## Scope

Rendering a notebook to a page image and exporting annotated PDFs happen inside
reMarkable's own apps and are not implemented here. `.rm` stroke files are parsed for
format versions 3, 5, and 6; within version 6, text and glyph blocks are kept but not
interpreted.

## Design

[`docs/design.md`](docs/design.md) describes the protocol this talks to and the reasoning
behind the API's shape.

## License

MIT
