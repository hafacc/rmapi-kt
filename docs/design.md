# rmapi-kt — Design

Why this library is shaped the way it is.

rmapi-kt is a Kotlin/JVM client for the reMarkable cloud: an undocumented,
reverse-engineered, content-addressed sync protocol. §1 describes that protocol, because
almost every decision here follows from it. §2 is the decisions themselves.

This document holds what the code cannot say for itself: what the wire does, and why a
choice went one way when the other way was reasonable. It does not restate signatures —
read the generated documentation for those — and it does not keep a record of what earlier
drafts did, because a design that has to be read alongside its own history is one nobody
reads.

---

## 1. The Protocol

The reMarkable cloud API is undocumented. Everything below was established by reverse
engineering and is corroborated by more than one independent implementation, but none of it
is guaranteed by the vendor — which is why §D4 and §D13 are as strict as they are.

**The store is immutable and content-addressed.** Every file lives under the SHA-256 of its
own bytes at `GET`/`PUT {rawHost}/sync/v3/files/{hash}`, with an `rm-filename` header naming
the logical file and `x-goog-hash: crc32c=<base64>` on uploads. Nothing is ever modified in
place; changing a file means writing a new one and re-pointing whatever referenced it.

**An item is an index, and the account is an index of indexes.** An entry index is a text
file of `hash:type:id:subfiles:size` lines. Schema 3 is just those lines behind a version
line; schema 4 adds a `0:<id>:<count>:<size>` header. A document's index lists its component
files (`.content`, `.metadata`, `.pagedata`, the pdf or epub, and the per-page `.rm` files);
the root index lists every item.

The two schemas hash differently, and this is the single most error-prone part of the
format: **schema 3 hashes an index as the SHA-256 of its entries' concatenated hashes**,
while **schema 4 hashes the index file's own bytes** like any other file. Newly written root
indexes must always be schema 4 — the cloud rejects a schema-3 root with a 400 "Software
must be updated" — while an item's index follows whatever schema the account uses. The root
index also names itself `.` rather than `root` in its own header line.

**Writes are compare-and-swap on a generation.** The root hash and a monotonically
increasing generation live at `GET {rawHost}/sync/v4/root` and `PUT {rawHost}/sync/v3/root`.
A PUT carrying a stale generation fails with `{"message":"precondition failed"}`. That is
the entire concurrency-control story: any edit is a read-modify-write of the root, and two
clients editing at once means one of them loses and must re-apply. §D12 covers how.

**Auth is two tokens.** `POST {authHost}/token/json/2/device/new`, carrying an eight-letter
one-time code plus a device description and uuid, returns a long-lived device token as raw
text — not JSON. `POST {authHost}/token/json/2/user/new` with that token as a bearer returns
a short-lived session token, used as the bearer everywhere else. Both are jwts, and both
carry the registering uuid in a `device-id` claim, which is how a client recovers its own
identity without storing anything. The default hosts are
`webapp-prod.cloud.remarkable.engineering` for auth, `eu.tectonic.remarkable.com` for blobs,
and `internal.cloud.remarkable.com` for ingestion.

**There is a second, simpler way to add a document.** `POST {uploadHost}/doc/v2/files` with
the file's mime type, an `rm-meta` header (base64 JSON naming the file), and
`rm-source: RoR-Browser` returns `{docID, hash}`. The server builds the item itself, so this
path never touches the root index or a generation. It is a genuinely different mechanism
from constructing the component files locally, which is why the API keeps both (§3).

**A websocket says when something changed.** `{rawHost}/notifications/ws/json/1`, bearer
authenticated, pushes a frame whenever a device finishes syncing. §D14 covers what it
carries and what it does not.

**Dead ends**, recorded so they are not rediscovered: sync10 and the
`/sync/v2/signed-urls/*` endpoints are gone; `POST /sync/v2/sync-complete` is a no-op stub.
The old per-account notification host from a service manager no longer resolves. Telling
other devices something changed is purely the `broadcast: true` flag on the root PUT;
hearing it is the socket above.

**Provenance.** The wire understanding here draws on the open-source
[rmapi-js](https://github.com/erikbrinkman/rmapi-js) and Go
[rmapi](https://github.com/ddvk/rmapi) implementations, which agree on the endpoints, the
hashing rules, and the root-generation semantics. Where they differ, the differences are
cosmetic and both work against the real cloud: Go sends `rm-filename` on the root PUT and a
content type on blob PUTs where js sends neither, and Go sorts index entries by byte order
where js uses a locale compare — identical for the ids actually in play, which is why this
library sorts with plain `compareTo` and pins the result with golden-hash tests. Go's
independent index and hashing fixtures are used as a cross-check (§4).

---

## 2. Design Decisions

### D1. Naming and layout

Repo `hafacc/rmapi-kt`, single Gradle module, group `io.hafa`, artifact `rmapi-kt`, package
`io.hafa.rmapikt`. Library only, no CLI. Package name = artifact name = repo basename, so
the JitPack coordinates and the import path agree with each other.

### D2. Kotlin/JVM targets and the Android floor

**JVM target 17**, the lowest current LTS, plus an API-usage discipline that keeps the
library **Android-safe at minSdk 21 with no desugaring**, enforced by animalsniffer against
`gummy-bears` API-21 signatures in CI.

Bytecode level is not the constraint — AGP 8+/D8 consume class-file 61 happily. *API
availability* is, which rules out several obvious choices:

- `java.time` would force API 26. The wire wants epoch-millis strings, so the internal clock
  is `System.currentTimeMillis()` and nothing needs dates (§D10).
- `java.net.http.HttpClient` is API 34. OkHttp's floor is API 21, and that is the library's
  floor too (§D3).
- `java.util.zip.CRC32C` is API 34, replaced by a ~30-line internal table-driven Castagnoli
  implementation in `Digest.kt`, pinned by RFC 3720 vectors and cross-checked against the JS
  `crc-32/crc32c` outputs.
- `java.util.Base64` is API 26 and `java.util.HexFormat` is JDK-only; the Kotlin stdlib's
  `kotlin.io.encoding.Base64` and a tiny internal hex pair cover both.
- There is no logging framework. The one warning worth emitting — writing a schema-3 root —
  is `System.err.println`: logcat-visible, no dependency, no API surface.

`MessageDigest`, `java.util.zip.Zip{Input,Output}Stream`, `java.util.UUID`,
`java.util.concurrent`, and both kotlinx libraries are all fine at API 21. Nothing touches
the filesystem — the cache dumps to an okio `Sink` and loads from a `Source` the caller
opens, and persisting a token is the caller's job — so there are no `java.nio.file` concerns.

### D3. HTTP: OkHttp used directly

OkHttp is used directly and internally, with no transport abstraction and no HTTP types of
this library's own. The only HTTP-related public surface is `SessionOptions.httpClient`.

Callers do not want to reimplement HTTP; they want timeouts, interceptors, certificate
pinning, or a proxy. Accepting an `OkHttpClient` gives them all of that with zero new types,
and it is the established JVM pattern. The honest cost is that OkHttp becomes part of the
public contract, so changing engines later would be breaking — acceptable for a client with
OkHttp's track record, and the same coupling Retrofit has shipped for years.

Two consequences worth stating. **Uploads are whole `ByteArray`s internally**, deliberately:
the protocol is content-addressed, so every upload must be fully materialized to hash it
before the request is made, and a transient failure sends the same bytes again. Downloads
are not: a pdf or epub is written into the caller's `Sink` a piece at a time, since one can
be larger than the heap has room for twice over, and only the small files the client decodes
itself are read whole. And **tests need no seam** — the host options exist
for rmfakecloud anyway, so pointing them at a local MockWebServer exercises real OkHttp over
a real socket (§4).

### D4. JSON: kotlinx-serialization, decoded strictly

**Decoding is strict.** Updates are read-modify-write of `.content`/`.metadata` files, so a
key the library doesn't model must not be quietly discarded on the way back out. There are
two ways to guarantee that: carry unknown keys through untouched, or refuse to decode a
payload you don't fully understand. This library takes the second — `ignoreUnknownKeys =
false` on every stored wire type, with an unrecognised key raising `ValidationException`.

Passthrough would cost an `extra: JsonObject` on a dozen types, a serializer that splits and
re-merges unknown keys, and — the deciding factor — a manual registry mapping each type to
that serializer, where a forgotten entry silently reverts that type to dropping unknown keys
with no compile error and no failing test. Strict decoding removes that failure mode by
construction.

**The one exception is what the notification socket pushes** (§D14), decoded through a
second `Json` that ignores unknown keys, reached by its own `decodePushed` rather than a
policy argument on `decodeWire` — so relaxing strictness stays a decision about a kind of
payload and never something a call site can opt into. Every reason for the rule is absent
there: a notification is never written back, the socket is shared with event families this
library does not model, and rmfakecloud sends a subset of what reMarkable does.

**Quirk keys** are the other relaxation, and a much narrower one: a small set is stripped
before decoding and written back verbatim afterwards, so it neither reaches the public API
nor breaks the parse. Today that set is exactly `modifed`, the device's own misspelling of
`modified`, present on 28 of 592 documents in a real account — strict decoding made
`listItems()` throw outright on that account. Modelling the key would put a firmware typo in
the public surface where a caller would have to decide what it meant.

**Accepted risk**: reMarkable adds fields over time — `cPages`, `keyboardMetadata`,
`transform`, `viewBackgroundFilter`, the `customZoom*` family and `originalPageCount` all
arrived this way. When it happens, `listItems()` throws for every user until a release
propagates, because it reads every item's `.content` and `.metadata`. The escape hatches are
deliberate: `ValidationException` carries the raw text, and the `raw` client returns
unparsed blobs, so a caller is never locked out of their data.

**The two tag shapes are one sealed field, not duplicated classes.** Older firmware writes
tags as bare strings and current firmware as objects with timestamps. That is a difference
in one *field*, so `Tags` is sealed over `Structured` and `Legacy` with uniform `names`
access, and a legacy payload re-encodes as strings rather than being silently upconverted.
`Content` is therefore three types, discriminated structurally: `fileType` present means a
document, otherwise a collection. Templates are not a third shape — see below.

**Templates are not a `.content` shape.** They were modelled as one, discriminated by a
`templateVersion` key; real templates have an *empty* `.content` and keep their definition
in a separate `.template` file, so all three templates in a real account read as folders. An
item's kind comes from `metadata.type`, which states it outright. `TemplateDefinition`'s own
fields also read as stricter than they are: `constants` values are numbers *or* expressions
over other constants (`"templateWidth - (offsetX * 2)"`), and `supportedScreens` and
`labels` are each absent on some real templates.

### D5. Async model: suspend functions, structured concurrency, stage/upload

Every network-touching function is a `suspend fun`. Internals use `coroutineScope { async
{ … } }` where work is genuinely parallel — entry resolution in `listRefs`, component
uploads in `putPdf` — unbounded, with no semaphores and no knobs: the cloud has not needed
throttling in practice, and a limit nobody can tune is worse than none. No dispatcher is
hard-coded; OkHttp's `enqueue` runs on its own executor and the suspend adapter just resumes
the caller, so the API is safe from any context including Android's main thread.

**Hashing and uploading are separate operations.** Hashing is pure, local, and cheap;
uploading is neither. `stageFile(...)` hashes locally and returns a `StagedFile` — the entry
naming the file, bound to the bytes it describes — and `upload(staged)` sends that file and
nothing else. A staged file is inert. What this rules out is starting the upload at *stage*
time, which would hand back a value plus an already-running job: a `Deferred` with no owner,
which structured concurrency exists to avoid. rmapi-js does start eagerly, because
`await using` is the nearest thing JavaScript has to a scope that waits for its children;
Kotlin has the scope itself, so high-level operations stage everything, use the entries
immediately, and `awaitAll` the uploads inside their own `coroutineScope`.

The split is what lets an edit send everything in one wave. Building the new root index
needs the docSchema's *hash*, not its upload, so nothing has to be in the store before the
root index is staged: an operation stages its changed components and the docSchema over
them, and hands both to `commitEdit`, which stages the root index, uploads all of them at
once, and commits. Waiting for the components first would cost a round trip on every edit.
It is also why the staging functions are named for what they do rather than for the `put*`
they correspond to on the wire — they do not put anything.

`stageEntries` takes `RawEntry` rather than `StagedFile`, as rmapi-js's `putEntries` does,
and has to: an edit rebuilds an index mostly out of rows read off the server, for files that
already exist and must not be re-sent. So nothing in the raw tier can stop a caller
committing a root that points at bytes never sent. The high-level client closes that in
`commitEdit`, `commitNewItem`, and `bulkMove`, which upload exactly the staged files whose
entries they splice in.

No blocking/Java facade — Java callers bridge with `kotlinx.coroutines.future.future {}`.
The one `Flow` in the API is a stream of events with no list form (§D14); a `Flow` variant
of a listing call would just be an alternative spelling of a suspend function.

### D6. Type modeling

The wire is all strings. The API's job is to stop the caller confusing one string for
another, and to make the states the protocol allows the only ones expressible.

- **`FileHash`, `ItemId`, and `DeviceId` are value classes**, the first two validated on
  construction. They are the strings that must never be swapped and are the same shape to
  the naked eye. All cost nothing at runtime.
- **`ItemRef(id, hash)` is the currency of the whole API.** An item has a stable id and a
  hash that changes on every write, and almost every operation needs both. Passing them as
  two arguments is the single most likely caller bug in a client for this protocol, so they
  travel as one type — in *and* out, since an edit returns a new ref.
- **There is no listing type.** A listing returns the wire's own `Metadata`, which already
  carries the name, parent, kind, and timestamps. A dedicated entry type was tried and
  removed: everything it exposed came from `Metadata` except `tags` and `fileType`, and
  fetching a `.content` per item for two fields made a listing cost 3N requests instead of
  2N. It also had to reconcile metadata against content, and that join was the sole cause of
  the templates-as-folders bug.
- **`sealed interface Parent { Root, Trash, Folder(id) }`.** The wire encodes the root as
  `""` and the trash as `"trash"`; leaving those as magic strings would make "move to the
  root" and "move to a folder named nothing" the same expression.
- **Enums for every closed wire vocabulary**: `SchemaVersion`, `FileType`, `Orientation`,
  `TextAlignment`, `ZoomMode`, `BackgroundFilter`, `UploadKind`, `EntryType`,
  `DocumentComponent`. `TextAlignment`'s device default is the empty string on the wire,
  which is exactly the kind of value that should not be typed as `String`.
- **Every wire type is an immutable data class**, updated with `copy()`. `ByteArray` never
  sits inside one, because its equality is identity.
- **Timestamps convert at the boundary.** `Metadata` keeps the wire's stringified epoch
  millis because it round-trips; `Entry` is built for the caller and never written back, so
  it exposes `Long`.

### D7. Surface principles

Rules the public API follows, which between them explain most of its shape.

**One name, one mechanism.** `put*` and `upload*` do genuinely different things — `put*`
builds every component file locally and commits it through the sync protocol, with full
options and a generation to lose; `upload*` hands the file to the server's ingestion
endpoint, which is robust but offers no control. Separate families rather than one function
with a flag, each KDoc pointing at its counterpart.

**Say the thing, don't flag it.** One `refreshRoot()` rather than a `refresh: Boolean` on
every read. A boolean threaded through a dozen signatures describes a single concept, and
naming it once is cheaper than repeating it — especially since the answer is almost always
"no".

**Return something usable.** An edit returns an `ItemRef`, not a bare hash, so the result of
one edit is the input to the next. A bulk edit returns only what it changed. It also named
what it could not find, until that half was removed: a ref the batch passed over is the
caller's own input minus the result, so carrying it cost a type and a field to say something
the caller already had.

**A method must add something a caller cannot.** The test is whether removing it would push
the caller down to `raw`. `uploadPdf` passes — without it there is no way to name the right
upload kind. `listMetadata()` failed: it was one line over `listRefs().associateWith(::getMetadata)`,
so it was a second name for something the API already said.

**Each tier has one currency, and bytes are not the high tier's.** `RemarkableClient`
returns `Metadata`, `Content`, `TemplateDefinition`, `RmFile`. A `.rm` file has a decoded
form, so handing back undecoded page bytes there would make pages the one component a caller
had to finish decoding themselves. `getPdf` and `getEpub` are not exceptions — a pdf has no
decoded form in this library, so bytes *are* its type. The escape hatch for anything this
tier refuses is the tier whose currency bytes actually are.

That is also what makes `getPages` all-or-nothing acceptable: it throws for the whole
document when one page is unreadable, and that is exactly what makes `setPages` safe, since
a document that cannot be read in full is never one this API will offer to write back.

**A short method is fine; a second name for the same thing is not.** `getPage` is not one
line over `getPages`, because fetching one blob is different work from downloading and
parsing every page to return one. `setPage` genuinely is one line over `setPages` and is
kept anyway — once `getPage` exists, a missing `setPage` becomes its own puzzle. `trash` and
`bulkTrash` are one line over `move`, and are kept because the trash is not a folder: naming
it as a verb is what tells a caller that trashing is a move, and therefore reversible.
`purge` is the operation that is not, and shares nothing with them: it shortens the root
index rather than writing any metadata. `purgeTrash` is its own call because the trash's
contents are a tree — trashing a folder does not touch what is inside it — so "everything in
the trash" is not a set a caller can hand over.
`raw.getRm`/`raw.stageRm` are kept for the same reason — the raw tier pairs a `get*` with a
`stage*` for every file kind it names, and a half-pair reads as though writing were
supported and reading were not.

**The root is a method, not a name.** `raw.getRootEntries` and `raw.stageRootEntries` exist
because the alternative is a caller spelling `"root.docSchema"` to read and `"root"` to
write, with two special cases they cannot see: the root must be schema 4, and the index
names itself `.`. Publishing those strings would publish the names without the behaviour
behind them. The schema argument disappears rather than being checked — the root's one legal
schema is not a runtime `require` when it can be the only thing the signature allows.

**Every write takes a value.** `setMetadata`, `setDocumentContent`, `setPages` and their
singular forms are all `set(ref, value)`; none takes a lambda over the stored value. A
`(T) -> T` reads as a partial update and saves one fetch, but it puts the caller's code
inside this library's control flow to buy nothing they cannot do themselves, and it does not
generalise — a page write never needs the old value. `rename`, `star`, and `move` sit beside
`setMetadata` because each names a wire field a caller would otherwise have to know is
called something else: `visibleName`, `pinned`, and a `Parent` instead of a raw string.

### D8. Error taxonomy — designed around what a caller branches on

A caller of this library branches on four things: *my view of the root was stale*; *the
server refused or broke*; *the payload didn't match the reverse-engineered schema*; and
*the thing I referenced isn't there* — the last split further into gone versus merely
stale, which have different recoveries. `RemarkableException` is sealed over exactly those,
so `when` is exhaustive.

- **`GenerationException` carries the generation this client sent.** A retry loop wants to
  know what it sent; the server doesn't report the new generation on conflict, so nothing
  more is pretended.
- **`ResponseException` does not opine on retryability.** The library retries internally
  where it can tell retrying is safe (§D12), so a `ResponseException` is by construction
  what survived that. It carries `status` and lets the caller draw their own line.
- **`ValidationException` carries the payload that failed.** Against a reverse-engineered
  format, "this didn't parse" is not a dead end if the caller can still see what arrived, so
  the escape hatch is built into the exception rather than left as advice. A cache load is
  the exception: the dump is read a piece at a time and never held whole, so there is no
  text to attach.
- **`HashNotFoundException` distinguishes gone from stale.** A `currentHash` means only the
  ref went stale and re-reading is enough; null means the item is not in the account.
- **`ComponentNotFoundException` carries a `DocumentComponent`**, so "this doc has no epub"
  is branchable without matching on a message.
- **Transport failures are not wrapped.** An `IOException` from OkHttp means the network is
  down, which is a different problem with a different handler than the API refusing a
  request.
- **Programmer errors stay outside the hierarchy.** A bad connect-code length or malformed
  hex is `require`/`check`; misuse is not a runtime condition to branch on.

### D9. Caching

Content-addressed data never goes stale, so blobs cache by hash forever. Small blobs
(indexes, `.content`, `.metadata`, `.rm` pages) are held whole; anything above
`maxCachedBlobBytes` records only that the hash **exists** — enough to skip a re-upload, not
a re-download — so one pdf cannot evict everything else to hold itself. `maxCacheBytes`
bounds the total, and defaults to a finite value so a long-running process cannot accumulate
blobs indefinitely.

- **Values are bytes, and the split is on size rather than kind.** An earlier cache held
  decoded text, which meant it could only admit a blob that decodes losslessly — so `.rm`
  pages, the one thing a stroke-editing client reads repeatedly, were fetched every time.
- **`CacheEntry` is sealed** rather than a nullable value in a string map, so "known to
  exist" reads as intent rather than as an accident of nullability.
- **The dump is versioned JSON**, written into a sink by `dumpCache(sink)` and read from a
  source by `loadCache(source)`, an entry at a time, with unknown versions rejected rather
  than silently misread. `cacheRevision` moves only when the contents do, so a caller can
  skip a write that would change nothing. JSON rather than an opaque binary format because
  the dump's second job is debuggability. The format is this library's own; a cache is a
  performance artifact, so a cold start costs only a few refetches.
- **LRU** via `LinkedHashMap(accessOrder = true)`, with `pruneCache` doing a BFS-from-root
  reachability sweep.
- **Thread safety** is `synchronized` blocks: critical sections are pure map operations with
  no suspension and no I/O, so a monitor beats a suspending `Mutex`. Two concurrent reads of
  one hash fetch twice and then agree, which for content-addressed data is a wasted request
  rather than a correctness problem.

### D10. No test seams at all

The client calls `System.currentTimeMillis()` and `UUID.randomUUID()` directly. There is no
clock or id abstraction and no injectable environment, because its only customer would be
the test suite.

Tests cope by driving the golden-hash paths with caller-supplied entries, so nothing is
minted there, and by asserting the few bodies that do contain minted values *structurally* —
decode, check every field, check the uuid and timestamp for well-formedness. That is a
deliberately weaker assertion, and it is the price of the deletion.

The only publicly controllable identity input is `register`'s `uuid`, which exists because a
caller genuinely needs a stable device identity — and which is recoverable afterwards as
`RemarkableClient.deviceId`, since the tokens carry it (§1).

### D11. Hosts

The three production hosts as defaults (`Hosts.AUTH`, `Hosts.RAW`, `Hosts.UPLOAD`), each
overridable in options — which is also the rmfakecloud story and the reason the test suite
needs no seam. No config-file handling; persisting a token is the caller's concern.

### D12. Retries — two layers, for two unrelated failures

- **Transient**, in `AuthedHttp`: a network exception, a 5xx, or a 429 is retried with
  exponential backoff and full jitter, base 200 ms, capped at 30 s, `maxTransientRetries`
  times (default 3). Applies to every request.
- **Generation conflict**, in `withGenerationRetry`: losing a race for the root index
  re-runs the whole read-modify-write, base 25 ms, `maxGenerationRetries` times (default
  10). Set to 0 to surface `GenerationException` immediately.

**Why the split matters**: a generation conflict must *not* be retried at the request layer.
Resending the identical root write resends the identical stale generation, so it can only
fail again; the work that has to be redone is the read-modify-write above it. That is why
`AuthedHttp` raises an internal `PreconditionFailedException` rather than retrying, and why
`putRootHash` — the only place that knows which generation was sent — converts it into the
public `GenerationException`.

Each attempt reads the root exactly once and derives both the entry list it merges and the
generation it commits against from that single `RootInfo`. Reading the root twice in one
attempt could interleave with another operation on the same client, and merging one
generation's entries into a commit against another's generation can *succeed*, silently
reverting whatever landed in between.

**The load-bearing detail**: every minted value — document ids, page ids, timestamps — is
resolved *before* the retry loop, so a retry re-sends byte-identical blobs and the cache
skips them. Minting inside the loop would produce a fresh set on every attempt, and since
the store is content-addressed and append-only, nothing would ever clean them up. A test
asserts that a conflict-and-retry does not re-upload the document body.

### D13. `.rm` page files

The `.rm` stroke format is parsed. It is undocumented but extensively reverse-engineered in
public, and the page files are already reachable as ordinary component blobs — so leaving
them as opaque bytes would be a gap rather than a boundary.

**Versions 3 and 5** decode fully: layers, strokes, and per-point
x/y/speed/direction/width/pressure. v5 differs only by one extra word in the stroke header.

**Version 6** — what firmware 3.0 and later writes — also decodes to strokes. Its payloads
use a tagged encoding: each field is introduced by a varint of `index shl 4 or tagType`,
where `0xF` is a CrdtId (a byte plus a varint), `0xC` a length-prefixed subblock, and
`0x8`/`0x4` fixed widths. Strokes live in block type `0x05` behind a one-byte item-type
discriminator; layer names live in block type `0x02`. Points are 14 bytes, with speed and
width as `uint16` scaled by 4, direction as a byte over 360°, and pressure as a byte over
255 — converted on read so `RmPoint` means the same thing in every version.

**A deleted stroke has no value subblock at all** — just a non-zero deleted length — and is
dropped rather than drawn. That case is not inferable from the layout; it was found by
decoding real pages and noticing the one block that would not parse.

`RmFile` is a sealed pair (`Lines`/`Scene`) rather than one type with half its fields empty.
Both expose `layers`, so reading strokes doesn't depend on the firmware that wrote the page,
but `Scene` additionally keeps every raw block, because a v6 page carries text, glyphs, and
editing history this library does not model and would otherwise silently discard. Parsing is
strict — a truncated stroke, an over-long block, a point run that isn't a whole number of
points, or trailing bytes all raise `ValidationException` — on the same reasoning as §D4:
partial output from a binary format is indistinguishable from correct output.

**Writing.** `serializeRmFile` inverts the parser, and round-tripping an untouched page
reproduces the original bytes exactly, which for a content-addressed store means the
original hash and therefore no upload at all. That property is what lets the typed API take
`RmFile` rather than bytes without rewriting pages the caller never touched, and it is why
the reader keeps two words per v3/v5 stroke that it does not interpret (`reserved`, and the
extra word v5 added) plus the byte a v6 block header reserves. A v6 page is written from its
blocks rather than its decoded layers, because those layers are a view of the blocks and
re-encoding them would drop everything this library frames but does not interpret.

`setPages` replaces pages and refuses to invent them. A `.rm` file the `.content`'s `pages`
list does not mention is one the device will never render, so accepting an unknown page id
would produce a write that appears to succeed and shows nothing. Adding a page is
`setDocumentContent`'s business, because that is the file which has to change.

**A page exists in the `.content`, not on disk.** The device writes a page's `.rm` only when
something is drawn on it, so a declared page with no `.rm` is a real, empty page rather than
a missing one. `getPage` returns null for such a page and raises only for an id the document
never declared, and a write to one creates the file. Keying off the `.rm` file instead would
make an untouched page indistinguishable from a nonexistent one.

**Still unmodelled**: text, glyphs, and the editing history — block types `0x00`, `0x01`,
`0x03`, `0x04`, `0x07`, `0x08`, `0x09`, `0x0A`, and `0x0D` are framed and kept but not
interpreted. Pens and colours are named one enum entry per wire code, including the v5
renumbering and the Paper Pro's extended palette, and a code neither names raises like any
other byte this library cannot account for.

### D14. Notifications

`RemarkableClient.notifications(): Flow<SyncEvent>` over the socket in §1, reopened for as
long as the flow is collected. A frame is a wire type like any other, and names the device
that synced and nothing about what changed, so `refreshRoot` is the answer to one and
comparing against `deviceId` is how a client ignores its own writes.

Three things the code cannot say for itself:

- **The socket is shared.** Screen-sharing and passcode events arrive on it with attributes
  shaped differently, so a frame that is not a `SyncComplete` is passed over rather than
  refused. Those are not syncs failing to parse, and raising on them would kill a listener
  the moment a user shares their screen.
- **rmfakecloud sends less** than reMarkable does, which is why only `sourceDeviceID` and
  `auth0UserID` are non-null.
- **The account ends a socket every couple of minutes**, measured, whether or not anything
  was said on it — so reconnecting is the feature and not error handling. The loop reuses
  §D12's transient backoff and counts only failures to *open*: one that opened resets the
  count, and `maxTransientRetries` in a row rethrow, that being the network rather than the
  account. A 401 short-circuits it, being the one failure reconnecting cannot fix.

It is not a queue. Whatever happens between two sockets is never delivered, so anything that
must not miss a change still reads the account on its own schedule.

---

## 3. API Shape

**Two tiers, both final classes.** `RemarkableClient` speaks in documents;
`RawRemarkableClient`, reachable as `client.raw`, speaks in hashes and index entries. The
raw tier is genuinely dangerous — a bad root write can orphan an account — so it sits behind
a property rather than mixed into the main surface.

Both are classes with internal constructors, not interfaces over hidden implementations. An
interface was tried and removed for three reasons: adding a method to a published interface
breaks every implementor and this API is still moving; an interface with exactly one internal
implementation is the same construct §D10 rejects, aimed at the consumer's tests instead of
ours; and the seam a consumer needs already exists and is better — point the host options at
a local server, as this library's own tests do, and the real protocol code runs. Mocking the
client instead tests the mock's idea of the protocol. Decoration is the honest cost, and it
lands where it belongs: `SessionOptions.httpClient` exposes OkHttp, so logging, caching, and
extra retries are interceptors rather than delegating methods.

**One identity currency, and both halves are load-bearing.** Reads and edits both take an
`ItemRef`, and every edit returns a new one. An edit locates its item by *both* halves: under
schema 3 an index is hashed from its entries' hashes and not their ids, so two items whose
component blobs happen to match hash identically, and a hash-only lookup would edit whichever
the root listed first. Matching on the pair also means a ref assembled from two different
items is rejected rather than silently applied to one of them.

```kotlin
val renamed = api.rename(entry.ref, "new name")
val starred = api.star(renamed, true)
api.move(starred, Parent.Folder(folder.id))
```

**Illegal states are unrepresentable where it is cheap to arrange.** `Zoom` is sealed, so a
custom fit carries all six of its numbers or none of them, instead of an enum sitting beside
six independently-optional fields. It reads both ways: `DocumentContent.zoom` projects the
flat wire fields back into the sealed type, returning null for the one state the wire can
hold and it cannot, and `withZoom` writes all seven fields together so changing the mode
cannot leave stale `customZoom*` values behind. An input-only sealed type would have forced
anyone editing an existing document to assemble by hand the shape it exists to prevent.

**Options are data classes with defaults, not builders.** Named arguments already give the
readability a builder would, and the immutability is free.

---

## 4. Build, CI, and testing

**Gradle**: Kotlin JVM with serialization, detekt (default rules plus a public-API KDoc
gate), Dokka with `failOnWarning`, `maven-publish`, Kover at 90% line coverage, and
animalsniffer against `gummy-bears` API-21 signatures — the automated enforcement behind
§D2. A custom `checkKdocSummaries` task gates KDoc shape, which detekt's comment rules do
not.

**Dependencies**: `kotlinx-serialization-json`, `kotlinx-coroutines-core`, and OkHttp 5 at
runtime; `kotlin("test")`, `kotlinx-coroutines-test`, and `mockwebserver3` for tests.
`mockwebserver3` is the OkHttp 5 package with the stabilized API, not the feature-frozen
legacy one, which is part of why the library is on OkHttp 5.

**Publishing**: JitPack, via `maven-publish` plus `jitpack.yml`. Central would be the better
home for a widely depended-on library; JitPack costs no account, no staging, and no key
management, at the price of a less conventional coordinate. `cut.yml` is the sole writer to
`main`: it pushes the branch before the tag, so a moved `main` is rejected before a tag
exists.

**Tests run against a local server that stores bytes and does not model the protocol.** An
earlier version reimplemented index serialisation and both hashing rules so tests could
assert against its state. That was a mistake twice over: a second implementation written from
the same assumptions agrees with the first whenever those assumptions are wrong, and its
shared mutable state raced the client's own fan-out, producing a 25% flake rate whose symptom
was a socket timeout in an unrelated test. The server now holds a blob table and a root
pointer; tests assert on **what the client sent**, which is the contract a wire-protocol
client actually has.

Two areas carry more than the rest, because they are where the risk is. **The index format**
is the one place bytes are the contract, since a schema-4 index's own hash is its address, so
both schemas' serialization, both hashing rules, the sort order, and every malformed-line
rejection are pinned explicitly. **`.rm` fixtures** are assembled byte by byte from the
format layout rather than produced by a writer in this library — a writer would only prove
the parser agrees with itself — and the rejection cases are tested because a binary parser
that half-succeeds is worse than one that fails.

---

## 5. Verification against real data

What matters for a reverse-engineered format is what it does against an actual account.

- **Index hashing**: schema-4 serialization and hashing reproduce a real 1408-entry
  production root index byte for byte, and compute the hash reMarkable itself stored it
  under.
- **A full account read**: every item, content, metadata, page, highlight, and template in a
  real account of 622 items. All 622 metadata reads, 600 page-metadata reads, 600 highlight
  reads and 3 template reads succeeded. Two folders raised `ComponentNotFoundException` for
  their `.content`, correctly — the server holds no `.content` blob for them at all.
- **`.rm` parsing**: 578 pages across those items, 149 version 5 and 429 version 6, of which
  **577 parsed** — 39,399 strokes and 1,005,236 points, with 39,027 version 6 blocks. Zero
  unknown pen or colour codes, which is what the strict enums rest on.
- **The one failure is not a parser defect.** That page contains 19,066 U+FFFD replacement
  sequences and decodes cleanly as UTF-8, which a binary file never should: something wrote
  it through a lossy UTF-8 round trip before upload. Its stored hash matches the bytes
  served, so the damage is at rest on the server, and refusing it is the intended behaviour.
- **Pages with no layers are a correct outcome, not silent loss**: some carry no stroke
  blocks at all, and others carry only tombstones — deleted strokes, independently confirmed.
- **Timestamps** were surveyed across 1,468 real `.metadata` blobs before `Entry` was allowed
  to expose them as numbers.
- **The notification socket** was opened against the real account: the handshake, the frame
  shape, and the server's habit of ending an idle socket were all observed rather than
  assumed.

---

## 6. Scope

**In scope**: registration, auth, and sessions; both client tiers; all wire types; the cache
with dump/prune/clear and an LRU bound; the error taxonomy; the device table; schema 3 and 4
reads with schema-4 root writes; the ingestion upload endpoint; `.rm` stroke parsing and
writing (§D13); and the notification socket (§D14). The only HTTP-related public surface is
`SessionOptions.httpClient` (§D3), and there are no test seams (§D10).

**Out of scope** — not deferred, simply not part of this project: CLI/shell, path filetree
and globbing, archive/raw-notebook upload, content-only file replace, automatic token
refresh (device tokens don't expire; session-token refresh is "recreate the client"),
request-concurrency limits, `Flow` listing variants, annotated-PDF export, thumbnails, disk
tree cache, sync10 and the dead `/sync/v2/*` endpoints, `.rmapi` config files, and a
blocking/Java facade.
