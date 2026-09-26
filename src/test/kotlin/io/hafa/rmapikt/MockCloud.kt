package io.hafa.rmapikt

import mockwebserver3.Dispatcher
import mockwebserver3.MockResponse
import mockwebserver3.MockWebServer
import mockwebserver3.RecordedRequest
import okhttp3.OkHttpClient
import okio.Buffer
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * A local server that stores what is PUT and serves what is GET.
 *
 * Deliberately not a model of the cloud. An earlier version reimplemented index
 * serialisation and the two hashing rules so tests could assert against its state — which
 * meant a large part of the suite checked the library against a second implementation
 * written from the same assumptions, and would have agreed with it had those assumptions
 * been wrong. This one holds a blob table and a root pointer, and knows nothing about the
 * protocol: the client sends bytes, they come back unchanged.
 *
 * Tests therefore assert on **what the client sent**, which is the contract a wire-protocol
 * client actually has. Where a test needs an account to already contain something, [seed]
 * builds it with the library's own staging — the same functions `IndexFormatTest` pins
 * byte-for-byte against a real production index.
 *
 * Every field is thread-safe: MockWebServer dispatches each request on its own thread and
 * the client fans out with `async`.
 */
class MockCloud(
    private val schemaVersion: SchemaVersion = SchemaVersion.V4,
) {
    /** one request the server saw */
    class Recorded(
        val method: String,
        val path: String,
        val fileName: String?,
        val body: ByteArray,
    ) {
        val text: String get() = body.toString(Charsets.UTF_8)
    }

    private val blobs = ConcurrentHashMap<String, ByteArray>()
    private val root = AtomicReference("")
    private val staleRead = AtomicLong(-1)
    private val gen = AtomicLong(1)
    private val rejectNext = AtomicBoolean(false)

    /** every request the server saw, in arrival order */
    val received: MutableList<Recorded> = CopyOnWriteArrayList()

    /** how many times each hash was uploaded, to detect redundant writes */
    val uploads: MutableMap<String, Int> = ConcurrentHashMap()

    /** the distinct `Authorization` headers seen, to prove every call is authenticated */
    val authHeaders: MutableSet<String?> = CopyOnWriteArraySet()

    /**
     * Anything this server threw while handling a request.
     *
     * Without this a dispatcher failure is invisible: MockWebServer drops the connection,
     * the client reports a socket timeout, and the test that fails is whichever one happened
     * to be running. Every failure here is surfaced as a 500 carrying the message, and
     * [assertNoServerFailures] fails the test that actually caused it.
     */
    val serverFailures: MutableList<Throwable> = CopyOnWriteArrayList()

    /** Fails if this server threw while handling any request. Call from test teardown. */
    fun assertNoServerFailures() {
        val first = serverFailures.firstOrNull() ?: return
        throw AssertionError("the test server failed while handling a request", first)
    }

    private val staging = stagingClient()

    val server: MockWebServer = MockWebServer()

    init {
        val empty = staging.stageEntries(ROOT_LIST, emptyList(), SchemaVersion.V4)
        blobs[empty.entry.hash.hex] = empty.bytes
        root.set(empty.entry.hash.hex)
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = try {
                route(request)
            } catch (error: Throwable) {
                serverFailures.add(error)
                MockResponse(code = 500, body = "test server failed: $error")
            }
        }
        server.start()
    }

    val host: String get() = server.url("/").toString().removeSuffix("/")

    val rootHash: String get() = root.get()

    val generation: Long get() = gen.get()

    fun close() {
        server.close()
    }

    /** makes the next root write fail as a stale generation, simulating a lost race */
    fun rejectNextRootWrite() {
        rejectNext.set(true)
    }

    /** serves the next root read at [generation], simulating a read that resolves late */
    fun staleNextRootRead(generation: Long) {
        staleRead.set(generation)
    }

    /** the bytes stored under [hash], whether seeded or uploaded by the client */
    fun blob(hash: String): ByteArray? = blobs[hash]

    /** stores [file] as though some other client had uploaded it, so this one has not cached it */
    fun seedBlob(file: StagedFile) {
        blobs[file.entry.hash.hex] = file.bytes
    }

    /** every request the client sent for a file whose logical name ends with [suffix] */
    fun requestsFor(suffix: String): List<Recorded> =
        received.filter { it.fileName?.endsWith(suffix) == true }

    /** the body of the last file the client uploaded whose name ends with [suffix] */
    fun lastUpload(suffix: String): String =
        requestsFor(suffix).last { it.method == "PUT" }.text

    /** the metadata the client most recently wrote */
    fun uploadedMetadata(): Metadata =
        decodeWire(Metadata.serializer(), lastUpload(METADATA_SUFFIX), "metadata")

    /** the content the client most recently wrote */
    fun uploadedContent(): Content = decodeContent(lastUpload(CONTENT_SUFFIX))

    /** the component entries of the item stored at [hash] */
    fun itemEntries(hash: String): List<RawEntry> =
        parseEntryIndex(blobs.getValue(hash).toString(Charsets.UTF_8)).entries

    /** the entries of the account's current root index */
    fun rootEntries(): List<RawEntry> =
        parseEntryIndex(blobs.getValue(root.get()).toString(Charsets.UTF_8)).entries

    /**
     * Installs an item so the account already contains it.
     *
     * [extraFiles] is keyed by the path within the item — `<pageid>.rm` for a page, or a
     * name already starting with the item id for a sibling file such as `.template`.
     */
    fun seed(
        metadata: Metadata,
        content: Content,
        id: String = UUID.randomUUID().toString(),
        extraFiles: Map<String, ByteArray> = emptyMap(),
    ): ItemRef {
        val files = buildList {
            add(staging.stageContent("$id$CONTENT_SUFFIX", content))
            add(staging.stageMetadata("$id$METADATA_SUFFIX", metadata))
            for ((path, bytes) in extraFiles) {
                add(staging.stageFile(if (path.startsWith(id)) path else "$id/$path", bytes))
            }
        }
        files.forEach { blobs[it.entry.hash.hex] = it.bytes }

        val item = staging.stageEntries(id, files.map { it.entry }, schemaVersion)
        blobs[item.entry.hash.hex] = item.bytes

        val updated = staging.stageEntries(
            ROOT_LIST,
            rootEntries() + item.entry,
            SchemaVersion.V4,
        )
        blobs[updated.entry.hash.hex] = updated.bytes
        root.set(updated.entry.hash.hex)
        return ItemRef(ItemId(id), item.entry.hash)
    }

    private fun route(request: RecordedRequest): MockResponse {
        val body = request.body?.toByteArray() ?: ByteArray(0)
        received.add(
            Recorded(request.method, request.target, request.headers["rm-filename"], body),
        )
        authHeaders.add(request.headers["Authorization"])
        val target = request.target
        return when {
            target == "/sync/v4/root" -> json(
                """{"hash":"${root.get()}",""" +
                    """"generation":${staleRead.getAndSet(-1).takeIf { it >= 0 } ?: gen.get()},""" +
                    """"schemaVersion":${schemaVersion.wire}}""",
            )
            target == "/sync/v3/root" -> writeRoot(body)
            target.startsWith("/sync/v3/files/") -> blobEndpoint(request, target, body)
            target == "/doc/v2/files" -> ingest(body)
            else -> MockResponse(code = 404, body = "no route for $target")
        }
    }

    private fun writeRoot(body: ByteArray): MockResponse {
        val stale = "{\"message\":\"precondition failed\"}\n"
        if (rejectNext.getAndSet(false)) {
            return MockResponse(code = 412, body = stale)
        }
        val sent = decodeWire(RootWrite.serializer(), body.toString(Charsets.UTF_8), "root write")
        return if (sent.generation != gen.get()) {
            MockResponse(code = 412, body = stale)
        } else {
            root.set(sent.hash)
            json("""{"hash":"${sent.hash}","generation":${gen.incrementAndGet()}}""")
        }
    }

    private fun blobEndpoint(
        request: RecordedRequest,
        target: String,
        body: ByteArray,
    ): MockResponse {
        val hash = target.removePrefix("/sync/v3/files/")
        return if (request.method == "GET") {
            blobs[hash]
                ?.let { MockResponse.Builder().code(200).body(Buffer().write(it)).build() }
                ?: MockResponse(code = 404, body = "no blob $hash")
        } else {
            blobs[hash] = body
            uploads.merge(hash, 1, Int::plus)
            MockResponse(code = 200, body = "")
        }
    }

    private fun ingest(body: ByteArray): MockResponse {
        val id = UUID.randomUUID().toString()
        return json("""{"docID":"$id","hash":"${sha256Hex(body + id.toByteArray())}"}""")
    }

    private fun json(body: String) = MockResponse.Builder()
        .code(200)
        .addHeader("Content-Type", "application/json")
        .body(body)
        .build()
}

@kotlinx.serialization.Serializable
internal data class RootWrite(val hash: String, val generation: Long, val broadcast: Boolean)

/**
 * A client for staging only, which never makes a request.
 *
 * `AuthedHttp` needs an `OkHttpClient` to construct, but hashing and index building are
 * pure. One shared instance: JUnit builds a fresh test object per method, so constructing a
 * client per instance leaks a dispatcher thread pool and a connection-pool cleanup thread
 * for every test in the run, and nothing closes them.
 */
private val stagingOnlyHttp = OkHttpClient()

internal fun stagingClient(): RawRemarkableClient = RawRemarkableClient(
    AuthedHttp(stagingOnlyHttp, "unused", 0),
    LruCache(Long.MAX_VALUE),
    "http://unused",
    "http://unused",
    Int.MAX_VALUE,
    Long.MAX_VALUE,
)
