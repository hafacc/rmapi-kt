// The raw tier is the protocol surface: index parsing and serialisation, blob staging and
// upload, and the `.rm` page dispatch. Splitting it to satisfy a function count would put
// halves of one format in two files.
@file:Suppress("TooManyFunctions")

package io.hafa.rmapikt

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import okio.Sink
import okio.Source
import okio.buffer
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.io.encoding.Base64

// `<hash>:<type>:<id>:<subfiles>:<size>`
private const val ENTRY_FIELD_COUNT = 5
/** the leading field of a schema 4 info line, which is always this */
private const val INFO_MARKER = "0"

// `<marker>:<id>:<count>:<size>`
private const val INFO_FIELD_COUNT = 4

// how much of a blob is read from the network before the next piece is asked for
private const val READ_CHUNK_BYTES = 64L * 1024

internal const val SCHEMA_SUFFIX = ".docSchema"
internal const val CONTENT_SUFFIX = ".content"
internal const val METADATA_SUFFIX = ".metadata"
internal const val RM_SUFFIX = ".rm"
internal const val HIGHLIGHTS_SUFFIX = ".highlights"
internal const val PAGE_METADATA_SUFFIX = "-metadata.json"
internal const val TEMPLATE_SUFFIX = ".template"

/**
 * Decodes responses leniently, where [wireJson] decodes stored files strictly.
 *
 * A response is read once and never written back, so an unrecognised field in one costs
 * nothing — unlike a stored file, where dropping a key on the way out would destroy it
 * (§D4). Being strict here would break the client the day reMarkable adds a field to a
 * response, in exchange for nothing.
 */
private val responseJson: Json = Json { ignoreUnknownKeys = true }

@Serializable
private data class RootHashResponse(
    val hash: String,
    val generation: Long,
    val schemaVersion: Int,
)

@Serializable
private data class UpdatedRootResponse(val hash: String, val generation: Long)

@Serializable
private data class NativeSimpleEntry(val docID: String, val hash: String)

@Serializable
private data class RootWriteRequest(
    val hash: String,
    val generation: Long,
    val broadcast: Boolean,
)

@Serializable
private data class UploadMeta(@SerialName("file_name") val fileName: String)

/**
 * a file that has been hashed locally but not yet sent
 *
 * Hashing is pure and cheap; uploading is neither. Splitting them lets a caller build every
 * component of a document, learn all their hashes, and only then decide what to send — and
 * it keeps the entry that names the file inseparable from the bytes it describes.
 *
 * The split is also what makes losing a root-generation race survivable. Because staging
 * makes no request, a caller can stage everything once, then retry the commit as many times
 * as it needs, re-sending byte-identical blobs. A combined hash-and-upload would mint a
 * fresh set of orphans on every attempt.
 *
 * A staged file is inert: nothing is sent until [RawRemarkableClient.upload], and the whole
 * transfer runs in the calling coroutine, so concurrency, cancellation, and failure follow
 * the caller's scope.
 */
public class StagedFile internal constructor(
    /** the index entry describing this file */
    public val entry: RawEntry,
    internal val fileName: String,
    internal val bytes: ByteArray,
)


/**
 * the low-level api, which works in hashes rather than documents
 *
 * reMarkable stores an immutable content-addressed filesystem: a file is named by the
 * sha-256 of its bytes, an item is an index listing its component files, and the whole
 * account is an index listing those indexes. Changing anything means writing new files and
 * then moving the root index to point at them.
 *
 * This is genuinely dangerous — a bad root write can orphan every document in the account —
 * so record [getRootHash] before writing, and prefer [RemarkableClient] unless you need
 * this level of control.
 */
public class RawRemarkableClient internal constructor(
    private val http: AuthedHttp,
    private val cache: LruCache,
    private val rawHost: String,
    private val uploadHost: String,
    private val maxCachedBlobBytes: Int,
    private val maxCacheBytes: Long,
) {

    /**
     * the current root index hash, its generation, and the account's index format
     *
     * Pass the generation back to [putRootHash] so the cloud can tell whether anything else
     * moved the root in the meantime.
     */
    public suspend fun getRootHash(): RootInfo {
        val text = http.request("$rawHost/sync/v4/root", method = "GET")
            .use { it.body.string() }
        val root = decodeResponse(RootHashResponse.serializer(), text, "root hash")
        return RootInfo(
            hash = FileHash.ofWire(root.hash),
            generation = root.generation,
            schemaVersion = SchemaVersion.ofWire(root.schemaVersion),
        )
    }

    /**
     * points the account at a new root index
     *
     * @throws GenerationException if [generation] is no longer current, meaning someone else
     *   wrote the root first and the change needs re-applying on top of theirs
     */
    public suspend fun putRootHash(
        hash: FileHash,
        generation: Long,
        broadcast: Boolean = true,
    ): RootUpdate {
        val body = encodeWire(
            RootWriteRequest.serializer(),
            RootWriteRequest(hash.hex, generation, broadcast),
        )
        val text = try {
            http.request(
                "$rawHost/sync/v3/root",
                method = "PUT",
                body = body.toRequestBody(),
            ).use { it.body.string() }
        } catch (_: PreconditionFailedException) {
            // only here is the generation that was rejected actually known
            throw GenerationException(generation)
        }
        val updated = decodeResponse(UpdatedRootResponse.serializer(), text, "updated root hash")
        return RootUpdate(FileHash.ofWire(updated.hash), updated.generation)
    }

    /**
     * writes the raw bytes stored under [hash] into [sink], a piece at a time
     *
     * [fileName] is the logical name, which the cloud validates against the hash.
     *
     * The blob is never held whole, so a file larger than the heap can be read — pass a
     * file's sink to keep it on disk, or a [Buffer] to keep it in memory. [sink] is flushed
     * but not closed.
     *
     * A failure partway leaves whatever had arrived in [sink]; nothing here can take it
     * back out. Nothing is retried once the first byte has been written, for the same
     * reason. The bytes are not checked against [hash].
     */
    public suspend fun getBlob(fileName: String, hash: FileHash, sink: Sink) {
        val cached = cache[hash.hex]
        if (cached is CacheEntry.Body) {
            val buffer = Buffer().write(cached.bytes)
            sink.write(buffer, buffer.size)
            sink.flush()
        } else {
            fetch(fileName, hash, sink)
        }
    }

    /** A blob whole, for the small files this library decodes itself. */
    internal suspend fun getBytes(fileName: String, hash: FileHash): ByteArray {
        val cached = cache[hash.hex]
        return if (cached is CacheEntry.Body) {
            cached.bytes
        } else {
            val buffer = Buffer()
            fetch(fileName, hash, buffer) ?: buffer.readByteArray()
        }
    }

    /** the blob stored under [hash], decoded as utf-8 */
    public suspend fun getText(fileName: String, hash: FileHash): String =
        getBytes(fileName, hash).toString(Charsets.UTF_8)

    /** parses [hash] as an entry index, in either schema 3 or schema 4 */
    public suspend fun getEntries(fileName: String, hash: FileHash): EntryIndex =
        parseEntryIndex(getText(fileName, hash))

    /**
     * parses [hash] as the account's root index
     *
     * The root is the one index that is not an item's, so it is reached by its own method
     * rather than by knowing the name it goes by — which is not the name [stageRootEntries]
     * writes it under either.
     */
    public suspend fun getRootEntries(hash: FileHash): EntryIndex = getEntries(ROOT_SCHEMA, hash)

    /** parses [hash] as a `.content` file */
    public suspend fun getContent(fileName: String, hash: FileHash): Content =
        decodeContent(getText(fileName, hash))

    /** parses [hash] as a `.metadata` file */
    public suspend fun getMetadata(fileName: String, hash: FileHash): Metadata =
        decodeWire(Metadata.serializer(), getText(fileName, hash), "metadata")

    /** parses [hash] as a `.rm` page file; the counterpart of [stageRm] */
    public suspend fun getRm(fileName: String, hash: FileHash): RmFile =
        parseRmFile(getBytes(fileName, hash))

    /** parses [hash] as a `.template` file; the counterpart of [stageTemplate] */
    public suspend fun getTemplate(fileName: String, hash: FileHash): TemplateDefinition =
        decodeWire(TemplateDefinition.serializer(), getText(fileName, hash), "template")

    /** parses [hash] as a page's highlights file; the counterpart of [stageHighlights] */
    public suspend fun getHighlights(fileName: String, hash: FileHash): List<List<Highlight>> =
        decodeWire(HighlightsFile.serializer(), getText(fileName, hash), "highlights").highlights

    /** parses [hash] as a page's layer metadata; the counterpart of [stagePageMetadata] */
    public suspend fun getPageMetadata(fileName: String, hash: FileHash): PageMetadata =
        decodeWire(PageMetadata.serializer(), getText(fileName, hash), "page metadata")

    /** hashes [bytes] locally, ready for [upload] */
    public fun stageFile(id: String, bytes: ByteArray): StagedFile = StagedFile(
        entry = RawEntry(
            type = RawEntryType.File,
            hash = FileHash(sha256Hex(bytes)),
            id = id,
            subfiles = 0,
            size = bytes.size.toLong(),
        ),
        fileName = id,
        bytes = bytes,
    )

    /** @throws IllegalArgumentException if [id] does not end in `.content` */
    public fun stageContent(id: String, content: Content): StagedFile {
        require(id.endsWith(CONTENT_SUFFIX)) { "id '$id' did not end with '$CONTENT_SUFFIX'" }
        return stageFile(id, encodeContent(content).encodeToByteArray())
    }

    /**
     * hashes a `.rm` page locally, ready for [upload]
     *
     * Writing a page the device already has is free: the bytes are identical, so the hash
     * is too, and [upload] skips a blob the store already holds.
     *
     * @throws IllegalArgumentException if [id] does not end in `.rm`
     */
    public fun stageRm(id: String, page: RmFile): StagedFile {
        require(id.endsWith(RM_SUFFIX)) { "id '$id' did not end with '$RM_SUFFIX'" }
        return stageFile(id, serializeRmFile(page))
    }

    /** hashes a template definition locally, ready for [upload] */
    public fun stageTemplate(id: String, definition: TemplateDefinition): StagedFile =
        stageFile(id, encodeWire(TemplateDefinition.serializer(), definition).encodeToByteArray())

    /** hashes a page's layer metadata locally, ready for [upload] */
    public fun stagePageMetadata(id: String, metadata: PageMetadata): StagedFile =
        stageFile(id, encodeWire(PageMetadata.serializer(), metadata).encodeToByteArray())

    /** hashes a page's highlights locally, ready for [upload] */
    public fun stageHighlights(id: String, highlights: List<List<Highlight>>): StagedFile =
        stageFile(id, encodeWire(HighlightsFile.serializer(), HighlightsFile(highlights)).encodeToByteArray())

    /** @throws IllegalArgumentException if [id] does not end in `.metadata` */
    public fun stageMetadata(id: String, metadata: Metadata): StagedFile {
        require(id.endsWith(METADATA_SUFFIX)) { "id '$id' did not end with '$METADATA_SUFFIX'" }
        return stageFile(id, encodeWire(Metadata.serializer(), metadata).encodeToByteArray())
    }

    /**
     * builds an entry index over [entries]
     *
     * The two schemas hash differently: schema 3 hashes the concatenated hashes of the
     * entries, schema 4 hashes the index file itself like any other blob.
     */
    public fun stageEntries(
        id: String,
        entries: List<RawEntry>,
        schemaVersion: SchemaVersion,
    ): StagedFile {
        require(id != ROOT_LIST || schemaVersion != SchemaVersion.V3) {
            "the root index must be schema 4; reMarkable rejects a schema 3 root with a 400 " +
                "\"Software must be updated\""
        }
        val sorted = entries.sortedBy { it.id }
        val totalSize = sorted.sumOf { it.size }
        val body = serializeEntryIndex(id, sorted, totalSize, schemaVersion)

        val hash = when (schemaVersion) {
            // a schema 3 index is addressed by the hash of its entries' hashes, not of the
            // file that lists them
            SchemaVersion.V3 -> sha256Hex(
                sorted.fold(ByteArray(0)) { acc, entry -> acc + entry.hash.hex.hexToBytes() },
            )
            SchemaVersion.V4 -> sha256Hex(body)
        }

        return StagedFile(
            entry = RawEntry(
                type = if (schemaVersion == SchemaVersion.V4) {
                    RawEntryType.File
                } else {
                    RawEntryType.Collection
                },
                hash = FileHash(hash),
                id = id,
                subfiles = sorted.size,
                size = totalSize,
            ),
            fileName = "$id$SCHEMA_SUFFIX",
            bytes = body,
        )
    }

    /**
     * builds the account's root index over [entries]
     *
     * No schema to choose: reMarkable refuses a schema 3 root with a 400 and "Software must
     * be updated", so the root is schema 4 by construction here rather than by a check that
     * fails at the wrong moment. The index also names itself `.` rather than `root`, which
     * is the other thing this spares a caller knowing.
     */
    public fun stageRootEntries(entries: List<RawEntry>): StagedFile =
        stageEntries(ROOT_LIST, entries, SchemaVersion.V4)

    /** sends one staged file; a hash the store already has costs no request */
    public suspend fun upload(staged: StagedFile) {
        val hash = staged.entry.hash.hex
        if (cache[hash] == null) {
            val crc = Base64.encode(crc32cBytes(staged.bytes))
            http.request(
                "$rawHost/sync/v3/files/$hash",
                method = "PUT",
                body = staged.bytes.toRequestBody(),
                headers = mapOf("rm-filename" to staged.fileName, "x-goog-hash" to "crc32c=$crc"),
            ).close()
        }
        // an existence marker must never replace already-cached text: that would turn a
        // future read of this hash back into a network fetch for no reason
        remember(hash, staged.bytes)
    }

    /**
     * hands a file to the server's ingestion endpoint, which builds the document itself
     *
     * This is the mechanism behind [RemarkableClient.uploadPdf] and friends, and is
     * unrelated to [upload], which sends one already-constructed component file.
     *
     * [body] is read to its end but not closed. It is read into memory first, since a
     * request that fails transiently is sent again.
     */
    public suspend fun uploadFile(
        visibleName: String,
        body: Source,
        kind: UploadKind,
    ): ItemRef {
        val bytes = body.buffer().readByteArray()
        val meta = Base64.encode(
            encodeWire(UploadMeta.serializer(), UploadMeta(visibleName)).toByteArray(Charsets.UTF_8),
        )
        val text = http.request(
            "$uploadHost/doc/v2/files",
            method = "POST",
            body = bytes.toRequestBody(),
            headers = mapOf(
                "Content-Type" to kind.mime,
                "rm-meta" to meta,
                "rm-source" to "RoR-Browser",
            ),
        ).use { it.body.string() }
        val entry = decodeResponse(NativeSimpleEntry.serializer(), text, "uploaded file")
        return ItemRef(ItemId.ofWire(entry.docID), FileHash.ofWire(entry.hash))
    }

    /** Keeps a blob whole when it is small enough, and otherwise only that the store has it. */
    private fun remember(hash: String, bytes: ByteArray) {
        cache[hash] = if (bytes.size <= maxCachedBlobBytes) {
            CacheEntry.Body(bytes)
        } else {
            CacheEntry.Exists
        }
    }

    internal fun dumpCache(sink: Sink): Long = cache.dump(sink)

    internal fun loadCache(source: Source): Long = cache.replaceWith(LruCache.load(source, maxCacheBytes))

    internal fun cacheRevision(): Long = cache.revision()

    internal fun clearCache() {
        cache.clear()
    }

    internal fun cachedHashes(): Set<String> = cache.hashes()

    internal fun forget(hash: String) {
        cache.remove(hash)
    }

    /**
     * Streams a blob into [sink] and remembers it.
     *
     * Returns its bytes when it was small enough to keep.
     * The copy for the cache is taken as the blob passes, and dropped the moment the blob
     * outgrows [maxCachedBlobBytes]: buffering a large file whole only to learn it is too
     * large to cache is the cost this exists to avoid. A copy between two [Buffer]s shares
     * segments rather than bytes, so keeping a small blob costs one array at the end.
     *
     * Two concurrent readers of one hash will both fetch and then agree; for
     * content-addressed data that is a wasted request, not a correctness problem.
     */
    private suspend fun fetch(fileName: String, hash: FileHash, sink: Sink): ByteArray? {
        var kept: Buffer? = Buffer()
        http.request(
            "$rawHost/sync/v3/files/${hash.hex}",
            method = "GET",
            headers = mapOf("rm-filename" to fileName),
        ).use { response ->
            val source = response.body.source()
            val piece = Buffer()
            while (true) {
                // the read blocks the calling thread, so this is the only place a cancelled
                // caller can be noticed before the whole blob has arrived
                currentCoroutineContext().ensureActive()
                val read = source.read(piece, READ_CHUNK_BYTES)
                if (read == -1L) break
                kept = kept?.takeIf { it.size + read <= maxCachedBlobBytes }
                    ?.also { piece.copyTo(it, 0, read) }
                sink.write(piece, read)
            }
        }
        sink.flush()
        val bytes = kept?.readByteArray()
        cache[hash.hex] = if (bytes != null) CacheEntry.Body(bytes) else CacheEntry.Exists
        return bytes
    }
}

private fun <T> decodeResponse(
    serializer: kotlinx.serialization.KSerializer<T>,
    text: String,
    what: String,
): T = try {
    responseJson.decodeFromString(serializer, text)
} catch (error: kotlinx.serialization.SerializationException) {
    throw ValidationException("could not parse $what: ${error.message}", text, error)
}

/**
 * Renders an entry index exactly as reMarkable writes it.
 *
 * Every byte matters: in schema 4 the file's own hash is its address, so a stray space or a
 * different sort order produces a different document.
 */
private fun serializeEntryIndex(
    id: String,
    sorted: List<RawEntry>,
    totalSize: Long,
    schemaVersion: SchemaVersion,
): ByteArray {
    val out = StringBuilder()
    out.append(schemaVersion.wire).append('\n')
    if (schemaVersion == SchemaVersion.V4) {
        val name = if (id == ROOT_LIST) "." else id
        out.append("0:").append(name).append(':')
            .append(sorted.size).append(':').append(totalSize).append('\n')
    }
    for (entry in sorted) {
        // schema 4 has no nested indexes, so every line is written as a plain file
        val lineType = if (schemaVersion == SchemaVersion.V4) {
            RawEntryType.File.wire
        } else {
            entry.type.wire
        }
        out.append(entry.hash.hex).append(':')
            .append(lineType).append(':')
            .append(entry.id).append(':')
            .append(entry.subfiles).append(':')
            .append(entry.size).append('\n')
    }
    return out.toString().toByteArray(Charsets.UTF_8)
}

private fun malformed(text: String, message: String): Nothing =
    throw ValidationException(message, text)

private fun String.intOrFail(text: String, message: String): Int =
    toIntOrNull() ?: malformed(text, message)

private fun String.longOrFail(text: String, message: String): Long =
    toLongOrNull() ?: malformed(text, message)

private fun parseEntryLine(line: String): RawEntry {
    val fields = line.split(":")
    if (fields.size != ENTRY_FIELD_COUNT) {
        malformed(line, "index line '$line' was not formatted correctly")
    }
    // a wire layout cannot reorder the way the data class this rule guards would
    @Suppress("DestructuringDeclarationWithTooManyEntries")
    val (hash, type, id, subfiles, size) = fields
    return RawEntry(
        type = RawEntryType.ofWire(type.intOrFail(line, "index line '$line' had a non-numeric type")),
        hash = FileHash.ofWire(hash),
        id = id,
        subfiles = subfiles.intOrFail(line, "index line '$line' had a non-numeric subfile count"),
        size = size.longOrFail(line, "index line '$line' had a non-numeric size"),
    )
}

internal fun parseEntryIndex(text: String): EntryIndex {
    val lines = text.removeSuffix("\n").split("\n")
    val version = SchemaVersion.ofWire(
        lines.first().intOrFail(text, "index version '${lines.first()}' was not a number"),
    )
    val rest = lines.drop(1)
    return when (version) {
        SchemaVersion.V3 -> EntryIndex(rest.filter { it.isNotEmpty() }.map(::parseEntryLine))
        SchemaVersion.V4 -> parseSchemaFourIndex(rest, text)
    }
}

private fun parseSchemaFourIndex(rest: List<String>, text: String): EntryIndex {
    val info = rest.firstOrNull()
        ?: malformed(text, "schema 4 index was missing its info line")
    val fields = info.split(":")
    if (fields.size != INFO_FIELD_COUNT) {
        malformed(text, "schema 4 info line '$info' was not formatted correctly")
    }
    @Suppress("DestructuringDeclarationWithTooManyEntries")
    val (marker, id, count, size) = fields
    if (marker != INFO_MARKER) {
        malformed(text, "schema 4 info line '$info' did not start with '$INFO_MARKER'")
    }
    val entries = rest.drop(1).filter { it.isNotEmpty() }.map(::parseEntryLine)
    val declared = count.intOrFail(text, "schema 4 info line '$info' had a non-numeric count")
    if (declared != entries.size) {
        malformed(text, "schema 4 index declared $declared entries but listed ${entries.size}")
    }
    return EntryIndex(
        entries = entries,
        id = id,
        size = size.longOrFail(text, "schema 4 info line '$info' had a non-numeric size"),
    )
}

/**
 * a parsed `.rm` page file
 *
 * Both variants expose [layers], so reading strokes does not depend on which firmware wrote
 * the page. They stay separate types because a version 6 file additionally carries
 * shared-editing structure that has no equivalent in the older format, and flattening the
 * two would mean pretending that structure doesn't exist.
 */
public sealed interface RmFile {
    /** the version recorded in the file's header */
    public val version: Int

    /** the page's layers, in drawing order */
    public val layers: List<RmLayer>

    /**
     * a version 3 or 5 file, written by firmware before 3.0
     *
     * The whole file is layers and strokes, so nothing else is carried.
     */
    public data class Lines(
        override val version: Int,
        override val layers: List<RmLayer>,
    ) : RmFile

    /**
     * a version 6 file, which is what current firmware writes
     *
     * [layers] holds the strokes and layer names decoded from [blocks]; [blocks] is the
     * whole file, including the text, glyph, and editing-history blocks this library does
     * not interpret.
     *
     * [layers] is a decoded *view* of [blocks], not a second copy of the page.
     * [serializeRmFile] writes [blocks], so editing [layers] and writing the result back
     * changes nothing — to alter a version 6 page you must edit its blocks.
     */
    public data class Scene(
        override val version: Int,
        override val layers: List<RmLayer>,
        /** every block in the file, in order */
        public val blocks: List<RmBlock>,
    ) : RmFile
}

/**
 * parses the header and body of a `.rm` page file
 *
 * The format is undocumented and reverse-engineered, so this is deliberately strict:
 * anything that doesn't match raises [ValidationException] rather than being guessed at,
 * because a wrong guess here yields strokes that look real and are not. The version 6
 * decoding was checked against pages a real device wrote; see `docs/design.md` D13.
 *
 * @throws ValidationException if the header is unrecognised, the version is unsupported, or
 *   the body is truncated or inconsistent
 */
public fun parseRmFile(bytes: ByteArray): RmFile = try {
    readRmFile(bytes)
} catch (error: BufferUnderflowException) {
    // reading past the end of a ByteBuffer raises this rather than returning short, and the
    // documented contract of this function is a ValidationException
    throw ValidationException("the .rm file ended in the middle of a value", null, error)
}

private fun readRmFile(bytes: ByteArray): RmFile {
    requireValid(bytes.size >= RM_HEADER_LENGTH, "") {
        "a .rm file is at least $RM_HEADER_LENGTH bytes, but this was ${bytes.size}"
    }
    val header = bytes.copyOf(RM_HEADER_LENGTH).toString(Charsets.US_ASCII)
    requireValid(header.startsWith(RM_HEADER_PREFIX), header) { "'$header' is not a .rm file header" }
    val version = header.substring(RM_HEADER_PREFIX.length).trim().toIntOrNull()
        ?: throw ValidationException("could not read a version from the header '$header'", header)

    val body = ByteBuffer.wrap(bytes, RM_HEADER_LENGTH, bytes.size - RM_HEADER_LENGTH)
        .slice()
        .order(ByteOrder.LITTLE_ENDIAN)
    return when (version) {
        3, 5 -> RmFile.Lines(version, readV5Layers(body, version, header))
        6 -> readBlocks(body, header).let { RmFile.Scene(version, decodeSceneLayers(it), it) }
        else -> throw ValidationException("`.rm` version $version is not supported", header)
    }
}

/**
 * writes a `.rm` page file back to bytes
 *
 * The inverse of [parseRmFile] for a page this library read: parsing and re-serialising an
 * untouched page yields the identical bytes, and therefore the identical hash. A version 3
 * or 5 page is written from its layers and strokes; a version 6 page is written from its
 * blocks, since [RmFile.Scene.layers] is a decoded view of those and re-encoding it would
 * discard everything the blocks carry that this library does not model.
 */
public fun serializeRmFile(page: RmFile): ByteArray = when (page) {
    is RmFile.Lines -> serializeRmV5(page)
    is RmFile.Scene -> serializeRmV6(page)
}
