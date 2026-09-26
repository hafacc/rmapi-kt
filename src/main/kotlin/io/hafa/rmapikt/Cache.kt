package io.hafa.rmapikt

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromStream
import okio.Sink
import okio.Source
import okio.buffer
import kotlin.io.encoding.Base64

/** the dump format this build writes, and the only one it accepts */
private const val CACHE_VERSION = 2

/** how a dumped body is encoded, so a later format change is legible rather than silent */
private const val CACHE_ENCODING = "base64"

/**
 * what the client knows about one hash
 *
 * Blobs are content-addressed, so a cached entry can never go stale and nothing needs a ttl.
 * The two cases split on size, not on kind: anything small enough is kept whole, and for a
 * blob too large to hold it is enough to remember the server has the hash so an upload can
 * be skipped.
 */
internal sealed interface CacheEntry {
    /** the full contents of a blob small enough to keep */
    class Body(val bytes: ByteArray) : CacheEntry {
        override fun equals(other: Any?): Boolean =
            this === other || (other is Body && bytes.contentEquals(other.bytes))

        override fun hashCode(): Int = bytes.contentHashCode()
    }

    /** a large blob known to be on the server; skip re-uploading it, but still fetch to read */
    data object Exists : CacheEntry
}

// the dump's field order, which is also the order they are written in
private const val VERSION_FIELD = 0
private const val ENCODING_FIELD = 1
private const val BODIES_FIELD = 2
private const val EXISTS_FIELD = 3

/**
 * A least-recently-used cache of blobs by hash, bounded by total size.
 *
 * Values are bytes rather than text. A cache that held decoded text could only admit blobs
 * that decode losslessly, which left the binary ones — `.rm` pages above all — fetching over
 * the network every time; bytes admit every kind, and a caller wanting text decodes on the
 * way out.
 *
 * Every operation is synchronized. The critical sections are pure map manipulation with no
 * suspension and no i/o, so a monitor is both cheaper and simpler than a suspending mutex.
 * Two concurrent readers of the same missing hash will both fetch it and then converge on
 * the same value, which is harmless for content-addressed data and much less machinery than
 * single-flighting.
 *
 * [revision] moves whenever what a dump would hold does, and not when a read only reorders
 * the entries, so a caller that persists the cache can skip a write that would change
 * nothing.
 */
internal class LruCache(private val maxBytes: Long) {
    private val lock = Any()
    private val entries = LinkedHashMap<String, CacheEntry>(INITIAL_CAPACITY, LOAD_FACTOR, true)
    private var currentBytes = 0L
    private var currentRevision = 0L

    operator fun get(hash: String): CacheEntry? = synchronized(lock) { entries[hash] }

    operator fun set(hash: String, entry: CacheEntry): Unit = synchronized(lock) {
        val previous = entries.remove(hash)
        previous?.let { currentBytes -= sizeOf(hash, it) }
        entries[hash] = entry
        currentBytes += sizeOf(hash, entry)
        if (previous != entry) {
            currentRevision++
        }
        // never evict the entry just written, so a single oversized blob is still cached
        while (currentBytes > maxBytes && entries.size > 1) {
            val eldest = entries.entries.iterator().next()
            entries.remove(eldest.key)
            currentBytes -= sizeOf(eldest.key, eldest.value)
            currentRevision++
        }
    }

    fun remove(hash: String): Boolean = synchronized(lock) {
        val removed = entries.remove(hash) ?: return false
        currentBytes -= sizeOf(hash, removed)
        currentRevision++
        return true
    }

    fun clear(): Unit = synchronized(lock) {
        if (entries.isNotEmpty()) {
            currentRevision++
        }
        entries.clear()
        currentBytes = 0
    }

    fun hashes(): Set<String> = synchronized(lock) { entries.keys.toSet() }

    fun byteCount(): Long = synchronized(lock) { currentBytes }

    fun revision(): Long = synchronized(lock) { currentRevision }

    /**
     * Writes the dump into [sink] one entry at a time, and returns the [revision] it holds.
     *
     * Only the list of entries is taken under the lock; encoding and writing happen outside
     * it, so a slow sink never stalls a reader. The bytes are shared rather than copied,
     * which is safe because a body is never written to once cached.
     *
     * Written by hand rather than through a serializer, which would build the whole dump —
     * every body as a base64 string — before writing any of it.
     */
    fun dump(sink: Sink): Long {
        val (snapshot, revision) = synchronized(lock) {
            entries.entries.map { it.key to it.value } to currentRevision
        }
        val out = sink.buffer()
        out.writeUtf8("{\"version\":").writeUtf8(CACHE_VERSION.toString())
            .writeUtf8(",\"encoding\":\"").writeUtf8(CACHE_ENCODING)
            .writeUtf8("\",\"bodies\":{")
        var first = true
        for ((hash, entry) in snapshot) {
            if (entry is CacheEntry.Body) {
                if (!first) out.writeUtf8(",")
                first = false
                // base64 needs no escaping, so only the key goes through the encoder
                out.writeUtf8(JsonPrimitive(hash).toString()).writeUtf8(":\"")
                    .writeUtf8(Base64.encode(entry.bytes)).writeUtf8("\"")
            }
        }
        out.writeUtf8("},\"exists\":[")
        first = true
        for ((hash, entry) in snapshot) {
            if (entry == CacheEntry.Exists) {
                if (!first) out.writeUtf8(",")
                first = false
                out.writeUtf8(JsonPrimitive(hash).toString())
            }
        }
        out.writeUtf8("]}")
        out.flush()
        return revision
    }

    /** Replaces everything held with what [other] holds, as one change. */
    fun replaceWith(other: LruCache): Long {
        val (adopted, bytes) = synchronized(other.lock) { LinkedHashMap(other.entries) to other.currentBytes }
        return synchronized(lock) {
            entries.clear()
            entries.putAll(adopted)
            currentBytes = bytes
            ++currentRevision
        }
    }

    private fun sizeOf(hash: String, entry: CacheEntry): Long = when (entry) {
        is CacheEntry.Body -> (hash.length + entry.bytes.size).toLong()
        CacheEntry.Exists -> hash.length.toLong()
    }

    companion object {
        private const val INITIAL_CAPACITY = 16
        private const val LOAD_FACTOR = 0.75f

        /**
         * Rebuilds a cache from a previous [dump], reading one entry at a time.
         *
         * Each body goes into the cache as it is read, so what is held at once is the cache
         * and one entry of the dump rather than the whole dump beside the cache it becomes.
         *
         * The version and encoding are checked rather than assumed, so a dump written by a
         * different build is refused outright instead of read as though nothing had changed.
         */
        @OptIn(ExperimentalSerializationApi::class)
        fun load(source: Source, maxBytes: Long): LruCache = try {
            wireJson.decodeFromStream(CacheLoader(maxBytes), source.buffer().inputStream())
        } catch (error: SerializationException) {
            throw ValidationException("could not parse cache dump: ${error.message}", null, error)
        } catch (error: IllegalArgumentException) {
            throw ValidationException("could not parse cache dump: ${error.message}", null, error)
        }
    }
}

/**
 * Decodes a dump straight into a cache, never holding the dump as a whole.
 *
 * The version is checked the moment it is read, which is before any body, so a dump from
 * another build is refused before any of it is decoded under the wrong assumptions.
 */
private class CacheLoader(private val maxBytes: Long) : DeserializationStrategy<LruCache> {
    override val descriptor: SerialDescriptor = CacheDumpShape.serializer().descriptor

    override fun deserialize(decoder: Decoder): LruCache = decoder.decodeStructure(descriptor) {
        val cache = LruCache(maxBytes)
        var version: Int? = null
        var encoding: String? = null
        while (true) {
            when (val index = decodeElementIndex(descriptor)) {
                VERSION_FIELD -> version = decodeIntElement(descriptor, index).also(::requireVersion)
                ENCODING_FIELD -> encoding = decodeStringElement(descriptor, index).also(::requireEncoding)
                BODIES_FIELD -> decodeSerializableElement(descriptor, index, BodiesInto(cache))
                EXISTS_FIELD -> decodeSerializableElement(descriptor, index, ExistsInto(cache))
                CompositeDecoder.DECODE_DONE -> break
                else -> throw SerializationException("unexpected field $index in cache dump")
            }
        }
        requireVersion(version)
        requireEncoding(encoding)
        cache
    }

    private fun requireVersion(version: Int?) {
        if (version != CACHE_VERSION) {
            throw ValidationException("cache dump is version $version; expected $CACHE_VERSION")
        }
    }

    private fun requireEncoding(encoding: String?) {
        if (encoding != CACHE_ENCODING) {
            throw ValidationException("cache dump is in $encoding; expected $CACHE_ENCODING")
        }
    }
}

/** The dump's shape, which is only ever read field by field and never built whole. */
@Serializable
private class CacheDumpShape(
    val version: Int,
    val encoding: String,
    val bodies: Map<String, String>,
    val exists: List<String>,
)

private class BodiesInto(private val cache: LruCache) : DeserializationStrategy<Unit> {
    override val descriptor: SerialDescriptor =
        MapSerializer(String.serializer(), String.serializer()).descriptor

    override fun deserialize(decoder: Decoder): Unit = decoder.decodeStructure(descriptor) {
        while (true) {
            val keyIndex = decodeElementIndex(descriptor)
            if (keyIndex == CompositeDecoder.DECODE_DONE) break
            val hash = decodeStringElement(descriptor, keyIndex)
            val body = decodeStringElement(descriptor, decodeElementIndex(descriptor))
            cache[hash] = CacheEntry.Body(Base64.decode(body))
        }
    }
}

private class ExistsInto(private val cache: LruCache) : DeserializationStrategy<Unit> {
    override val descriptor: SerialDescriptor = ListSerializer(String.serializer()).descriptor

    override fun deserialize(decoder: Decoder): Unit = decoder.decodeStructure(descriptor) {
        while (true) {
            val index = decodeElementIndex(descriptor)
            if (index == CompositeDecoder.DECODE_DONE) break
            cache[decodeStringElement(descriptor, index)] = CacheEntry.Exists
        }
    }
}
