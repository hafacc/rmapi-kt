package io.hafa.rmapikt

import okio.Buffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

private fun LruCache.dump(): String = Buffer().also { dump(it) }.readUtf8()

private fun load(dump: String, maxBytes: Long): LruCache = LruCache.load(Buffer().writeUtf8(dump), maxBytes)

class CacheTest {
    private fun body(value: String) = CacheEntry.Body(value.encodeToByteArray())

    @Test
    fun `eviction is least recently used, and reading counts as use`() {
        // keys are 1 byte each here, so the bound is dominated by the values
        val cache = LruCache(maxBytes = 10)
        cache["a"] = body("1")
        cache["b"] = body("long")
        assertEquals(body("1"), cache["a"])

        // rewriting a key replaces its size rather than adding to it, so nothing is evicted
        cache["b"] = body("longer")
        assertEquals(body("1"), cache["a"])

        // "a" was read most recently, so "b" is the one that goes
        cache["c"] = body("short")
        assertNull(cache["b"])
        assertEquals(body("1"), cache["a"])

        cache.remove("c")
        cache["d"] = body("short")
        assertNull(cache["c"])
        assertEquals(body("1"), cache["a"])

        cache.clear()
        assertEquals(0L, cache.byteCount())
        assertNull(cache["a"])
    }

    @Test
    fun `a single entry larger than the bound is still cached`() {
        val cache = LruCache(maxBytes = 4)
        cache["k"] = body("a value far longer than the bound")
        assertTrue(cache["k"] != null, "evicting the entry just written would make it uncacheable")
    }

    @Test
    fun `an existence marker costs only its key`() {
        val cache = LruCache(maxBytes = 1000)
        cache["abc"] = CacheEntry.Exists
        assertEquals(3L, cache.byteCount())
    }

    @Test
    fun `size counts characters of the key and the value`() {
        val cache = LruCache(maxBytes = 1000)
        cache["key"] = body("value")
        assertEquals(3L + 5, cache.byteCount())
    }

    @Test
    fun `a dump round trips through load`() {
        val cache = LruCache(maxBytes = 1000)
        cache["hash1"] = body("""{"visibleName":"a"}""")
        cache["hash2"] = CacheEntry.Exists
        cache["hash3"] = body("more")

        val restored = load(cache.dump(), maxBytes = 1000)
        assertEquals(body("""{"visibleName":"a"}"""), restored["hash1"])
        assertEquals(CacheEntry.Exists, restored["hash2"])
        assertEquals(body("more"), restored["hash3"])
        assertEquals(cache.hashes(), restored.hashes())
    }

    @Test
    fun `a dump from an unrecognised version is refused rather than misread`() {
        val error = assertFailsWith<ValidationException> {
            load("""{"version":99,"text":{},"exists":[]}""", maxBytes = 1000)
        }
        assertTrue("99" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `a corrupt dump is refused`() {
        assertFailsWith<ValidationException> { load("not a cache", maxBytes = 1000) }
    }

    @Test
    fun `loading into a smaller bound evicts rather than overflowing`() {
        val cache = LruCache(maxBytes = 1000)
        for (index in 1..20) {
            cache["key$index"] = body("value$index")
        }
        val restored = load(cache.dump(), maxBytes = 40)
        assertTrue(restored.byteCount() <= 40, "restored size was ${restored.byteCount()}")
        assertTrue(restored.hashes().isNotEmpty())
    }

    @Test
    fun `a dump returns the revision it holds, and loads back from the same buffer`() {
        val cache = LruCache(maxBytes = 1000)
        cache["hash1"] = body("""{"visibleName":"a"}""")
        cache["hash2"] = CacheEntry.Exists
        cache["hash3"] = body("more")

        val sink = Buffer()
        assertEquals(cache.revision(), cache.dump(sink))

        val restored = LruCache.load(sink, maxBytes = 1000)
        assertEquals(body("""{"visibleName":"a"}"""), restored["hash1"])
        assertEquals(CacheEntry.Exists, restored["hash2"])
        assertEquals(cache.hashes(), restored.hashes())
    }

    @Test
    fun `the revision moves when the contents do, and only then`() {
        val cache = LruCache(maxBytes = 10)
        val empty = cache.revision()
        cache.clear()
        assertEquals(empty, cache.revision(), "clearing nothing changes nothing")

        cache["a"] = body("1")
        val one = cache.revision()
        assertTrue(one > empty)

        cache["a"] = body("1")
        cache["a"]
        assertEquals(one, cache.revision(), "rewriting the same bytes, or reading them, is no change")

        cache["b"] = body("long long")
        val evicted = cache.revision()
        assertNull(cache["a"])
        assertTrue(evicted > one)

        cache.remove("b")
        assertTrue(cache.revision() > evicted)
    }

    @Test
    fun `replacing the contents is one change that brings the other cache's entries`() {
        val cache = LruCache(maxBytes = 1000)
        cache["a"] = body("1")
        val before = cache.revision()
        val other = LruCache(maxBytes = 1000).apply { this["b"] = body("2") }

        val replaced = cache.replaceWith(other)
        assertEquals(cache.revision(), replaced)
        assertTrue(cache.revision() > before)
        assertEquals(setOf("b"), cache.hashes())
        assertEquals(1L + 1, cache.byteCount())
    }
}
