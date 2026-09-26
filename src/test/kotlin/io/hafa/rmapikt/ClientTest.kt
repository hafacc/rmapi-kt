package io.hafa.rmapikt

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okio.Buffer
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Assertions are about what the client put on the wire, not about the state of a modelled
 * server — see [MockCloud] for why.
 */
class ClientTest {
    private val cloud = MockCloud()

    // its own client: these tests start and stop a server per test, and a connection
    // pooled to a closed one can be handed to whichever server next binds that port
    private val http = OkHttpClient()

    private fun client(
        maxGenerationRetries: Int = 10,
        server: MockCloud = cloud,
    ): RemarkableClient = session(
        SessionToken("session-token"),
        SessionOptions(
            rawHost = server.host,
            uploadHost = server.host,
            maxGenerationRetries = maxGenerationRetries,
            httpClient = http,
        ),
    )

    @AfterTest
    fun shutdown() {
        cloud.close()
        http.connectionPool.evictAll()
        http.dispatcher.executorService.shutdown()
        // a server-side failure would otherwise surface as a timeout in an unrelated test
        cloud.assertNoServerFailures()
    }

    private fun documentMetadata(name: String, parent: Parent = Parent.Root) = Metadata(
        visibleName = name,
        parent = parent,
        pinned = false,
        type = EntryType.Document,
        lastModified = "1700000000000",
        lastOpened = "1700000000000",
    )

    private fun documentContent(fileType: FileType = FileType.Pdf) = DocumentContent(
        coverPageNumber = -1,
        documentMetadata = DocumentMetadata(),
        extraMetadata = emptyMap(),
        fileType = fileType,
        fontName = "",
        lineHeight = -1,
        orientation = Orientation.Portrait,
        pageCount = 1,
        textAlignment = TextAlignment.Justify,
        textScale = 1.0,
    )

    private fun uploadedNames(): Set<String> = cloud.received
        .filter { it.method == "PUT" && it.fileName != null }
        .map { it.fileName!!.substringAfterLast('.') }
        .toSet()

    @Test
    fun `raw reads and writes the root without being told what it is called`() = runTest {
        val api = client()
        val folder = api.putFolder("things")
        val root = api.refreshRoot()

        val entries = api.raw.getRootEntries(root.hash).entries
        assertTrue(entries.any { it.id == folder.id.value })
        // restaging what was just read reproduces the root, so neither name was guessed
        assertEquals(root.hash, api.raw.stageRootEntries(entries).entry.hash)
    }

    @Test
    fun `listing an empty account returns nothing`() = runTest {
        assertEquals(emptyMap(), client().metadataByRef())
    }

    @Test
    fun `listing returns each item's metadata, keyed by ref`() = runTest {
        val document = cloud.seed(documentMetadata("a document"), documentContent())
        cloud.seed(
            Metadata("a folder", Parent.Root, true, EntryType.Collection, "1700000000001"),
            CollectionContent(tags = Tags.Structured(listOf(Tag("work", 1700000000000)))),
        )

        val listed = client().metadataByRef()
        assertEquals(2, listed.size)
        assertEquals("a document", listed.getValue(document).visibleName)
        assertEquals(Parent.Root, listed.getValue(document).parent)
        assertEquals(1700000000000, listed.getValue(document).modifiedAt)

        val folder = listed.values.single { it.type == EntryType.Collection }
        assertEquals("a folder", folder.visibleName)
        assertTrue(folder.pinned)
    }

    @Test
    fun `listing does not fetch content`() = runTest {
        cloud.seed(documentMetadata("a document"), documentContent())
        client().metadataByRef()
        assertEquals(
            emptyList(),
            cloud.requestsFor(CONTENT_SUFFIX),
            "a listing pays for metadata only",
        )
    }

    @Test
    fun `listRefs does not fetch any component files`() = runTest {
        cloud.seed(documentMetadata("a document"), documentContent())
        assertEquals(1, client().listRefs().size)
        assertEquals(emptyList(), cloud.requestsFor(METADATA_SUFFIX))
        assertEquals(emptyList(), cloud.requestsFor(CONTENT_SUFFIX))
    }

    @Test
    fun `the wire's stringified timestamps read as numbers`() = runTest {
        cloud.seed(documentMetadata("a document"), documentContent())
        val metadata = client().metadataByRef().values.single()
        assertEquals(1700000000000, metadata.modifiedAt)
        assertEquals(1700000000000, metadata.openedAt)
        assertNull(metadata.createdAt, "this item was seeded without a createdTime")
    }

    @Test
    fun `putPdf uploads every component file and commits a new root`() = runTest {
        val pdf = "%PDF-1.4 fake".toByteArray()
        val ref = client().putPdf("my doc", source(pdf), PutOptions(starred = true))

        assertEquals(setOf("content", "metadata", "pagedata", "pdf", "docSchema"), uploadedNames())

        val metadata = cloud.uploadedMetadata()
        assertEquals("my doc", metadata.visibleName)
        assertEquals(Parent.Root, metadata.parent)
        assertTrue(metadata.pinned)
        assertEquals(EntryType.Document, metadata.type)

        val content = assertIs<DocumentContent>(cloud.uploadedContent())
        assertEquals(FileType.Pdf, content.fileType)
        assertEquals(1, content.pageCount)
        assertEquals(1, content.pages?.size, "the device needs a page list even before rendering")
        assertEquals(pdf.size.toString(), content.sizeInBytes)
        assertEquals(1, content.formatVersion)

        assertEquals(listOf(ref.hash.hex), cloud.rootEntries().map { it.hash.hex })
    }

    @Test
    fun `putPdf mints a well formed id and timestamps rather than fixed values`() = runTest {
        val ref = client().putPdf("doc", source(byteArrayOf(1)))
        val metadata = cloud.uploadedMetadata()
        assertTrue(
            Regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
                .matches(ref.id.value),
            ref.id.value,
        )
        assertTrue(metadata.lastModified.toLong() > 0)
        assertEquals(metadata.lastModified, metadata.createdTime)
        assertEquals("0", metadata.lastOpened)
    }

    @Test
    fun `putEpub with no options writes the documented defaults`() = runTest {
        client().putEpub("plain", source(byteArrayOf(1)))

        val content = assertIs<DocumentContent>(cloud.uploadedContent())
        assertEquals(FileType.Epub, content.fileType)
        assertEquals(-1, content.coverPageNumber)
        assertEquals("", content.fontName, "empty selects the device font")
        assertEquals(-1, content.lineHeight, "-1 selects the device line height")
        assertEquals(125, content.margins, "spelled out, so a changed default is visible here")
        assertEquals(Orientation.Portrait, content.orientation)
        assertEquals(TextAlignment.Justify, content.textAlignment)
        assertEquals(1.0, content.textScale)
        assertEquals(ZoomMode.BestFit, content.zoomMode)
        assertNull(content.viewBackgroundFilter)
        assertEquals(emptyMap(), content.extraMetadata)

        val metadata = cloud.uploadedMetadata()
        assertEquals(Parent.Root, metadata.parent)
        assertFalse(metadata.pinned, "a put document is not starred by default")
    }

    @Test
    fun `put options reach the content file`() = runTest {
        client().putEpub(
            "styled",
            source(byteArrayOf(1)),
            PutOptions(
                lineHeight = 180,
                margins = 50,
                textScale = 1.5,
                fontName = "EB Garamond",
                textAlignment = TextAlignment.Left,
                orientation = Orientation.Landscape,
                zoom = Zoom.Custom(
                    scale = 2.0,
                    centerX = 0.0,
                    centerY = 0.0,
                    pageWidth = 1404.0,
                    pageHeight = 1872.0,
                    orientation = Orientation.Portrait,
                ),
                viewBackgroundFilter = BackgroundFilter.FullPage,
                tags = listOf("reading"),
                authors = listOf("an author"),
                title = "a title",
            ),
        )
        val content = assertIs<DocumentContent>(cloud.uploadedContent())
        assertEquals(FileType.Epub, content.fileType)
        assertEquals(180, content.lineHeight)
        assertEquals(50, content.margins)
        assertEquals(1.5, content.textScale)
        assertEquals("EB Garamond", content.fontName)
        assertEquals(TextAlignment.Left, content.textAlignment)
        assertEquals(Orientation.Landscape, content.orientation)
        assertEquals(ZoomMode.CustomFit, content.zoomMode)
        assertEquals(2.0, content.customZoomScale)
        assertEquals(1404.0, content.customZoomPageWidth)
        assertEquals(BackgroundFilter.FullPage, content.viewBackgroundFilter)
        assertEquals(listOf("reading"), content.tags.names)
        assertEquals(listOf("an author"), content.documentMetadata.authors)
        assertEquals("a title", content.documentMetadata.title)
    }

    @Test
    fun `putFolder uploads only content and metadata`() = runTest {
        client().putFolder("a folder")
        assertEquals(setOf("content", "metadata", "docSchema"), uploadedNames())
        assertEquals(EntryType.Collection, cloud.uploadedMetadata().type)
        assertIs<CollectionContent>(cloud.uploadedContent())
    }

    @Test
    fun `putting into a folder records the folder as the parent`() = runTest {
        val api = client()
        val folder = api.putFolder("parent")
        api.putPdf("child", source(byteArrayOf(1)), PutOptions(parent = Parent.Folder(folder.id)))
        assertEquals(Parent.Folder(folder.id), cloud.uploadedMetadata().parent)
    }

    @Test
    fun `rename rewrites the name and bumps the metadata version`() = runTest {
        val api = client()
        val original = api.putPdf("before", source(byteArrayOf(1)))
        val renamed = api.rename(original, "after")

        assertNotEquals(original.hash, renamed.hash, "an edit produces a new hash")
        assertEquals(original.id, renamed.id, "the id is stable across an edit")

        val metadata = cloud.uploadedMetadata()
        assertEquals("after", metadata.visibleName)
        assertEquals(1, metadata.version, "the device is told the change came from elsewhere")
        assertEquals(true, metadata.metadatamodified)
    }

    @Test
    fun `move and trash rewrite the parent`() = runTest {
        val api = client()
        val folder = api.putFolder("dest")
        val document = api.putPdf("doc", source(byteArrayOf(1)))

        val moved = api.move(document, Parent.Folder(folder.id))
        assertEquals(Parent.Folder(folder.id), cloud.uploadedMetadata().parent)

        api.trash(moved)
        assertEquals(Parent.Trash, cloud.uploadedMetadata().parent)
    }

    @Test
    fun `star toggles the pinned flag`() = runTest {
        val api = client()
        val document = api.putPdf("doc", source(byteArrayOf(1)))
        val starred = api.star(document, true)
        assertTrue(cloud.uploadedMetadata().pinned)
        api.star(starred, false)
        assertTrue(!cloud.uploadedMetadata().pinned)
    }

    @Test
    fun `setMetadata reaches a field the named edits do not`() = runTest {
        val api = client()
        val document = api.putPdf("doc", source(byteArrayOf(1)))
        api.setMetadata(document, api.getMetadata(document).copy(lastOpenedPage = 7, source = "com.example"))

        val metadata = cloud.uploadedMetadata()
        assertEquals(7, metadata.lastOpenedPage)
        assertEquals("com.example", metadata.source)
        assertEquals("doc", metadata.visibleName, "the rest of the metadata is untouched")
    }

    @Test
    fun `setMetadata marks the change as coming from off the device`() = runTest {
        val api = client()
        val document = api.putPdf("doc", source(byteArrayOf(1)))
        api.setMetadata(document, api.getMetadata(document).copy(version = 41, metadatamodified = false))

        val metadata = cloud.uploadedMetadata()
        assertEquals(42, metadata.version, "the returned version is bumped, not written as given")
        assertEquals(true, metadata.metadatamodified)
    }

    @Test
    fun `an edit returns a ref that can be used directly for the next call`() = runTest {
        val api = client()
        val first = api.putPdf("one", source(byteArrayOf(1)))
        val renamed = api.rename(first, "two")
        val starred = api.star(renamed, true)
        val moved = api.move(starred, Parent.Trash)
        assertEquals(first.id, moved.id)
        assertEquals("two", api.getMetadata(moved).visibleName)
        assertEquals(Parent.Trash, api.getMetadata(moved).parent)
    }

    @Test
    fun `an edit refuses a ref whose id does not match its hash`() = runTest {
        val api = client()
        val target = api.putPdf("target", source(byteArrayOf(1)))
        val other = api.putPdf("other", source(byteArrayOf(2)))

        // the hash names a real item, but paired with a different item's id
        val mismatched = ItemRef(other.id, target.hash)
        assertFailsWith<HashNotFoundException> { api.rename(mismatched, "renamed") }
        assertEquals("target", api.getMetadata(target).visibleName, "the item is untouched")
    }

    @Test
    fun `bulkMove refuses a ref whose id does not match its hash`() = runTest {
        val api = client()
        val target = api.putPdf("target", source(byteArrayOf(1)))
        val other = api.putPdf("other", source(byteArrayOf(2)))

        val mismatched = ItemRef(other.id, target.hash)
        assertEquals(emptyMap(), api.bulkMove(listOf(mismatched), Parent.Trash))
        assertEquals(Parent.Root, api.getMetadata(target).parent, "the item is untouched")
    }

    @Test
    fun `editing an unknown hash reports it rather than corrupting the root`() = runTest {
        val error = assertFailsWith<HashNotFoundException> {
            client().rename(
                ItemRef(ItemId("00000000-0000-4000-8000-000000000000"), FileHash("f".repeat(64))),
                "nope",
            )
        }
        assertEquals("f".repeat(64), error.ref.hash.hex)
        assertNull(error.currentHash, "the item is gone, not merely moved on")
    }

    @Test
    fun `a ref left behind by another client says where the item went`() = runTest {
        val api = client()
        val stale = api.putPdf("doc", source(byteArrayOf(1)))
        val moved = api.rename(stale, "renamed")

        val error = assertFailsWith<HashNotFoundException> { api.rename(stale, "again") }
        assertEquals(moved.hash, error.currentHash, "so a caller can re-read rather than give up")
        assertEquals(stale.id, error.ref.id)
    }

    @Test
    fun `setDocumentContent applies the caller's edit and leaves metadata alone`() = runTest {
        val api = client()
        val document = api.putPdf("doc", source(byteArrayOf(1)))
        api.setDocumentContent(document, api.getDocumentContent(document).copy(textScale = 2.0, lineHeight = 200))

        val content = assertIs<DocumentContent>(cloud.uploadedContent())
        assertEquals(2.0, content.textScale)
        assertEquals(200, content.lineHeight)
        assertEquals("doc", cloud.uploadedMetadata().visibleName, "metadata is untouched")
    }

    @Test
    fun `updating with the wrong type is refused`() = runTest {
        val api = client()
        val folder = api.putFolder("a folder")
        val error = assertFailsWith<ValidationException> {
            api.getDocumentContent(folder)
        }
        assertTrue("Collection" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `setCollectionContent edits a folder's tags`() = runTest {
        val api = client()
        val folder = api.putFolder("a folder")
        val content = api.getCollectionContent(folder)
        api.setCollectionContent(
            folder,
            content.copy(tags = Tags.Structured(listOf(Tag("archive", 1700000000000)))),
        )
        assertEquals(listOf("archive"), assertIs<CollectionContent>(cloud.uploadedContent()).tags.names)
    }

    @Test
    fun `content whose shape contradicts its metadata is reported, not a cast failure`() = runTest {
        // metadata says document, but the content has no fileType so it reads as a folder's
        val ref = cloud.seed(documentMetadata("odd"), CollectionContent())
        val error = assertFailsWith<ValidationException> {
            client().getDocumentContent(ref)
        }
        assertTrue("Collection" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `setPages rewrites the pages it names and leaves the rest alone`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to serializeRmFile(rmPage(2f)),
                "page-a-metadata.json" to "{}".toByteArray(),
            ),
        )
        val api = client()
        val updated = api.setPages(ref, mapOf("page-a" to rmPage(7f)))

        val pages = api.getPages(updated)
        assertEquals(rmPage(7f), pages.getValue("page-a"))
        assertEquals(rmPage(2f), pages.getValue("page-b"), "an unnamed page is untouched")
        assertTrue(
            cloud.itemEntries(updated.hash.hex).any { it.id.endsWith("page-a-metadata.json") },
            "a sibling file survives the index rewrite",
        )
        assertEquals("a notebook", api.getMetadata(updated).visibleName, "metadata is untouched")
    }

    @Test
    fun `a read, an edit and a write compose without touching bytes`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a")),
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        val api = client()

        val recoloured = api.getPages(ref).mapValues { (_, file) ->
            val lines = assertIs<RmFile.Lines>(file)
            lines.copy(
                layers = lines.layers.map { layer ->
                    layer.copy(strokes = layer.strokes.map { it.copy(color = RmColor.Blue) })
                },
            )
        }
        val updated = api.setPages(ref, recoloured)

        val stroke = assertIs<RmFile.Lines>(api.getPages(updated).getValue("page-a"))
            .layers.single().strokes.single()
        assertEquals(RmColor.Blue, stroke.color)
        assertEquals(RmPen.FinelinerV5, stroke.pen, "the rest of the stroke round-tripped")
        assertEquals(1f, stroke.points.single().x)
    }

    @Test
    fun `writing back a whole page map only uploads the pages that changed`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to serializeRmFile(rmPage(2f)),
            ),
        )
        val api = client()
        val pages = api.getPages(ref).toMutableMap()
        pages["page-a"] = rmPage(7f)
        api.setPages(ref, pages)

        val written = cloud.requestsFor(".rm").filter { it.method == "PUT" }
        assertEquals(1, written.size, "an unedited page re-serialised to the hash it was read from")
        assertTrue(written.single().fileName!!.endsWith("page-a.rm"), "and page-a is the one sent")
        assertContentEquals(serializeRmFile(rmPage(7f)), written.single().body)
    }

    @Test
    fun `setPages writes the first strokes onto a declared blank page`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "blank")),
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        val api = client()
        val updated = api.setPage(ref, "blank", rmPage(7f))

        assertEquals(rmPage(7f), api.getPage(updated, "blank"))
        assertEquals(rmPage(1f), api.getPage(updated, "page-a"), "the drawn page is untouched")
        assertEquals(setOf("page-a", "blank"), api.getPages(updated).keys)
    }

    @Test
    fun `setPages refuses a page the document does not declare`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a")),
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        val error = assertFailsWith<ValidationException> {
            client().setPages(ref, mapOf("page-z" to rmPage(2f)))
        }
        assertTrue("page-z" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `one bad page id in a batch writes none of them`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to serializeRmFile(rmPage(2f)),
            ),
        )
        val api = client()
        val error = assertFailsWith<ValidationException> {
            api.setPages(ref, mapOf("page-a" to rmPage(7f), "page-z" to rmPage(8f)))
        }
        assertTrue("page-z" in error.message.orEmpty(), error.message.orEmpty())
        assertTrue(cloud.received.none { it.method == "PUT" }, "the valid page is not written")
        assertEquals(rmPage(1f), api.getPage(ref, "page-a"), "and the document is unchanged")
    }

    @Test
    fun `setPages with nothing to write makes no request at all`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a")),
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        assertEquals(ref, client().setPages(ref, emptyMap()))
        assertTrue(cloud.received.none { it.method == "PUT" })
    }

    @Test
    fun `setPage rewrites the one page it names`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to serializeRmFile(rmPage(2f)),
            ),
        )
        val api = client()
        val updated = api.setPage(ref, "page-a", rmPage(7f))
        assertEquals(rmPage(7f), api.getPage(updated, "page-a"))
        assertEquals(rmPage(2f), api.getPage(updated, "page-b"), "the other page is untouched")
    }

    @Test
    fun `bulkMove rewrites many items in a single root write`() = runTest {
        val api = client()
        val folder = api.putFolder("dest")
        val first = api.putPdf("one", source(byteArrayOf(1)))
        val second = api.putPdf("two", source(byteArrayOf(2)))

        val before = cloud.generation
        val moved = api.bulkMove(listOf(first, second), Parent.Folder(folder.id))

        assertEquals(setOf(first, second), moved.keys)
        assertEquals(before + 1, cloud.generation, "one root write, not one per item")
        assertEquals(3, cloud.rootEntries().size, "the folder and both documents remain")
        for (newRef in moved.values) {
            assertEquals(Parent.Folder(folder.id), api.getMetadata(newRef).parent)
        }
    }

    @Test
    fun `bulkTrash moves everything to the trash`() = runTest {
        val api = client()
        val first = api.putPdf("one", source(byteArrayOf(1)))
        val second = api.putPdf("two", source(byteArrayOf(2)))
        val moved = api.bulkTrash(listOf(first, second))
        for (newRef in moved.values) {
            assertEquals(Parent.Trash, api.getMetadata(newRef).parent)
        }
    }

    @Test
    fun `purge drops an item from the root instead of moving it`() = runTest {
        val api = client()
        val doomed = api.putPdf("doomed", source(byteArrayOf(1)))
        val kept = api.putPdf("kept", source(byteArrayOf(2)))

        val metadataWrites = { cloud.requestsFor(METADATA_SUFFIX).count { it.method == "PUT" } }
        val writesBefore = metadataWrites()
        api.purge(doomed)

        assertEquals(listOf(kept), api.listRefs())
        assertEquals(writesBefore, metadataWrites(), "the item is dropped, not marked as anything")
    }

    @Test
    fun `purge refuses a ref the root does not list`() = runTest {
        val api = client()
        val kept = api.putPdf("kept", source(byteArrayOf(1)))
        val stale = ItemRef(kept.id, FileHash("a".repeat(64)))

        val error = assertFailsWith<HashNotFoundException> { api.purge(stale) }
        assertEquals(kept.hash, error.currentHash, "the item is still there, under its own hash")
        assertEquals(listOf(kept), api.listRefs(), "and the root is untouched")
    }

    @Test
    fun `purgeTrash removes a trashed folder and everything inside it`() = runTest {
        val api = client()
        val folder = api.putFolder("box")
        val inside = api.putPdf("inside", source(byteArrayOf(1)), PutOptions(Parent.Folder(folder.id)))
        val nested = api.putFolder("inner box", Parent.Folder(folder.id))
        val deeper = api.putPdf("deeper", source(byteArrayOf(2)), PutOptions(Parent.Folder(nested.id)))
        val kept = api.putPdf("kept", source(byteArrayOf(3)))
        // only the folder is moved; its contents keep pointing at it, as the device leaves them
        val trashed = api.trash(folder)

        val before = cloud.generation
        val purged = api.purgeTrash()

        assertEquals(setOf(trashed, inside, nested, deeper), purged)
        assertEquals(listOf(kept), api.listRefs())
        assertEquals(before + 1, cloud.generation, "one root write for the whole tree")
    }

    @Test
    fun `purgeTrash leaves the contents of a folder that is not in the trash`() = runTest {
        val api = client()
        val folder = api.putFolder("box")
        val inside = api.putPdf("inside", source(byteArrayOf(1)), PutOptions(Parent.Folder(folder.id)))
        val trashed = api.trash(api.putPdf("elsewhere", source(byteArrayOf(2))))

        assertEquals(setOf(trashed), api.purgeTrash())
        assertEquals(setOf(folder, inside), api.listRefs().toSet())
    }

    @Test
    fun `purgeTrash with an empty trash leaves the root and its generation alone`() = runTest {
        val api = client()
        val kept = api.putPdf("kept", source(byteArrayOf(1)))
        val before = cloud.rootEntries().map { it.hash.hex }
        val rootWrites = { cloud.received.count { it.path.endsWith("/root") && it.method == "PUT" } }
        val writesBefore = rootWrites()

        assertEquals(emptySet(), api.purgeTrash())
        assertEquals(before, cloud.rootEntries().map { it.hash.hex }, "the root is unchanged")
        assertEquals(writesBefore, rootWrites(), "and no generation was burned")
        assertEquals(listOf(kept), api.listRefs())
    }

    @Test
    fun `bulkPurge drops many items in a single root write`() = runTest {
        val api = client()
        val first = api.putPdf("one", source(byteArrayOf(1)))
        val second = api.putPdf("two", source(byteArrayOf(2)))
        val kept = api.putPdf("three", source(byteArrayOf(3)))

        val before = cloud.generation
        val purged = api.bulkPurge(listOf(first, second))

        assertEquals(setOf(first, second), purged)
        assertEquals(listOf(kept), api.listRefs())
        assertEquals(before + 1, cloud.generation, "one root write, not one per item")
    }

    @Test
    fun `bulkPurge leaves a ref the root no longer lists out of its result`() = runTest {
        val api = client()
        val present = api.putPdf("one", source(byteArrayOf(1)))
        val stale = ItemRef(present.id, FileHash("a".repeat(64)))
        val absent = ItemRef(
            ItemId("00000000-0000-4000-8000-000000000000"),
            FileHash("b".repeat(64)),
        )
        val rootWrites = { cloud.received.count { it.path.endsWith("/root") && it.method == "PUT" } }
        val writesBefore = rootWrites()

        assertEquals(emptySet(), api.bulkPurge(listOf(stale, absent)))
        assertEquals(listOf(present), api.listRefs(), "nothing is removed on its id alone")
        assertEquals(writesBefore, rootWrites(), "and no generation was burned")
    }

    @Test
    fun `bulkMove leaves a ref it could not find out of its result`() = runTest {
        val api = client()
        val folder = api.putFolder("dest")
        val present = api.putPdf("one", source(byteArrayOf(1)))
        val absent = ItemRef(
            ItemId("00000000-0000-4000-8000-000000000000"),
            FileHash("a".repeat(64)),
        )

        val result = api.bulkMove(listOf(present, absent), Parent.Folder(folder.id))
        assertEquals(setOf(present), result.keys, "the caller subtracts to find the absent one")
    }

    @Test
    fun `a lost race is retried and then succeeds`() = runTest {
        val api = client(maxGenerationRetries = 3)
        val document = api.putPdf("doc", source(byteArrayOf(1)))

        cloud.rejectNextRootWrite()
        api.rename(document, "renamed after a conflict")
        assertEquals("renamed after a conflict", cloud.uploadedMetadata().visibleName)
    }

    @Test
    fun `a lost race is surfaced once the retries are used up`() = runTest {
        val api = client(maxGenerationRetries = 0)
        val document = api.putPdf("doc", source(byteArrayOf(1)))

        cloud.rejectNextRootWrite()
        assertFailsWith<GenerationException> { api.rename(document, "never lands") }
    }

    @Test
    fun `retrying a conflict does not re-upload blobs it already sent`() = runTest {
        val api = client(maxGenerationRetries = 3)
        val pdf = "some bytes".toByteArray()
        val document = api.putPdf("doc", source(pdf))
        val pdfHash = sha256Hex(pdf)
        assertEquals(1, cloud.uploads[pdfHash])

        cloud.rejectNextRootWrite()
        api.rename(document, "renamed")
        assertEquals(1, cloud.uploads[pdfHash], "the pdf is unchanged by a rename")
    }

    @Test
    fun `getPdf writes the stored bytes into a sink and a missing component is reported`() = runTest {
        val api = client()
        val pdf = "%PDF fake".toByteArray()
        val ref = api.putPdf("doc", source(pdf))
        val sink = Buffer()
        api.getPdf(ref, sink)
        assertContentEquals(pdf, sink.readByteArray())

        val error = assertFailsWith<ComponentNotFoundException> { api.getEpub(ref, Buffer()) }
        assertEquals(DocumentComponent.Epub, error.component)
    }

    @Test
    fun `getEpub writes the stored bytes into a sink`() = runTest {
        val api = client()
        val epub = "PK fake epub".toByteArray()
        val ref = api.putEpub("book", source(epub))
        assertContentEquals(epub, Buffer().also { api.getEpub(ref, it) }.readByteArray())
    }

    @Test
    fun `getContent and getMetadata read the item's own files`() = runTest {
        val api = client()
        val ref = api.putPdf("named", source(byteArrayOf(1)))
        assertEquals("named", api.getMetadata(ref).visibleName)
        assertIs<DocumentContent>(api.getContent(ref))
    }

    @Test
    fun `the component files are reachable through the raw client`() = runTest {
        val api = client()
        val pdf = "%PDF".toByteArray()
        val ref = api.putPdf("doc", source(pdf))

        val entries = api.raw.getEntries("${ref.id.value}$SCHEMA_SUFFIX", ref.hash).entries
        assertEquals(
            setOf("content", "metadata", "pagedata", "pdf"),
            entries.map { it.id.substringAfterLast('.') }.toSet(),
        )
        val pdfEntry = entries.single { it.id.endsWith(".pdf") }
        assertContentEquals(pdf, api.raw.blob(pdfEntry.id, pdfEntry.hash))
    }

    @Test
    fun `getPages returns each page keyed by page id`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to serializeRmFile(rmPage(2f)),
                // per-page json is not a stroke file
                "page-a-metadata.json" to "{}".toByteArray(),
            ),
        )
        val pages = client().getPages(ref)
        assertEquals(setOf("page-a", "page-b"), pages.keys)
        assertEquals(rmPage(1f), pages.getValue("page-a"))
        assertEquals(rmPage(2f), pages.getValue("page-b"))
    }

    @Test
    fun `getPages fails the whole document when one page is unreadable`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to byteArrayOf(9, 9, 9),
            ),
        )
        assertFailsWith<ValidationException> { client().getPages(ref) }

        // the bytes remain reachable through the tier whose currency they are
        val entries = client().raw.getEntries("${ref.id.value}$SCHEMA_SUFFIX", ref.hash).entries
        val page = entries.single { it.id.endsWith("page-b.rm") }
        assertContentEquals(byteArrayOf(9, 9, 9), client().raw.blob(page.id, page.hash))
    }

    @Test
    fun `getPages parses each page's strokes`() = runTest {
        val page = java.io.ByteArrayOutputStream().apply {
            val text = "reMarkable .lines file, version=3"
            write(text.toByteArray(Charsets.US_ASCII))
            repeat(43 - text.length) { write(' '.code) }
            fun int(value: Int) = write(
                java.nio.ByteBuffer.allocate(4)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(value).array(),
            )
            fun float(value: Float) = write(
                java.nio.ByteBuffer.allocate(4)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN).putFloat(value).array(),
            )
            int(1); int(1)
            int(4); int(0); int(0); float(2f)
            int(1)
            float(3f); float(4f); float(0f); float(0f); float(0f); float(1f)
        }.toByteArray()

        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a")),
            extraFiles = mapOf("page-a.rm" to page),
        )
        val parsed = client().getPages(ref)
        val stroke = assertIs<RmFile.Lines>(parsed.getValue("page-a"))
            .layers.single().strokes.single()
        assertEquals(RmPen.Fineliner, stroke.pen)
        assertEquals(3f, stroke.points.single().x)
    }

    @Test
    fun `getPage fetches only the page asked for`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to serializeRmFile(rmPage(2f)),
            ),
        )
        assertEquals(rmPage(2f), client().getPage(ref, "page-b"))
        assertEquals(1, cloud.requestsFor(".rm").size, "page-a was never downloaded")
    }

    @Test
    fun `getPage survives a document another page has broken`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-b.rm" to byteArrayOf(9, 9, 9),
            ),
        )
        assertFailsWith<ValidationException> { client().getPages(ref) }
        assertEquals(rmPage(1f), client().getPage(ref, "page-a"), "the readable page still reads")
    }

    @Test
    fun `getPage reports a page the document does not declare`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a")),
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        val error = assertFailsWith<ValidationException> { client().getPage(ref, "page-z") }
        assertTrue("page-z" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `getPage returns null for a declared page nothing has been drawn on`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "blank")),
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        assertNull(client().getPage(ref, "blank"), "a page with no strokes yet is not an error")
    }

    @Test
    fun `raw getRm parses a page, mirroring raw stageRm`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a")),
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        val raw = client().raw
        val entry = raw.getEntries("${ref.id.value}$SCHEMA_SUFFIX", ref.hash)
            .entries.single { it.id.endsWith("page-a.rm") }
        assertEquals(rmPage(1f), raw.getRm(entry.id, entry.hash))
    }

    @Test
    fun `getPagedata reads one template name per page and drops the trailing blank`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            id = id,
            extraFiles = mapOf("$id.pagedata" to "Grid\n\n".toByteArray()),
        )
        assertEquals(listOf("Grid", ""), client().getPagedata(ref), "the second page has none")
    }

    @Test
    fun `getPagedata is empty for a document without the file`() = runTest {
        val ref = cloud.seed(documentMetadata("a pdf"), documentContent())
        assertEquals(emptyList(), client().getPagedata(ref))
    }

    @Test
    fun `setPagedata round-trips and creates the file when absent`() = runTest {
        val ref = cloud.seed(documentMetadata("a pdf"), documentContent())
        val api = client()
        val updated = api.setPagedata(ref, listOf("Grid", "", "Dots"))
        assertEquals(listOf("Grid", "", "Dots"), api.getPagedata(updated))
        assertEquals("Grid\n\nDots\n", cloud.lastUpload(".pagedata"), "one line each, newline terminated")
    }

    @Test
    fun `getPageMetadata reads layer names and ignores the page file beside it`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a", "page-b")),
            id = id,
            extraFiles = mapOf(
                "page-a.rm" to serializeRmFile(rmPage(1f)),
                "page-a-metadata.json" to """{"layers":[{"name":"Layer 1"},{"name":"ink"}]}""".toByteArray(),
            ),
        )
        val api = client()
        val all = api.getPageMetadata(ref)
        assertEquals(setOf("page-a"), all.keys, "the .rm beside it is not metadata")
        assertEquals(listOf("Layer 1", "ink"), all.getValue("page-a").layers.map { it.name })
        assertNull(api.getPageMetadata(ref, "page-b"), "a declared page with none reads as null")
        assertFailsWith<ValidationException> { api.getPageMetadata(ref, "page-z") }
    }

    @Test
    fun `setPageMetadata adds a file for a page that had none`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook).copy(pages = listOf("page-a")),
            id = id,
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        val api = client()
        val metadata = PageMetadata(listOf(PageLayer("sketch")))
        val updated = api.setPageMetadata(ref, "page-a", metadata)

        assertEquals(metadata, api.getPageMetadata(updated, "page-a"))
        assertEquals(rmPage(1f), api.getPage(updated, "page-a"), "the page itself is untouched")
    }

    @Test
    fun `getHighlights returns every page that carries them, keyed by page id`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val page = """
            {"highlights":[[{"text":"a passage","color":3,"start":10,"length":9,
             "rects":[{"x":1.0,"y":2.0,"width":3.0,"height":4.0}]}]]}
        """.trimIndent()
        val ref = cloud.seed(
            documentMetadata("a pdf"),
            documentContent().copy(pages = listOf("page-a", "page-b")),
            id = id,
            extraFiles = mapOf("$id.highlights/page-a.json" to page.toByteArray()),
        )
        val api = client()
        val all = api.getHighlights(ref)
        assertEquals(setOf("page-a"), all.keys, "page-b has no highlights file")

        val fragment = all.getValue("page-a").single().single()
        assertEquals("a passage", fragment.text)
        assertEquals(3, fragment.color)
        assertEquals(HighlightRect(1.0, 2.0, 3.0, 4.0), fragment.rects.single())
        assertNull(api.getHighlights(ref, "page-b"), "a declared page with none reads as null")
        assertFailsWith<ValidationException> { api.getHighlights(ref, "page-z") }
    }

    @Test
    fun `setHighlights adds a file for a page that had none and leaves others alone`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val existing = """{"highlights":[[{"text":"kept","color":1,"start":0,"length":4,"rects":[]}]]}"""
        val ref = cloud.seed(
            documentMetadata("a pdf"),
            documentContent().copy(pages = listOf("page-a", "page-b")),
            id = id,
            extraFiles = mapOf("$id.highlights/page-a.json" to existing.toByteArray()),
        )
        val api = client()
        val added = listOf(listOf(Highlight("new", 2, 5, 3, listOf(HighlightRect(0.0, 0.0, 1.0, 1.0)))))
        val updated = api.setHighlights(ref, "page-b", added)

        assertEquals(added, api.getHighlights(updated, "page-b"))
        assertEquals("kept", api.getHighlights(updated, "page-a")!!.single().single().text)
    }

    @Test
    fun `setHighlights refuses a page the document does not declare`() = runTest {
        val ref = cloud.seed(
            documentMetadata("a pdf"),
            documentContent().copy(pages = listOf("page-a")),
        )
        val error = assertFailsWith<ValidationException> {
            client().setHighlights(ref, "page-z", listOf(listOf(Highlight("x", 1, 0, 1, emptyList()))))
        }
        assertTrue("page-z" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `setHighlights with nothing to write makes no request at all`() = runTest {
        val ref = cloud.seed(documentMetadata("a pdf"), documentContent())
        assertEquals(ref, client().setHighlights(ref, emptyMap()))
        assertTrue(cloud.received.none { it.method == "PUT" })
    }

    @Test
    fun `setTemplate rewrites a template's definition`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val definition = """
            {"name":"grid","author":"reMarkable","iconData":"c3Zn","categories":["Planning"],
             "orientation":"portrait","templateVersion":"1.0.0","formatVersion":1,
             "items":[{"type":"line"}]}
        """.trimIndent()
        val ref = cloud.seed(
            Metadata(
                visibleName = "a template",
                parent = Parent.Root,
                pinned = false,
                type = EntryType.Template,
                lastModified = "1700000000000",
            ),
            CollectionContent(),
            id = id,
            extraFiles = mapOf("$id.template" to definition.toByteArray()),
        )
        val api = client()
        val updated = api.setTemplate(ref, api.getTemplate(ref).copy(name = "dots"))

        assertEquals("dots", api.getTemplate(updated).name)
        assertEquals("reMarkable", api.getTemplate(updated).author, "the rest round-trips")
    }

    @Test
    fun `bulkMove with nothing to move leaves the root and its generation alone`() = runTest {
        val api = client()
        api.putPdf("untouched", source(byteArrayOf(1)))
        val before = cloud.rootEntries().map { it.hash.hex }
        val rootWrites = { cloud.received.count { it.path.endsWith("/root") && it.method == "PUT" } }
        val writesBefore = rootWrites()
        val absent = ItemRef(ItemId("22222222-2222-4222-8222-222222222222"), FileHash("a".repeat(64)))

        assertEquals(emptyMap(), api.bulkMove(listOf(absent), Parent.Trash))
        assertEquals(before, cloud.rootEntries().map { it.hash.hex }, "the root is unchanged")
        assertEquals(writesBefore, rootWrites(), "and no generation was burned")
    }

    @Test
    fun `refreshRoot keeps its view when a stale read resolves after a newer write`() = runTest {
        val api = client()
        api.putPdf("first", source(byteArrayOf(1)))
        val current = api.refreshRoot()

        cloud.staleNextRootRead(current.generation - 1)
        assertEquals(current, api.refreshRoot(), "the older read is refused")

        // and the cache is still usable: an edit commits against the generation it kept
        val ref = api.listRefs().single()
        assertEquals("renamed", api.getMetadata(api.rename(ref, "renamed")).visibleName)
    }

    @Test
    fun `a document the device never opened still has readable, writable pages`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        // pages is null until the device opens the document, but the .rm files are there
        val ref = cloud.seed(
            documentMetadata("never opened"),
            documentContent(FileType.Notebook),
            id = id,
            extraFiles = mapOf("page-a.rm" to serializeRmFile(rmPage(1f))),
        )
        val api = client()
        assertEquals(setOf("page-a"), api.getPages(ref).keys)
        assertEquals(rmPage(1f), api.getPage(ref, "page-a"), "the single read agrees with the map")
        val updated = api.setPage(ref, "page-a", rmPage(7f))
        assertEquals(rmPage(7f), api.getPage(updated, "page-a"))
    }

    @Test
    fun `zoom reads back as the sealed type it was written as`() = runTest {
        val custom = Zoom.Custom(
            scale = 1.5,
            centerX = 10.0,
            centerY = 20.0,
            pageWidth = 1404.0,
            pageHeight = 1872.0,
            orientation = Orientation.Landscape,
        )
        val api = client()
        val ref = api.putPdf("doc", source(byteArrayOf(1)), PutOptions(zoom = custom))
        assertEquals(custom, api.getDocumentContent(ref).zoom)
    }

    @Test
    fun `changing zoom clears the custom numbers it no longer needs`() = runTest {
        val api = client()
        val ref = api.putPdf(
            "doc",
            source(byteArrayOf(1)),
            PutOptions(
                zoom = Zoom.Custom(1.5, 10.0, 20.0, 1404.0, 1872.0, Orientation.Portrait),
            ),
        )
        val content = api.getDocumentContent(ref)
        val updated = api.setDocumentContent(ref, content.withZoom(Zoom.BestFit))

        val after = api.getDocumentContent(updated)
        assertEquals(Zoom.BestFit, after.zoom)
        assertNull(after.customZoomScale, "a stale custom number would outlive its mode")
        assertNull(after.customZoomOrientation)
    }

    @Test
    fun `setPagedata refuses a folder`() = runTest {
        val api = client()
        val folder = api.putFolder("a folder")
        val error = assertFailsWith<ValidationException> { api.setPagedata(folder, listOf("Grid")) }
        assertTrue("Collection" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `a read refuses the item kinds its write would refuse`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val template = cloud.seed(
            Metadata("a template", Parent.Root, false, EntryType.Template, "1700000000000"),
            // a template's .content is empty, so it decodes as a folder's
            CollectionContent(),
            id = id,
            extraFiles = mapOf("$id.template" to templateJson().toByteArray()),
        )
        val api = client()
        val document = api.putPdf("doc", source(byteArrayOf(1)))

        // the shape says folder, but the metadata says template: the read must agree with
        // setCollectionContent, which refuses it
        assertFailsWith<ValidationException> { api.getCollectionContent(template) }
        assertFailsWith<ValidationException> { api.getDocumentContent(template) }
        assertFailsWith<ValidationException> { api.getTemplate(document) }
        assertFailsWith<ValidationException> { api.setTemplate(document, definitionOf(api, template)) }
    }

    @Test
    fun `setTemplate refuses an item that is not a template`() = runTest {
        val api = client()
        val document = api.putPdf("doc", source(byteArrayOf(1)))
        val template = TemplateDefinition(
            name = "grid",
            author = "reMarkable",
            iconData = "c3Zn",
            categories = listOf("Planning"),
            orientation = Orientation.Portrait,
            templateVersion = "1.0.0",
            items = listOf(JsonObject(emptyMap())),
            formatVersion = 1,
        )
        val error = assertFailsWith<ValidationException> { api.setTemplate(document, template) }
        assertTrue("Document" in error.message.orEmpty(), error.message.orEmpty())
    }

    @Test
    fun `a template is identified by its metadata and read from its template file`() = runTest {
        val id = java.util.UUID.randomUUID().toString()
        val definition = """
            {"name":"grid","author":"reMarkable","iconData":"c3Zn","categories":["Planning"],
             "orientation":"portrait","templateVersion":"1.0.0","formatVersion":1,
             "items":[{"type":"line"}],"constants":[{"offsetX":121},
             {"lineWidth":"templateWidth - (offsetX * 2)"}],"supportedScreens":["rm2"]}
        """.trimIndent()
        val ref = cloud.seed(
            Metadata(
                visibleName = "a template",
                parent = Parent.Root,
                pinned = false,
                type = EntryType.Template,
                lastModified = "1700000000000",
                source = "com.remarkable.methods",
                new = true,
            ),
            CollectionContent(),
            id = id,
            extraFiles = mapOf("$id.template" to definition.toByteArray()),
        )

        val api = client()
        val metadata = api.metadataByRef().values.single()
        assertEquals(EntryType.Template, metadata.type, "metadata is what says it is a template")
        assertEquals("com.remarkable.methods", metadata.source)

        val loaded = api.getTemplate(ref)
        assertEquals("grid", loaded.name)
        assertEquals("1.0.0", loaded.templateVersion)
        assertEquals(2, loaded.constants?.size)
    }

    @Test
    fun `a document survives an export and import round trip`() = runTest {
        val api = client()
        val pdf = "%PDF original".toByteArray()
        val original = api.putPdf("original", source(pdf))

        val archive = Buffer()
        api.exportArchive(original, archive)
        val restored = api.importArchive(archive)

        assertTrue(archive.exhausted(), "the archive is read to its end")
        assertNotEquals(original.id, restored.id, "a restore gets a fresh id by default")
        assertContentEquals(pdf, Buffer().also { api.getPdf(restored, it) }.readByteArray())
        assertEquals("original", api.getMetadata(restored).visibleName)
        assertEquals(2, cloud.rootEntries().size)

        val paths = api.raw
            .getEntries("${restored.id.value}$SCHEMA_SUFFIX", restored.hash).entries.map { it.id }
        assertTrue(
            paths.all { it.startsWith(restored.id.value) },
            "every archived path is rewritten to the new id: $paths",
        )
    }

    @Test
    fun `importArchive can override the name and parent, and always mints an id`() = runTest {
        val api = client()
        val folder = api.putFolder("dest")
        val original = api.putPdf("original", source(byteArrayOf(1)))
        val archive = Buffer().also { api.exportArchive(original, it) }

        val restored = api.importArchive(
            archive,
            ImportOptions(
                parent = Parent.Folder(folder.id),
                visibleName = "renamed on restore",
            ),
        )
        assertNotEquals(original.id, restored.id, "a restore is a new item, never the old one")
        val metadata = api.getMetadata(restored)
        assertEquals("renamed on restore", metadata.visibleName)
        assertEquals(Parent.Folder(folder.id), metadata.parent)
    }

    @Test
    fun `an archive without metadata is refused`() = runTest {
        val empty = java.io.ByteArrayOutputStream().also {
            java.util.zip.ZipOutputStream(it).use { zip ->
                zip.putNextEntry(java.util.zip.ZipEntry("something.txt"))
                zip.write(byteArrayOf(1))
                zip.closeEntry()
            }
        }.toByteArray()
        assertFailsWith<ValidationException> { client().importArchive(Buffer().write(empty)) }
    }

    @Test
    fun `the upload family hands the file to the ingestion endpoint`() = runTest {
        val api = client()
        api.uploadPdf("a pdf", source(byteArrayOf(1)))
        api.uploadEpub("an epub", source(byteArrayOf(2)))
        api.uploadFolder("a folder")

        assertEquals(3, cloud.received.count { it.path == "/doc/v2/files" })
        assertEquals(
            0,
            cloud.received.count { it.path == "/sync/v3/root" },
            "ingestion does not touch the root index",
        )
    }

    @Test
    fun `refreshRoot picks up a change made by someone else`() = runTest {
        val api = client()
        assertEquals(emptyMap(), api.metadataByRef())

        cloud.seed(documentMetadata("added behind our back"), documentContent())
        assertEquals(emptyMap(), api.metadataByRef(), "the cached root is still the old one")

        api.refreshRoot()
        assertEquals(1, api.metadataByRef().size)
    }

    @Test
    fun `a text blob is served from cache on the second read`() = runTest {
        val api = client()
        val ref = api.putPdf("doc", source(byteArrayOf(1)))
        api.getMetadata(ref)
        val before = cloud.received.count { it.method == "GET" }
        api.getMetadata(ref)
        assertEquals(before, cloud.received.count { it.method == "GET" }, "the second read is cached")
    }

    @Test
    fun `reading a binary blob as text does not corrupt a later read of its bytes`() = runTest {
        val binary = "reMarkable .lines file, version=6".toByteArray() +
            byteArrayOf(0xFF.toByte(), 0xFE.toByte(), 0x00, 0x81.toByte(), 0xC0.toByte())
        val ref = cloud.seed(
            documentMetadata("a notebook"),
            documentContent(FileType.Notebook),
            extraFiles = mapOf("page-a.rm" to binary),
        )
        val raw = client().raw
        val page = raw.getEntries("${ref.id.value}$SCHEMA_SUFFIX", ref.hash).entries
            .single { it.id.endsWith(".rm") }

        val asText = raw.getText(page.id, page.hash)
        assertTrue(
            !asText.toByteArray(Charsets.UTF_8).contentEquals(binary),
            "the premise of this test is that decoding these bytes is lossy",
        )
        assertContentEquals(binary, raw.blob(page.id, page.hash), "a lossy read must not poison the blob")
        assertEquals(page.hash.hex, sha256Hex(raw.blob(page.id, page.hash)))
    }

    @Test
    fun `a write does not refetch the index it just wrote`() = runTest {
        val api = client()
        val ref = api.putPdf("doc", source(byteArrayOf(1)))
        val before = cloud.requestsFor(SCHEMA_SUFFIX).count { it.method == "GET" }

        api.rename(ref, "renamed")
        assertEquals(
            before,
            cloud.requestsFor(SCHEMA_SUFFIX).count { it.method == "GET" },
            "the root and item indexes were staged locally, so reading them back is a cache hit",
        )
    }

    @Test
    fun `a blob under the size limit is served from cache, a larger one is refetched`() = runTest {
        val api = session(
            SessionToken("session-token"),
            SessionOptions(
                rawHost = cloud.host,
                uploadHost = cloud.host,
                httpClient = http,
                maxCachedBlobBytes = 16,
            ),
        )
        val raw = api.raw
        val small = raw.stageFile("small.rm", ByteArray(16) { 1 })
        val large = raw.stageFile("large.rm", ByteArray(17) { 2 })
        raw.upload(small)
        raw.upload(large)

        val before = cloud.received.size
        assertContentEquals(ByteArray(16) { 1 }, raw.blob("small.rm", small.entry.hash))
        assertEquals(before, cloud.received.size, "the small blob never left the cache")

        assertContentEquals(ByteArray(17) { 2 }, raw.blob("large.rm", large.entry.hash))
        assertEquals(before + 1, cloud.received.size, "the large one is known, not held")
    }

    private fun smallBlobClient(): RemarkableClient = session(
        SessionToken("session-token"),
        SessionOptions(
            rawHost = cloud.host,
            uploadHost = cloud.host,
            httpClient = http,
            maxCachedBlobBytes = 16,
        ),
    )

    @Test
    fun `a blob streamed into a sink arrives whole, and a small one is cached on the way`() = runTest {
        val raw = smallBlobClient().raw
        val small = raw.stageFile("small.rm", ByteArray(16) { 1 })
        cloud.seedBlob(small)

        val first = Buffer()
        raw.getBlob("small.rm", small.entry.hash, first)
        assertContentEquals(ByteArray(16) { 1 }, first.readByteArray())

        val before = cloud.received.size
        val second = Buffer()
        raw.getBlob("small.rm", small.entry.hash, second)
        assertContentEquals(ByteArray(16) { 1 }, second.readByteArray())
        assertEquals(before, cloud.received.size, "the second read came out of the cache")
    }

    @Test
    fun `a blob too large to cache is streamed without being kept`() = runTest {
        val api = smallBlobClient()
        val raw = api.raw
        // several read chunks long, so the copy for the cache is abandoned partway through
        val contents = ByteArray(300_000) { (it % 251).toByte() }
        val large = raw.stageFile("large.pdf", contents)
        cloud.seedBlob(large)

        val sink = Buffer()
        raw.getBlob("large.pdf", large.entry.hash, sink)
        assertContentEquals(contents, sink.readByteArray())

        val dump = Json.parseToJsonElement(Buffer().also { api.dumpCache(it) }.readUtf8()).jsonObject
        assertEquals(
            listOf(large.entry.hash.hex),
            (dump.getValue("exists") as JsonArray).map { it.jsonPrimitive.content },
            "only that the store has it is remembered",
        )
        val before = cloud.received.size
        assertContentEquals(contents, raw.blob("large.pdf", large.entry.hash))
        assertEquals(before + 1, cloud.received.size, "so reading it again is a request")
    }

    @Test
    fun `the cache revision moves with what is cached, not with what is read`() = runTest {
        val api = client()
        val ref = api.putPdf("doc", source(byteArrayOf(1)))
        val dumped = api.dumpCache(Buffer())
        assertEquals(api.cacheRevision, dumped)

        api.getMetadata(ref)
        assertEquals(dumped, api.cacheRevision, "a cache hit changes nothing worth writing")

        api.rename(ref, "renamed")
        assertTrue(api.cacheRevision > dumped, "a write caches new blobs")
    }

    @Test
    fun `the cache round trips through a sink and a source into a new session`() = runTest {
        val api = client()
        val ref = api.putPdf("doc", source(byteArrayOf(1)))
        val dump = Buffer()
        api.dumpCache(dump)

        val warm = client()
        val loaded = warm.loadCache(dump)
        assertEquals(warm.cacheRevision, loaded)
        val before = cloud.received.size
        assertEquals("doc", warm.getMetadata(ref).visibleName)
        assertTrue(
            cloud.received.size - before < 3,
            "a warm cache should avoid refetching the item's files",
        )
    }

    @Test
    fun `an unreadable dump is refused and leaves the cache as it was`() = runTest {
        val api = client()
        api.putPdf("doc", source(byteArrayOf(1)))
        val held = cachedHashes(api)
        assertFailsWith<ValidationException> { api.loadCache(Buffer().writeUtf8("not a cache")) }
        assertEquals(held, cachedHashes(api))
    }

    private fun cachedHashes(api: RemarkableClient): Set<String> {
        val dump = Json.parseToJsonElement(Buffer().also { api.dumpCache(it) }.readUtf8()).jsonObject
        val text = (dump.getValue("bodies") as JsonObject).keys
        val exists = (dump.getValue("exists") as JsonArray).map { it.jsonPrimitive.content }
        return text + exists
    }

    @Test
    fun `clearCache empties the cache`() = runTest {
        val api = client()
        api.putPdf("doc", source(byteArrayOf(1)))
        assertTrue(cachedHashes(api).isNotEmpty())
        api.clearCache()
        assertEquals(emptySet(), cachedHashes(api))
    }

    @Test
    fun `pruneCache drops the hashes the root can no longer reach`() = runTest {
        val api = client()
        val original = api.putPdf("doc", source(byteArrayOf(1)))
        val superseded = original.hash.hex
        api.rename(original, "renamed")

        assertTrue(superseded in cachedHashes(api), "the old index should still be cached")
        api.pruneCache()
        assertTrue(
            superseded !in cachedHashes(api),
            "the superseded item index is unreachable and should have been dropped",
        )
        assertEquals("renamed", api.getMetadata(api.listRefs().single()).visibleName)
    }

    @Test
    fun `a schema 3 account gets schema 3 item indexes and a schema 4 root`() = runTest {
        val legacy = MockCloud(schemaVersion = SchemaVersion.V3)
        try {
            val api = client(server = legacy)
            val ref = api.putPdf("doc", source(byteArrayOf(1)))

            val itemIndex = legacy.blob(ref.hash.hex)!!.toString(Charsets.UTF_8)
            assertTrue(itemIndex.startsWith("3\n"), "item index was: ${itemIndex.take(20)}")

            val rootIndex = legacy.blob(legacy.rootHash)!!.toString(Charsets.UTF_8)
            assertTrue(rootIndex.startsWith("4\n0:.:"), "root index was: ${rootIndex.take(20)}")

            // a schema 3 index is addressed by its entries' hashes, not by its own bytes
            val entries = parseEntryIndex(itemIndex).entries
            assertEquals(
                sha256Hex(entries.fold(ByteArray(0)) { acc, e -> acc + e.hash.hex.hexToBytes() }),
                ref.hash.hex,
            )

            assertEquals("doc", api.getMetadata(ref).visibleName)
            val renamed = api.rename(ref, "renamed")
            assertEquals("renamed", api.getMetadata(renamed).visibleName)
        } finally {
            legacy.close()
        }
    }

    @Test
    fun `every request carries the session token`() = runTest {
        val api = client()
        val ref = api.putPdf("doc", source(byteArrayOf(1)))
        api.rename(ref, "renamed")
        api.uploadPdf("ingested", source(byteArrayOf(2)))

        assertTrue(cloud.received.size > 5, "expected a spread of calls: ${cloud.received.size}")
        assertEquals(
            setOf<String?>("Bearer session-token"),
            cloud.authHeaders.toSet(),
            "every call, on every host, must be authenticated",
        )
    }
}

/**
 * The listing the client no longer provides.
 *
 * Both of these were once methods; each was one line over calls that remain, so they carry
 * their own composition here instead of in the public api.
 */
private fun source(bytes: ByteArray): Buffer = Buffer().write(bytes)

private suspend fun RawRemarkableClient.blob(fileName: String, hash: FileHash): ByteArray =
    Buffer().also { getBlob(fileName, hash, it) }.readByteArray()

private suspend fun RemarkableClient.metadataByRef(): Map<ItemRef, Metadata> =
    listRefs().associateWith { getMetadata(it) }

/** a one-stroke page, distinguishable from another by [x] */
private fun rmPage(x: Float): RmFile.Lines = RmFile.Lines(
    version = 5,
    layers = listOf(
        RmLayer(
            listOf(
                RmStroke(
                    pen = RmPen.FinelinerV5,
                    color = RmColor.Black,
                    width = 2.0f,
                    points = listOf(RmPoint(x, 2f, 3f, 4f, 5f, 6f)),
                ),
            ),
        ),
    ),
)

private fun templateJson(): String = """
    {"name":"grid","author":"reMarkable","iconData":"c3Zn","categories":["Planning"],
     "orientation":"portrait","templateVersion":"1.0.0","formatVersion":1,
     "items":[{"type":"line"}]}
""".trimIndent()

private suspend fun definitionOf(api: RemarkableClient, ref: ItemRef): TemplateDefinition =
    api.getTemplate(ref)
