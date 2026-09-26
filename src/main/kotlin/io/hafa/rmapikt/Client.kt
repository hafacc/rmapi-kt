package io.hafa.rmapikt

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import okio.Buffer
import okio.ForwardingSink
import okio.ForwardingSource
import okio.Sink
import okio.Source
import okio.blackholeSink
import okio.buffer
import okio.sink
import java.util.UUID
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

private const val GENERATION_BASE_MS = 25L
private const val PAGEDATA_SUFFIX = ".pagedata"
private const val DEFAULT_MARGINS = 125

/**
 * how a document is scaled to the screen
 *
 * A sealed type rather than an enum plus six loose numbers: the custom fit needs all of
 * them and the other modes need none, so pairing a mode with the wrong fields is not
 * representable instead of being silently ignored.
 */
public sealed interface Zoom {
    /** fit the whole page on screen */
    public data object BestFit : Zoom

    /** scale so the page height fills the screen */
    public data object FitToHeight : Zoom

    /** scale so the page width fills the screen */
    public data object FitToWidth : Zoom

    /** an explicitly positioned and scaled view */
    public data class Custom(
        /** the scale, which reMarkable generally keeps between 0.5 and 5 */
        public val scale: Double,
        /** the horizontal centre, in device pixels from the page's centre */
        public val centerX: Double,
        /** the vertical centre, in device pixels from the page's top */
        public val centerY: Double,
        /** the rendered page width in device pixels */
        public val pageWidth: Double,
        /** the rendered page height in device pixels, `heightPoints * dpi / 72` */
        public val pageHeight: Double,
        /** the orientation this view was configured in */
        public val orientation: Orientation,
    ) : Zoom
}

/** how a `put*` builds the document it commits */
public data class PutOptions(
    /** where the document lands */
    public val parent: Parent = Parent.Root,
    /** whether the document starts starred; the wire calls this `pinned` */
    public val starred: Boolean = false,
    /** the thumbnail page; -1 means the last visited */
    public val coverPageNumber: Int = -1,
    /** the authors recorded in the document's own metadata */
    public val authors: List<String>? = null,
    /** the title recorded in the document's own metadata, independent of its name */
    public val title: String? = null,
    /** the publication date */
    public val publicationDate: String? = null,
    /** the publisher */
    public val publisher: String? = null,
    /** pen settings and similar, passed through untouched */
    public val extraMetadata: Map<String, String> = emptyMap(),
    /** the render font; empty selects the device default */
    public val fontName: String = "",
    /** the line height; -1 selects the device default */
    public val lineHeight: Int = -1,
    /** the page margin in pixels */
    public val margins: Int = DEFAULT_MARGINS,
    /** the page orientation */
    public val orientation: Orientation = Orientation.Portrait,
    /** tags to apply, which are given the current timestamp */
    public val tags: List<String>? = null,
    /** how text is aligned */
    public val textAlignment: TextAlignment = TextAlignment.Justify,
    /** the font size */
    public val textScale: Double = 1.0,
    /** how the document is scaled to the screen */
    public val zoom: Zoom = Zoom.BestFit,
    /** the adaptive contrast filter */
    public val viewBackgroundFilter: BackgroundFilter? = null,
)

/** what to override when restoring an archive with [RemarkableClient.importArchive] */
public data class ImportOptions(
    /** where the restored document lands; keeps the archived parent when null */
    public val parent: Parent? = null,
    /** the restored document's name; keeps the archived name when null */
    public val visibleName: String? = null,
)

/**
 * the reMarkable cloud, in terms of documents rather than hashes
 *
 * Every mutating call is a read-modify-write against the account's root index, so two
 * clients editing at once will race. Losing that race is normal, and this client handles
 * it by re-reading and re-applying the change up to
 * [SessionOptions.maxGenerationRetries] times before giving up with a
 * [GenerationException].
 */
@Suppress("TooManyFunctions")
public class RemarkableClient internal constructor(
    private val rawClient: RawRemarkableClient,
    private val socket: NotificationSocket,
    sessionToken: SessionToken,
    private val maxGenerationRetries: Int,
) {
    /** the low-level api, for operations this one doesn't cover */
    public val raw: RawRemarkableClient get() = rawClient

    /**
     * the id this client registered under
     *
     * The uuid handed to [register], which reMarkable stamps on everything this client
     * does — so it is what [notifications] reports for this client's own writes, and what
     * tells those apart from a tablet's. Read out of the session token, which carries it.
     */
    public val deviceId: DeviceId by lazy { deviceIdOf(sessionToken) }

    private val rootLock = Any()

    @Volatile
    private var lastRoot: RootInfo? = null

    /**
     * re-reads the root index
     *
     * Only needed to observe someone else's change: this client's own writes keep its view
     * current, and a lost race is retried without help. A read that comes back older than
     * what is already cached is discarded, and the cached root returned instead.
     */
    public suspend fun refreshRoot(): RootInfo {
        val fetched = rawClient.getRootHash()
        return synchronized(rootLock) {
            val cached = lastRoot
            // a slow read can land after a newer write, and taking it would rewind the
            // cache to a root the account has already moved past
            if (cached == null || fetched.generation >= cached.generation) {
                lastRoot = fetched
                fetched
            } else {
                cached
            }
        }
    }

    /**
     * every sync of this account, for as long as the flow is collected
     *
     * reMarkable pushes a notification whenever a device finishes syncing. It names the
     * device and nothing else about the change — no item, no generation — so [refreshRoot]
     * is the answer to one: this shortens the wait before the trip, it does not save it.
     * Comparing against [deviceId] is how a client ignores its own writes. Other kinds of
     * event share the socket, screen sharing among them, and are passed over.
     *
     * Cold, so nothing is opened until somebody collects. The socket is then reopened
     * whenever the account drops it, which it does every couple of minutes, and leaving
     * the flow closes it. Which also makes this not a queue — whatever happens between two
     * sockets is never delivered — so anything that must not miss a change still reads on
     * its own schedule. A session token lasts a few hours, so a flow collected for longer
     * than that eventually fails to reconnect.
     *
     * ```kotlin
     * api.notifications()
     *     .filter { it.attributes.sourceDeviceID != api.deviceId }
     *     .collect { show(api.refreshRoot()) }
     * ```
     *
     * @throws ResponseException if the session token is refused, the one failure
     *   reconnecting cannot fix; get a fresh token with [auth] and build a new client
     * @throws java.io.IOException if the socket will not open [SessionOptions.maxTransientRetries]
     *   times in a row, which means the network rather than the account
     * @throws ValidationException if a notification carries something this library cannot
     *   read
     */
    public fun notifications(): Flow<SyncEvent> = socket.notifications()

    /**
     * The cached root, fetched on first use.
     *
     * Two calls can return different roots, so a mutating operation must take one snapshot
     * and derive both the entries it merges and the generation it commits against from it.
     * Mixing two would commit one root's entries against another's generation, which the
     * server accepts, reverting whatever landed in between.
     */
    private suspend fun root(): RootInfo = lastRoot ?: refreshRoot()

    private suspend fun commitRoot(hash: FileHash, generation: Long) {
        try {
            val updated = rawClient.putRootHash(hash, generation)
            lastRoot = lastRoot?.copy(hash = updated.hash, generation = updated.generation)
        } catch (error: GenerationException) {
            // the cached view is now known to be behind, so the retry must re-read
            lastRoot = null
            throw error
        }
    }

    /**
     * Re-runs [operation] when it loses a race for the root index.
     *
     * A lost race means the entry list the new root was built from is stale, so everything
     * derived from it is rebuilt per attempt. Ids and timestamps are minted before the call
     * — minting inside would orphan a fresh set of blobs each time — and re-sending the
     * blobs themselves costs nothing, as the cache skips a hash the store already has.
     */
    private suspend fun <T> withGenerationRetry(operation: suspend () -> T): T {
        var attempt = 0
        while (true) {
            try {
                return operation()
            } catch (error: GenerationException) {
                if (attempt >= maxGenerationRetries) {
                    throw error
                }
                delay(backoffMillis(attempt, GENERATION_BASE_MS))
                attempt++
            }
        }
    }

    private suspend fun uploadAll(staged: List<StagedFile>): Unit = coroutineScope {
        staged.map { async { rawClient.upload(it) } }.awaitAll()
    }

    /**
     * Adds a freshly built item to the root index, sending every blob in one wave.
     *
     * [files] are the item's component blobs and [docSchema] the index listing them.
     * Staging the new root index needs only [docSchema]'s hash, not its upload, so all of
     * them go out in one parallel batch together with the root index.
     */
    private suspend fun commitNewItem(files: List<StagedFile>, docSchema: StagedFile) =
        withGenerationRetry {
            val current = root()
            val entries = rawClient.getRootEntries(current.hash).entries
            // ids are minted UUIDs, so a collision means a bug up the stack; appending
            // anyway would commit a root holding two items under one id
            check(entries.none { it.id == docSchema.entry.id }) {
                "id '${docSchema.entry.id}' is already in the root index"
            }
            val rootIndex =
                rawClient.stageRootEntries(entries + docSchema.entry)
            uploadAll(files + docSchema + rootIndex)
            commitRoot(rootIndex.entry.hash, current.generation)
        }

    /**
     * Swaps [ref]'s root entry for [docSchema]'s, sending every blob in one wave.
     *
     * [files] are the component blobs the edit rewrote and [docSchema] the rebuilt index
     * listing them; the same one-batch reasoning as [commitNewItem] applies.
     *
     * Matching the entry on id and hash together: under schema 3 an index hashes its
     * entries' hashes and not their ids, so two items whose component blobs match hash
     * the same, and a hash-only lookup would edit whichever the root listed first.
     *
     * @throws HashNotFoundException if [ref] has since been written over
     */
    private suspend fun commitEdit(
        ref: ItemRef,
        files: List<StagedFile>,
        docSchema: StagedFile,
    ): ItemRef {
        withGenerationRetry {
            val attempt = root()
            val entries = rawClient.getRootEntries(attempt.hash).entries.toMutableList()
            val index = entries.indexOfFirst { it.id == ref.id.value && it.hash == ref.hash }
            if (index < 0) {
                throw HashNotFoundException(ref, entries.firstOrNull { it.id == ref.id.value }?.hash)
            }
            entries[index] = docSchema.entry
            val rootIndex = rawClient.stageRootEntries(entries)
            uploadAll(files + docSchema + rootIndex)
            commitRoot(rootIndex.entry.hash, attempt.generation)
        }
        return ItemRef(ref.id, docSchema.entry.hash)
    }

    /**
     * Starts an edit: refuses a ref the root does not list, and gives the schema version.
     *
     * A stale or fabricated ref fails as [HashNotFoundException] before any component is
     * fetched or staged, instead of surfacing as whatever fetching a hash the store never
     * held happens to throw. The merge in [commitEdit] checks again from the snapshot it
     * commits, so this is the early report, not the guard.
     */
    private suspend fun beginEdit(ref: ItemRef): SchemaVersion {
        val current = root()
        val entries = rawClient.getRootEntries(current.hash).entries
        if (entries.none { it.id == ref.id.value && it.hash == ref.hash }) {
            throw HashNotFoundException(ref, entries.firstOrNull { it.id == ref.id.value }?.hash)
        }
        return current.schemaVersion
    }

    /** every item's id and hash, without fetching any metadata */
    public suspend fun listRefs(): List<ItemRef> =
        rawClient.getRootEntries(root().hash).entries
            .map { ItemRef(ItemId.ofWire(it.id), it.hash) }

    private suspend fun componentEntries(ref: ItemRef): List<RawEntry> =
        rawClient.getEntries("${ref.id.value}$SCHEMA_SUFFIX", ref.hash).entries

    private suspend fun component(ref: ItemRef, which: DocumentComponent): RawEntry =
        componentEntries(ref).firstOrNull { it.id.endsWith(which.suffix) }
            ?: throw ComponentNotFoundException(ref, which)

    /** the item's `.content` file */
    public suspend fun getContent(ref: ItemRef): Content =
        component(ref, DocumentComponent.Content).let { rawClient.getContent(it.id, it.hash) }

    /** the item's `.metadata` file */
    public suspend fun getMetadata(ref: ItemRef): Metadata =
        component(ref, DocumentComponent.Metadata).let { rawClient.getMetadata(it.id, it.hash) }

    /**
     * A template's `.content` is empty, so its definition lives here instead.
     *
     * @throws ComponentNotFoundException if the item is not a template
     */
    public suspend fun getTemplate(ref: ItemRef): TemplateDefinition {
        requireKind(ref, EntryType.Template)
        return component(ref, DocumentComponent.Template).let {
            rawClient.getTemplate(it.id, it.hash)
        }
    }

    /**
     * writes the item's pdf into [sink] without holding it whole
     *
     * [sink] is flushed but not closed; [RawRemarkableClient.getBlob] says what a failure
     * leaves.
     *
     * @throws ComponentNotFoundException if the item has no pdf
     */
    public suspend fun getPdf(ref: ItemRef, sink: Sink): Unit =
        component(ref, DocumentComponent.Pdf).let { rawClient.getBlob(it.id, it.hash, sink) }

    /**
     * writes the item's epub into [sink] without holding it whole
     *
     * [sink] is flushed but not closed; [RawRemarkableClient.getBlob] says what a failure
     * leaves.
     *
     * @throws ComponentNotFoundException if the item has no epub
     */
    public suspend fun getEpub(ref: ItemRef, sink: Sink): Unit =
        component(ref, DocumentComponent.Epub).let { rawClient.getBlob(it.id, it.hash, sink) }

    /**
     * the pen strokes, keyed by page id; order them by [DocumentContent.pages]
     *
     * A reMarkable notebook is nothing else — it has no pdf or epub behind it.
     *
     * One unparseable page fails the whole call, which is what makes [setPages] safe: a
     * document that cannot be read in full is never one this api offers to write back. To
     * salvage the readable pages of one that fails, walk it through [raw].
     *
     * @throws ValidationException if any page is malformed
     */
    public suspend fun getPages(ref: ItemRef): Map<String, RmFile> = pagedFiles(ref, PagedFile.Rm)

    /**
     * one page, or null if nothing has been drawn on it yet
     *
     * Costs one blob, where [getPages] downloads the whole document and fails if any *other*
     * page is malformed.
     *
     * A page exists because [DocumentContent.pages] lists it; the device writes its `.rm`
     * only once something is drawn. So an untouched page is real and empty, not missing.
     *
     * @throws ValidationException if the document has no page [pageId]
     */
    public suspend fun getPage(ref: ItemRef, pageId: String): RmFile? =
        pagedFile(ref, pageId, PagedFile.Rm)

    /**
     * Rejects an item that is not the kind the caller asked for.
     *
     * From `.metadata`, which states the kind, rather than from the `.content`, which is told
     * apart by which keys it has — a template's is empty and reads as a folder's.
     */
    private suspend fun requireKind(ref: ItemRef, expected: EntryType) {
        val actual = getMetadata(ref).type
        if (actual != expected) {
            throw ValidationException(
                "expected a ${expected.name} at '${ref.hash.hex}' but found a ${actual.name}",
            )
        }
    }

    /**
     * The page ids the item's `.content` declares, or every page it has a file for.
     *
     * [DocumentContent.pages] is null until the device first opens the document; the
     * component list is the same truth one step further out.
     */
    private suspend fun declaredPages(ref: ItemRef): Set<String> {
        val declared = (getContent(ref) as? DocumentContent)?.pages
        return declared?.toSet() ?: pagedFileIds(ref)
    }

    private suspend fun pagedFileIds(ref: ItemRef): Set<String> =
        componentEntries(ref).mapNotNullTo(mutableSetOf()) {
            PagedFile.Rm.pageIdOf(ref.id.value, it.id)
        }

    /**
     * writes every component file of the item into [sink], zipped
     *
     * A transfer format for round-tripping through [importArchive], not the document itself
     * — for that use [getPdf]/[getEpub], or walk the item with [raw].
     *
     * The pdf or epub is streamed into the zip rather than held whole. [sink] is flushed but
     * not closed. A failure partway is raised, and whatever has reached [sink] by then is not
     * a usable archive even though it may open.
     */
    public suspend fun exportArchive(ref: ItemRef, sink: Sink): Unit =
        writeArchive(rawClient, componentEntries(ref), sink)

    /**
     * restores an archive produced by [exportArchive]
     *
     * Always lands under a fresh id, so a folder's children still point at the original
     * folder rather than the restored one.
     *
     * Takes an archive, not a pdf — for that use [putPdf] or [uploadPdf].
     *
     * [archive] is read to its end but not closed, and every file in it is held in memory
     * at once, since each must be hashed before the commit and may be sent again on a retry.
     * On a failure it is left where the read stopped, still not closed.
     */
    public suspend fun importArchive(
        archive: Source,
        options: ImportOptions = ImportOptions(),
    ): ItemRef {
        val files = readArchive(archive)
        val metadataPath = files.keys.firstOrNull { it.endsWith(METADATA_SUFFIX) }
            ?: throw ValidationException("archive did not contain a .metadata file")
        require(!metadataPath.dropLast(METADATA_SUFFIX.length).contains('/')) {
            "unexpected nested .metadata path '$metadataPath'"
        }
        val oldId = metadataPath.dropLast(METADATA_SUFFIX.length)
        // minted before the retry loop so a retry re-uploads identical blobs
        val newId = ItemId(UUID.randomUUID().toString())
        val lastModified = System.currentTimeMillis().toString()

        val schemaVersion = root().schemaVersion
        val staged = files.map { (path, bytes) ->
            require(path.startsWith(oldId)) {
                "archived file '$path' did not start with '$oldId'"
            }
            // every archived path is prefixed with the document's own id, so restoring under
            // a new id means rewriting that prefix on all of them
            val newPath = newId.value + path.removePrefix(oldId)
            if (path == metadataPath) {
                rawClient.stageMetadata(
                    newPath,
                    restoredMetadata(bytes, options, lastModified),
                )
            } else {
                rawClient.stageFile(newPath, bytes)
            }
        }
        val docSchema = rawClient.stageEntries(newId.value, staged.map { it.entry }, schemaVersion)
        commitNewItem(staged, docSchema)
        return ItemRef(newId, docSchema.entry.hash)
    }

    private fun restoredMetadata(
        archivedBytes: ByteArray,
        options: ImportOptions,
        lastModified: String,
    ): Metadata {
        val archived = decodeWire(
            Metadata.serializer(),
            archivedBytes.toString(Charsets.UTF_8),
            "archived metadata",
        )
        return archived.copy(
            parent = options.parent ?: archived.parent,
            visibleName = options.visibleName ?: archived.visibleName,
            lastModified = lastModified,
        )
    }

    /**
     * builds a pdf document locally and commits it through the sync protocol
     *
     * This constructs every component file itself, so it takes the full [PutOptions] and is
     * sensitive to the root generation. Its counterpart [uploadPdf] hands the file to the
     * server instead: fewer options, but the server does the work.
     *
     * [pdf] is read to its end but not closed. It is read into memory first: a blob is
     * named by its hash, so none of it can be sent before all of it has been read.
     */
    public suspend fun putPdf(
        visibleName: String,
        pdf: Source,
        options: PutOptions = PutOptions(),
    ): ItemRef = putDocumentFile(visibleName, FileType.Pdf, pdf.buffer().readByteArray(), options)

    /** the epub counterpart of [putPdf]; see it for how this differs from [uploadEpub] */
    public suspend fun putEpub(
        visibleName: String,
        epub: Source,
        options: PutOptions = PutOptions(),
    ): ItemRef = putDocumentFile(visibleName, FileType.Epub, epub.buffer().readByteArray(), options)

    private suspend fun putDocumentFile(
        visibleName: String,
        fileType: FileType,
        bytes: ByteArray,
        options: PutOptions,
    ): ItemRef {
        // every minted value is resolved here, before any retry, so re-running the commit
        // reuses the same blobs rather than orphaning a new set each time
        val id = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()
        val metadata = newDocumentMetadata(visibleName, options, now)
        val content = newDocumentContent(fileType, bytes.size, options, now)

        val schemaVersion = root().schemaVersion
        val staged = listOf(
            rawClient.stageContent("$id$CONTENT_SUFFIX", content),
            rawClient.stageMetadata("$id$METADATA_SUFFIX", metadata),
            rawClient.stageFile("$id$PAGEDATA_SUFFIX", "\n".encodeToByteArray()),
            rawClient.stageFile("$id.${fileType.extension}", bytes),
        )
        val docSchema = rawClient.stageEntries(id, staged.map { it.entry }, schemaVersion)
        commitNewItem(staged, docSchema)
        return ItemRef(ItemId(id), docSchema.entry.hash)
    }

    private fun newDocumentMetadata(
        visibleName: String,
        options: PutOptions,
        now: Long,
    ): Metadata = Metadata(
        visibleName = visibleName,
        parent = options.parent,
        pinned = options.starred,
        type = EntryType.Document,
        lastModified = now.toString(),
        createdTime = now.toString(),
        lastOpened = "0",
        lastOpenedPage = 0,
    )

    private fun newDocumentContent(
        fileType: FileType,
        sizeInBytes: Int,
        options: PutOptions,
        now: Long,
    ): DocumentContent = DocumentContent(
        coverPageNumber = options.coverPageNumber,
        documentMetadata = DocumentMetadata(
            authors = options.authors,
            title = options.title,
            publicationDate = options.publicationDate,
            publisher = options.publisher,
        ),
        extraMetadata = options.extraMetadata,
        fileType = fileType,
        fontName = options.fontName,
        lineHeight = options.lineHeight,
        orientation = options.orientation,
        // the device wants a page count of 1 and a matching page list even though it has
        // not rendered the file yet; anything else produces a document it won't open
        pageCount = 1,
        originalPageCount = 1,
        pages = listOf(UUID.randomUUID().toString()),
        redirectionPageMap = listOf(0),
        pageTags = emptyList(),
        textAlignment = options.textAlignment,
        textScale = options.textScale,
        zoomMode = options.zoom.mode,
        customZoomScale = (options.zoom as? Zoom.Custom)?.scale,
        customZoomCenterX = (options.zoom as? Zoom.Custom)?.centerX,
        customZoomCenterY = (options.zoom as? Zoom.Custom)?.centerY,
        customZoomPageWidth = (options.zoom as? Zoom.Custom)?.pageWidth,
        customZoomPageHeight = (options.zoom as? Zoom.Custom)?.pageHeight,
        customZoomOrientation = (options.zoom as? Zoom.Custom)?.orientation,
        tags = options.tags
            ?.let { names -> Tags.Structured(names.map { Tag(it, now) }) }
            ?: Tags.Empty,
        formatVersion = 1,
        margins = options.margins,
        sizeInBytes = sizeInBytes.toString(),
        viewBackgroundFilter = options.viewBackgroundFilter,
    )

    /** creates a folder through the sync protocol; the counterpart of [uploadFolder] */
    public suspend fun putFolder(visibleName: String, parent: Parent = Parent.Root): ItemRef {
        val id = UUID.randomUUID().toString()
        val now = System.currentTimeMillis().toString()
        val metadata = Metadata(
            visibleName = visibleName,
            parent = parent,
            pinned = false,
            type = EntryType.Collection,
            lastModified = now,
            createdTime = now,
        )
        val schemaVersion = root().schemaVersion
        val staged = listOf(
            rawClient.stageContent("$id$CONTENT_SUFFIX", CollectionContent()),
            rawClient.stageMetadata("$id$METADATA_SUFFIX", metadata),
        )
        val docSchema = rawClient.stageEntries(id, staged.map { it.entry }, schemaVersion)
        commitNewItem(staged, docSchema)
        return ItemRef(ItemId(id), docSchema.entry.hash)
    }

    /**
     * hands a pdf to the server's ingestion endpoint, which builds the document
     *
     * Robust and simple, but offers no control over how the document is rendered. Use
     * [putPdf] when you need [PutOptions].
     *
     * [pdf] is read as [RawRemarkableClient.uploadFile] says.
     */
    public suspend fun uploadPdf(visibleName: String, pdf: Source): ItemRef =
        rawClient.uploadFile(visibleName, pdf, UploadKind.Pdf)

    /** the epub counterpart of [uploadPdf]; see [putEpub] for the alternative */
    public suspend fun uploadEpub(visibleName: String, epub: Source): ItemRef =
        rawClient.uploadFile(visibleName, epub, UploadKind.Epub)

    /** creates a folder through the ingestion endpoint; the counterpart of [putFolder] */
    public suspend fun uploadFolder(visibleName: String): ItemRef =
        rawClient.uploadFile(visibleName, Buffer(), UploadKind.Folder)

    /** @throws ValidationException if the item at [ref] is not a document */
    public suspend fun getDocumentContent(ref: ItemRef): DocumentContent {
        requireKind(ref, EntryType.Document)
        return getContent(ref).orFail(ref.hash)
    }

    /** @throws ValidationException if the item at [ref] is not a folder */
    public suspend fun getCollectionContent(ref: ItemRef): CollectionContent {
        requireKind(ref, EntryType.Collection)
        return getContent(ref).orFail(ref.hash)
    }

    /**
     * writes a document's `.content`
     *
     * Keys the wire carries but this api does not model are preserved from what is already
     * stored, so writing back a value that never saw them cannot drop them.
     *
     * @throws ValidationException if the item at [ref] is not a document
     */
    public suspend fun setDocumentContent(ref: ItemRef, content: DocumentContent): ItemRef =
        editContent(ref, EntryType.Document) { content }

    /** see [setDocumentContent]; @throws ValidationException if [ref] is not a folder */
    public suspend fun setCollectionContent(ref: ItemRef, content: CollectionContent): ItemRef =
        editContent(ref, EntryType.Collection) { content }

    private suspend fun editContent(
        ref: ItemRef,
        expected: EntryType,
        update: (Content) -> Content,
    ): ItemRef {
        val schemaVersion = beginEdit(ref)
        requireKind(ref, expected)

        val components = componentEntries(ref).toMutableList()
        val index = components.indexOfFirst { it.id.endsWith(CONTENT_SUFFIX) }
        if (index < 0) {
            throw ComponentNotFoundException(ref, DocumentComponent.Content)
        }
        val entry = components[index]
        // read the raw text rather than the decoded value: a firmware quirk key the api
        // deliberately doesn't model still has to be written back exactly as it arrived
        val original = rawClient.getText(entry.id, entry.hash)
        val staged = rawClient.stageFile(
            entry.id,
            encodeContent(update(decodeContent(original)), contentQuirks(original)).encodeToByteArray(),
        )
        components[index] = staged.entry
        val docSchema = rawClient.stageEntries(ref.id.value, components, schemaVersion)
        return commitEdit(ref, listOf(staged), docSchema)
    }

    /**
     * Rewrites one item's `.metadata`, returning the new file and the docSchema listing it.
     *
     * Bumping `version` and setting `metadatamodified` is what tells the device the change
     * came from somewhere other than itself.
     */
    private suspend fun stageMetadataEdit(
        item: ItemRef,
        schemaVersion: SchemaVersion,
        update: (Metadata) -> Metadata,
    ): Pair<StagedFile, StagedFile> {
        val components = componentEntries(item).toMutableList()
        val index = components.indexOfFirst { it.id.endsWith(METADATA_SUFFIX) }
        if (index < 0) {
            throw ComponentNotFoundException(item, DocumentComponent.Metadata)
        }
        val entry = components[index]
        val current = rawClient.getMetadata(entry.id, entry.hash)
        val updated = update(current).let {
            it.copy(version = (it.version ?: 0) + 1, metadatamodified = true)
        }
        val staged = rawClient.stageMetadata(entry.id, updated)
        components[index] = staged.entry
        return staged to rawClient.stageEntries(item.id.value, components, schemaVersion)
    }

    /**
     * writes a template's `.template`
     *
     * The counterpart of [getTemplate]. A template's `.content` is empty, so this is the
     * whole of what the device renders it from.
     *
     * @throws ValidationException if the item at [ref] is not a template
     */
    public suspend fun setTemplate(ref: ItemRef, definition: TemplateDefinition): ItemRef {
        val schemaVersion = beginEdit(ref)
        requireKind(ref, EntryType.Template)

        val components = componentEntries(ref).toMutableList()
        val index = components.indexOfFirst { it.id.endsWith(TEMPLATE_SUFFIX) }
        if (index < 0) {
            throw ComponentNotFoundException(ref, DocumentComponent.Template)
        }
        val staged = rawClient.stageTemplate(components[index].id, definition)
        components[index] = staged.entry
        val docSchema = rawClient.stageEntries(ref.id.value, components, schemaVersion)
        return commitEdit(ref, listOf(staged), docSchema)
    }

    /**
     * writes an item's `.metadata`
     *
     * The general form of [move], [rename] and [star], and the only way to reach the rest of
     * [Metadata] — `lastOpened`, `lastOpenedPage`, `deleted`, `source`, the flags.
     *
     * [Metadata.version] and [Metadata.metadatamodified] are overwritten regardless of what
     * [metadata] carries: they are how the device is told a change came from elsewhere.
     */
    public suspend fun setMetadata(ref: ItemRef, metadata: Metadata): ItemRef =
        editMetadata(ref) { metadata }

    private suspend fun editMetadata(
        ref: ItemRef,
        update: (Metadata) -> Metadata,
    ): ItemRef {
        val (metadataFile, docSchema) = stageMetadataEdit(ref, beginEdit(ref), update)
        return commitEdit(ref, listOf(metadataFile), docSchema)
    }

    /**
     * rewrites pages the document already has, leaving the rest alone
     *
     * Handing back a map from [getPages] uploads only what changed: an unedited page
     * serialises to the bytes it was read from, so it re-stages to a hash already cached.
     * A page that has never been drawn on gets its `.rm` created here.
     *
     * `lastModified` is not touched — call [setMetadata] too if the document should look
     * freshly edited.
     *
     * @throws ValidationException if [pages] names a page the document does not have. Adding
     * a page means listing it in [DocumentContent.pages], so that belongs to
     * [setDocumentContent].
     */
    public suspend fun setPages(ref: ItemRef, pages: Map<String, RmFile>): ItemRef =
        setPagedFiles(ref, pages, PagedFile.Rm)

    /** the single-page case of [setPages] */
    public suspend fun setPage(ref: ItemRef, pageId: String, page: RmFile): ItemRef =
        setPages(ref, mapOf(pageId to page))

    /**
     * every page's highlights, keyed by page id
     *
     * Only pages that carry highlights appear. A page's highlights are a list of lists
     * because reMarkable splits one highlighted passage into a fragment per line it spans.
     */
    public suspend fun getHighlights(ref: ItemRef): Map<String, List<List<Highlight>>> =
        pagedFiles(ref, PagedFile.Highlights)

    /** one page's highlights, or null if it has none; see [getHighlights] */
    public suspend fun getHighlights(ref: ItemRef, pageId: String): List<List<Highlight>>? =
        pagedFile(ref, pageId, PagedFile.Highlights)

    /**
     * writes the highlights of the pages named, leaving the rest alone
     *
     * A page with no highlights file yet gets one. Nothing checks the text against the
     * document: [Highlight.start] and [Highlight.length] index into the page's own text, and
     * this library does not extract that.
     */
    public suspend fun setHighlights(
        ref: ItemRef,
        highlights: Map<String, List<List<Highlight>>>,
    ): ItemRef = setPagedFiles(ref, highlights, PagedFile.Highlights)

    /** the single-page case of [setHighlights] */
    public suspend fun setHighlights(
        ref: ItemRef,
        pageId: String,
        highlights: List<List<Highlight>>,
    ): ItemRef = setHighlights(ref, mapOf(pageId to highlights))

    /**
     * the template name behind each page, in page order
     *
     * One entry per [DocumentContent.pages] entry, empty for a page with no template.
     * Returns an empty list for a document with no `.pagedata` at all.
     */
    public suspend fun getPagedata(ref: ItemRef): List<String> {
        val fileName = "${ref.id.value}$PAGEDATA_SUFFIX"
        val entry = componentEntries(ref).firstOrNull { it.id == fileName } ?: return emptyList()
        // exactly one trailing empty, from the terminating newline: dropping every trailing
        // empty would lose a real final page that has no template
        val lines = rawClient.getText(entry.id, entry.hash).split("\n")
        return if (lines.lastOrNull() == "") lines.dropLast(1) else lines
    }

    /**
     * writes the template name behind each page
     *
     * The device expects one entry per page and reads them positionally, so a list shorter
     * than [DocumentContent.pages] silently leaves later pages without a template. Nothing
     * here checks that, because the page list is in the `.content` and this call does not
     * read it.
     */
    public suspend fun setPagedata(ref: ItemRef, templates: List<String>): ItemRef {
        val schemaVersion = beginEdit(ref)
        requireKind(ref, EntryType.Document)
        val components = componentEntries(ref).toMutableList()
        val fileName = "${ref.id.value}$PAGEDATA_SUFFIX"
        val staged = rawClient.stageFile(fileName, templates.joinToString("") { "$it\n" }.encodeToByteArray())
        val index = components.indexOfFirst { it.id == fileName }
        if (index < 0) components.add(staged.entry) else components[index] = staged.entry
        val docSchema = rawClient.stageEntries(ref.id.value, components, schemaVersion)
        return commitEdit(ref, listOf(staged), docSchema)
    }

    /**
     * every page's layer metadata, keyed by page id
     *
     * Only pages that carry a metadata file appear; a page whose layers have never been
     * named has none. The layer order matches [RmFile.layers].
     */
    public suspend fun getPageMetadata(ref: ItemRef): Map<String, PageMetadata> =
        pagedFiles(ref, PagedFile.LayerMetadata)

    /** one page's layer metadata, or null if it has none; see [getPageMetadata] */
    public suspend fun getPageMetadata(ref: ItemRef, pageId: String): PageMetadata? =
        pagedFile(ref, pageId, PagedFile.LayerMetadata)

    /** writes the layer metadata of the pages named, leaving the rest alone */
    public suspend fun setPageMetadata(
        ref: ItemRef,
        metadata: Map<String, PageMetadata>,
    ): ItemRef = setPagedFiles(ref, metadata, PagedFile.LayerMetadata)

    /** the single-page case of [setPageMetadata] */
    public suspend fun setPageMetadata(
        ref: ItemRef,
        pageId: String,
        metadata: PageMetadata,
    ): ItemRef = setPageMetadata(ref, mapOf(pageId to metadata))

    /** every page that has this file, keyed by page id */
    private suspend fun <T> pagedFiles(ref: ItemRef, kind: PagedFile<T>): Map<String, T> =
        coroutineScope {
            componentEntries(ref)
                .mapNotNull { entry -> kind.pageIdOf(ref.id.value, entry.id)?.to(entry) }
                .map { (pageId, entry) ->
                    async { pageId to kind.read(rawClient, entry.id, entry.hash) }
                }
                .awaitAll()
                .toMap()
        }

    /** one page's file, or null when the page carries none */
    private suspend fun <T> pagedFile(ref: ItemRef, pageId: String, kind: PagedFile<T>): T? {
        if (pageId !in declaredPages(ref)) {
            noSuchPages(ref.id.value, listOf(pageId))
        }
        val fileName = kind.fileName(ref.id.value, pageId)
        val entry = componentEntries(ref).firstOrNull { it.id == fileName } ?: return null
        return kind.read(rawClient, entry.id, entry.hash)
    }

    private suspend fun <T> setPagedFiles(
        ref: ItemRef,
        values: Map<String, T>,
        kind: PagedFile<T>,
    ): ItemRef = if (values.isEmpty()) {
        ref
    } else {
        val schemaVersion = beginEdit(ref)
        val unknown = values.keys - declaredPages(ref)
        if (unknown.isNotEmpty()) {
            noSuchPages(ref.id.value, unknown)
        }

        // the device writes one of these only when there is something to write, so a
        // first write adds an entry where a later one replaces it
        val components = componentEntries(ref).toMutableList()
        val existing = components.withIndex()
            .mapNotNull { (index, entry) -> kind.pageIdOf(ref.id.value, entry.id)?.to(index) }
            .toMap()
        val staged = values.map { (pageId, value) ->
            kind.stage(rawClient, kind.fileName(ref.id.value, pageId), value)
                .also { file ->
                    val index = existing[pageId]
                    if (index == null) {
                        components.add(file.entry)
                    } else {
                        components[index] = file.entry
                    }
                }
        }
        val docSchema = rawClient.stageEntries(ref.id.value, components, schemaVersion)
        commitEdit(ref, staged, docSchema)
    }

    /** moves an item */
    public suspend fun move(ref: ItemRef, parent: Parent): ItemRef =
        editMetadata(ref) { it.copy(parent = parent) }

    /** moves an item to the trash, where the device can still restore it */
    public suspend fun trash(ref: ItemRef): ItemRef = move(ref, Parent.Trash)

    /**
     * removes an item from the account instead of moving it to the trash
     *
     * The entry leaves the root index, which is as deleted as anything gets here: the
     * item's blobs stay in the store, no longer reachable from any index, and nothing
     * restores it. Only the item named goes — purging a folder leaves whatever was inside
     * it pointing at a folder that is no longer there, so purge the contents first.
     *
     * @throws HashNotFoundException if [ref] is not in the current root index
     */
    public suspend fun purge(ref: ItemRef): Unit = withGenerationRetry {
        val current = root()
        val entries = rawClient.getRootEntries(current.hash).entries
        if (entries.none { it.id == ref.id.value && it.hash == ref.hash }) {
            throw HashNotFoundException(ref, entries.firstOrNull { it.id == ref.id.value }?.hash)
        }
        commitWithout(current, entries, setOf(ref.id.value))
    }

    /** renames an item */
    public suspend fun rename(ref: ItemRef, visibleName: String): ItemRef =
        editMetadata(ref) { it.copy(visibleName = visibleName) }

    /** stars or unstars an item; the wire calls this `pinned` */
    public suspend fun star(ref: ItemRef, starred: Boolean): ItemRef =
        editMetadata(ref) { it.copy(pinned = starred) }

    /**
     * moves many items in a single root write
     *
     * Racing another client is normal, so a ref no longer in the root index is passed over
     * rather than failing the batch; subtract the keys from what was handed in to see those.
     *
     * @return each ref that moved, mapped to a ref at its new state
     */
    public suspend fun bulkMove(refs: Collection<ItemRef>, parent: Parent): Map<ItemRef, ItemRef> {
        val current = root()
        // keyed on both halves, per [commitEdit], and keying on the hash alone would also
        // silently drop a duplicate ref
        val wanted = refs.associateBy { it.id.value to it.hash }
        val toUpdate = rawClient.getRootEntries(current.hash).entries
            .filter { (it.id to it.hash) in wanted }
        if (toUpdate.isEmpty()) {
            return emptyMap()
        }

        val staged = coroutineScope {
            toUpdate.map { item ->
                async {
                    val (metadataFile, docSchema) = stageMetadataEdit(
                        ItemRef(ItemId.ofWire(item.id), item.hash),
                        current.schemaVersion,
                    ) { it.copy(parent = parent) }
                    Triple(item, metadataFile, docSchema)
                }
            }.awaitAll()
        }
        val files = staged.flatMap { (_, metadataFile, docSchema) ->
            listOf(metadataFile, docSchema)
        }
        val edits = staged.associate { (item, _, docSchema) ->
            (item.id to item.hash) to docSchema.entry
        }

        // an item another client wrote in the meantime is no longer at the hash these
        // rebuilt indexes were derived from, so it drops out of the merge and is reported
        // as not found rather than being written over
        val rewritten = withGenerationRetry {
            val attempt = root()
            val entries = rawClient.getRootEntries(attempt.hash).entries
            val applied = entries.mapNotNull { entry ->
                edits[entry.id to entry.hash]?.let { (entry.id to entry.hash) to it }
            }
            // the server accepts an unchanged root and still burns a generation
            if (applied.isEmpty()) {
                emptyList()
            } else {
                val rootIndex = rawClient.stageRootEntries(
                    entries.map { edits[it.id to it.hash] ?: it },
                )
                uploadAll(files + rootIndex)
                commitRoot(rootIndex.entry.hash, attempt.generation)
                applied
            }
        }

        return rewritten.associate { (key, entry) ->
            val ref = wanted.getValue(key)
            ref to ItemRef(ref.id, entry.hash)
        }
    }

    /** trashes many items in one root write; see [bulkMove] */
    public suspend fun bulkTrash(refs: Collection<ItemRef>): Map<ItemRef, ItemRef> =
        bulkMove(refs, Parent.Trash)

    /**
     * purges many items in one root write
     *
     * The bulk form of [purge], and like [bulkMove] it passes over a ref the root index no
     * longer lists rather than failing the batch; subtract the result from what was handed
     * in to see those. An item another client has written since is at a hash this call was
     * not given, so it is left alone rather than removed on the strength of its id.
     *
     * @return the refs that were removed
     */
    public suspend fun bulkPurge(refs: Collection<ItemRef>): Set<ItemRef> = withGenerationRetry {
        val current = root()
        val entries = rawClient.getRootEntries(current.hash).entries
        // keyed on both halves, per [commitEdit]
        val wanted = refs.associateBy { it.id.value to it.hash }
        val found = entries.filter { (it.id to it.hash) in wanted }
        if (found.isEmpty()) {
            // the server accepts an unchanged root and still burns a generation
            emptySet()
        } else {
            commitWithout(current, entries, found.mapTo(mutableSetOf()) { it.id })
            found.mapTo(mutableSetOf()) { wanted.getValue(it.id to it.hash) }
        }
    }

    /**
     * purges everything the trash holds, in a single root write
     *
     * Trashing a folder does not touch what is inside it — those items keep pointing at
     * the folder, which is what lets the device restore the lot together — so the trash's
     * contents are the whole tree hanging off it, not just the items that name it. Reading
     * that tree costs one `.metadata` per item in the account.
     *
     * @return a ref to each item removed, at the state it was removed in
     * @throws ValidationException if any item's `.metadata` cannot be read, since an item
     *   whose parent is unknown cannot be shown to be outside the trash
     */
    public suspend fun purgeTrash(): Set<ItemRef> = withGenerationRetry {
        val current = root()
        val entries = rawClient.getRootEntries(current.hash).entries
        val trashed = trashedIds(entries)
        if (trashed.isEmpty()) {
            emptySet()
        } else {
            commitWithout(current, entries, trashed)
            entries.filter { it.id in trashed }
                .mapTo(mutableSetOf()) { ItemRef(ItemId.ofWire(it.id), it.hash) }
        }
    }

    /**
     * Commits a root index with [doomed] left out of it.
     *
     * Nothing else is staged: the remaining items are already in the store at the hashes
     * this index lists, and removal is only ever a shorter root.
     */
    private suspend fun commitWithout(
        current: RootInfo,
        entries: List<RawEntry>,
        doomed: Set<String>,
    ) {
        val rootIndex = rawClient.stageRootEntries(entries.filterNot { it.id in doomed })
        uploadAll(listOf(rootIndex))
        commitRoot(rootIndex.entry.hash, current.generation)
    }

    /**
     * writes the cache into [sink], for [loadCache] in a later session
     *
     * The dump is versioned JSON, written an entry at a time and never built whole, so
     * persisting a full cache costs about one entry of heap rather than several times the
     * cache. [sink] is flushed but not closed. An okio [Buffer] is an in-memory sink, so
     * `Buffer().also { api.dumpCache(it) }.readUtf8()` gives the dump as a string.
     *
     * @return the [cacheRevision] the dump holds; a later revision means the cache has
     *   changed since, and an equal one that writing it again would write the same thing
     */
    public fun dumpCache(sink: Sink): Long = rawClient.dumpCache(sink)

    /**
     * replaces the cache with a dump from [dumpCache], reading it an entry at a time
     *
     * An unreadable dump leaves the cache as it was, and its [ValidationException] carries no
     * raw text, since the dump is read a piece at a time and never held whole. [source] is
     * read to its end but not closed; on a failure it is left where the read stopped, still
     * not closed.
     *
     * @return the [cacheRevision] the loaded cache is at, which is the dump's
     * @throws ValidationException if [source] is not a dump this build can read
     */
    public fun loadCache(source: Source): Long = rawClient.loadCache(source)

    /**
     * a number that moves whenever the cache's contents do
     *
     * Compare it with what [dumpCache] or [loadCache] returned to learn, without dumping
     * anything, whether a dump written then is still current. A read that only reorders the
     * cache does not move it.
     */
    public val cacheRevision: Long get() = rawClient.cacheRevision()

    /** empties the cache */
    public fun clearCache(): Unit = rawClient.clearCache()

    /** drops every cached hash the root index can no longer reach */
    public suspend fun pruneCache() {
        val current = root()
        val unreachable = rawClient.cachedHashes().toMutableSet()
        unreachable.remove(current.hash.hex)

        var frontier = listOf(rawClient.getRootEntries(current.hash).entries)
        while (frontier.isNotEmpty()) {
            val reached = frontier.flatten()
            reached.forEach { unreachable.remove(it.hash.hex) }
            frontier = descend(reached.filter { it.subfiles > 0 })
        }
        unreachable.forEach(rawClient::forget)
    }

    private suspend fun descend(parents: List<RawEntry>): List<List<RawEntry>> = coroutineScope {
        parents
            .map { async { rawClient.getEntries("${it.id}$SCHEMA_SUFFIX", it.hash).entries } }
            .awaitAll()
    }
}

/** Every id in the trash, walking down from the items that name it as their parent. */
private suspend fun RemarkableClient.trashedIds(entries: List<RawEntry>): Set<String> =
    coroutineScope {
        val children = entries
            .map { entry ->
                async {
                    val ref = ItemRef(ItemId.ofWire(entry.id), entry.hash)
                    getMetadata(ref).parent.wire to entry.id
                }
            }
            .awaitAll()
            .groupBy({ (parent, _) -> parent }, { (_, id) -> id })

        val trashed = mutableSetOf<String>()
        var frontier = children[Parent.Trash.wire].orEmpty()
        while (frontier.isNotEmpty()) {
            trashed.addAll(frontier)
            // subtracting what has been seen also ends a parent cycle, which the account
            // should never hold but which would otherwise loop here forever
            frontier = frontier.flatMap { children[it].orEmpty() } - trashed
        }
        trashed
    }

/** Reports page ids an item does not declare; shared so a read and a write agree. */
private fun noSuchPages(itemId: String, pageIds: Collection<String>): Nothing =
    throw ValidationException(
        "document '$itemId' has no page ${pageIds.sorted().joinToString()}",
    )

/**
 * Narrows decoded content to the variant the caller asked to edit.
 *
 * An item's `.metadata` says what kind of thing it is, but the `.content` file is told
 * apart by which keys it has, and the two can disagree: a document whose content is `{}`
 * decodes as a collection. Checking the metadata alone would leave an unchecked cast to
 * fail as a ClassCastException, where the api promises a ValidationException.
 */
private inline fun <reified T : Content> Content.orFail(hash: FileHash): T = this as? T
    ?: throw ValidationException(
        "the content at '${hash.hex}' is ${this::class.simpleName}, " +
            "not the ${T::class.simpleName} its metadata implies",
    )

/**
 * a file a document keeps once per page, named `<prefix><pageId><suffix>`
 *
 * Deriving both the name and the page id from one prefix and suffix is the point: two
 * independent functions would let the writer and the reader drift apart, and the `.rm` and
 * the layer metadata of a page differ only by suffix.
 *
 * A page exists because [DocumentContent.pages] lists it. Whether it has any given one of
 * these is separate: the device writes each only when there is something to write, so an
 * absent file means an empty page rather than a missing one.
 */
internal sealed class PagedFile<T>(private val suffix: String) {
    protected abstract fun prefix(docId: String): String

    // the raw tier owns every codec; these only say which of its accessors to use
    abstract suspend fun read(raw: RawRemarkableClient, fileName: String, hash: FileHash): T

    abstract fun stage(raw: RawRemarkableClient, id: String, value: T): StagedFile

    fun fileName(docId: String, pageId: String): String = "${prefix(docId)}$pageId$suffix"

    fun pageIdOf(docId: String, fileId: String): String? {
        val prefix = prefix(docId)
        return if (fileId.startsWith(prefix) && fileId.endsWith(suffix)) {
            fileId.removePrefix(prefix).removeSuffix(suffix)
        } else {
            null
        }
    }

    /** the pen strokes, `<docId>/<pageId>.rm` */
    object Rm : PagedFile<RmFile>(RM_SUFFIX) {
        override fun prefix(docId: String): String = "$docId/"

        override suspend fun read(raw: RawRemarkableClient, fileName: String, hash: FileHash) =
            raw.getRm(fileName, hash)

        override fun stage(raw: RawRemarkableClient, id: String, value: RmFile) =
            raw.stageRm(id, value)
    }

    /** text highlights, `<docId>.highlights/<pageId>.json` */
    object Highlights : PagedFile<List<List<Highlight>>>(".json") {
        override fun prefix(docId: String): String = "$docId$HIGHLIGHTS_SUFFIX/"

        override suspend fun read(raw: RawRemarkableClient, fileName: String, hash: FileHash) =
            raw.getHighlights(fileName, hash)

        override fun stage(raw: RawRemarkableClient, id: String, value: List<List<Highlight>>) =
            raw.stageHighlights(id, value)
    }

    /** layer names, `<docId>/<pageId>-metadata.json`, beside the page's own `.rm` */
    object LayerMetadata : PagedFile<PageMetadata>(PAGE_METADATA_SUFFIX) {
        override fun prefix(docId: String): String = "$docId/"

        override suspend fun read(raw: RawRemarkableClient, fileName: String, hash: FileHash) =
            raw.getPageMetadata(fileName, hash)

        override fun stage(raw: RawRemarkableClient, id: String, value: PageMetadata) =
            raw.stagePageMetadata(id, value)
    }
}

/**
 * how the document is scaled to the screen, read back from the flat wire fields
 *
 * Null when the `.content` has no `zoomMode`, or when it says `customFit` without carrying
 * all six numbers a [Zoom.Custom] needs — a state the wire can hold and this type cannot.
 */
public val DocumentContent.zoom: Zoom?
    get() = when (zoomMode) {
        null -> null
        ZoomMode.BestFit -> Zoom.BestFit
        ZoomMode.FitToHeight -> Zoom.FitToHeight
        ZoomMode.FitToWidth -> Zoom.FitToWidth
        ZoomMode.CustomFit -> Zoom.Custom(
            scale = customZoomScale ?: return null,
            centerX = customZoomCenterX ?: return null,
            centerY = customZoomCenterY ?: return null,
            pageWidth = customZoomPageWidth ?: return null,
            pageHeight = customZoomPageHeight ?: return null,
            orientation = customZoomOrientation ?: return null,
        )
    }

/** the same content scaled a different way, with every `customZoom*` field rewritten */
public fun DocumentContent.withZoom(zoom: Zoom): DocumentContent {
    val custom = zoom as? Zoom.Custom
    return copy(
        zoomMode = zoom.mode,
        customZoomScale = custom?.scale,
        customZoomCenterX = custom?.centerX,
        customZoomCenterY = custom?.centerY,
        customZoomPageWidth = custom?.pageWidth,
        customZoomPageHeight = custom?.pageHeight,
        customZoomOrientation = custom?.orientation,
    )
}

/** the wire's flat mode value for a [Zoom] */
internal val Zoom.mode: ZoomMode
    get() = when (this) {
        Zoom.BestFit -> ZoomMode.BestFit
        Zoom.FitToHeight -> ZoomMode.FitToHeight
        Zoom.FitToWidth -> ZoomMode.FitToWidth
        is Zoom.Custom -> ZoomMode.CustomFit
    }

/** The pdf or epub is streamed from [raw] into its entry; the small files are fetched whole, together. */
private suspend fun writeArchive(raw: RawRemarkableClient, entries: List<RawEntry>, sink: Sink) {
    val small = coroutineScope {
        entries.filterNot { it.isDocumentFile() }
            .map { entry -> async { entry.id to raw.getBytes(entry.id, entry.hash) } }
            .awaitAll()
            .toMap()
    }
    val unclosable = object : ForwardingSink(sink) {
        override fun close() = flush()
    }
    ZipOutputStream(unclosable.buffer().outputStream()).use { zip ->
        for (entry in entries) {
            zip.putNextEntry(ZipEntry(entry.id))
            val bytes = small[entry.id]
            if (bytes != null) {
                zip.write(bytes)
            } else {
                // not closed: that would end the zip partway
                raw.getBlob(entry.id, entry.hash, zip.sink())
            }
            zip.closeEntry()
        }
    }
}

private fun RawEntry.isDocumentFile(): Boolean =
    id.endsWith(DocumentComponent.Pdf.suffix) || id.endsWith(DocumentComponent.Epub.suffix)

private fun readArchive(archive: Source): Map<String, ByteArray> {
    val files = LinkedHashMap<String, ByteArray>()
    val unclosable = object : ForwardingSource(archive) {
        override fun close() = Unit
    }
    val buffered = unclosable.buffer()
    ZipInputStream(buffered.inputStream()).use { zip ->
        while (true) {
            val entry = zip.nextEntry ?: break
            if (!entry.isDirectory) {
                files[entry.name] = zip.readBytes()
            }
            zip.closeEntry()
        }
        // the central directory is past the last entry, and the source is promised read to its end
        buffered.readAll(blackholeSink())
    }
    return files
}
